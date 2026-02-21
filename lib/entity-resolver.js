/**
 * Entity Resolver — Conversational 3-tier resolution.
 *
 * Instead of Hindsight-style algorithmic scoring that risks silent false merges,
 * this uses a conversational model: the agent asks for clarification the same
 * way a human would.
 *
 * Three tiers:
 *   assume (>0.8)  — auto-merge, single candidate seen recently
 *   ask    (<0.4)  — inject [GRAPH NOTE] so agent asks naturally
 *   defer  (0.4-0.8) — write with pending_resolution, revisit later
 *
 * Phase 3 of the knowledge graph plugin.
 */

class EntityResolver {
    /**
     * @param {import('./graph-store')} store - GraphStore instance
     * @param {Object} config - entityResolution config section
     */
    constructor(store, config) {
        this.store = store;
        this.assumeThreshold = config?.assumeThreshold ?? 0.8;
        this.askThreshold = config?.askThreshold ?? 0.4;
        this.pendingMaxAgeDays = config?.pendingMaxAgeDays ?? 30;
        this.recencyBoostDays = config?.recencyBoostDays ?? 7;
        this.cooccurrenceMinCount = config?.cooccurrenceMinCount ?? 2;
    }

    /**
     * Resolve a mention to an existing entity or determine what to do.
     *
     * @param {string} mention - The entity name as mentioned in text
     * @param {string} agentId - Agent context
     * @param {string[]} contextEntities - Other entities in this exchange (for co-occurrence scoring)
     * @returns {{ tier: string, confidence: number, entity?: Object, candidates?: Array, graphNote?: string }}
     */
    resolve(mention, agentId, contextEntities) {
        const normalizedId = this.store.normalizeEntityId(mention);
        const existing = this.store.getEntity(normalizedId);

        // Exact match — highest confidence, just update
        if (existing) {
            return { tier: 'exact', confidence: 1.0, entity: existing };
        }

        // Find candidates via alias search and prefix matching
        const candidates = this._findCandidates(mention, agentId);

        if (candidates.length === 0) {
            return { tier: 'new', confidence: 1.0 };
        }

        // Score each candidate
        const scored = candidates.map(c => ({
            ...c,
            resolutionScore: this._scoreCandidate(c, contextEntities)
        })).sort((a, b) => b.resolutionScore - a.resolutionScore);

        const best = scored[0];

        if (scored.length === 1) {
            return this._singleCandidateResult(best, mention);
        }

        return this._multipleCandidateResult(scored, mention);
    }

    /**
     * Compute resolution confidence for a single candidate.
     */
    _scoreCandidate(candidate, contextEntities) {
        let score = 0.5; // base for any match

        // Recency boost
        const daysSince = this._daysSinceLastSeen(candidate);
        if (daysSince < this.recencyBoostDays) {
            score += 0.3;
        } else if (daysSince < 30) {
            score += 0.1;
        }

        // Co-occurrence with current context entities
        if (contextEntities && contextEntities.length > 0) {
            const coocCount = this._countContextCooccurrences(candidate.id, contextEntities);
            if (coocCount >= this.cooccurrenceMinCount) {
                score += 0.2;
            } else if (coocCount > 0) {
                score += 0.1;
            }
        }

        // Mention frequency boost (well-established entities)
        if (candidate.mention_count > 10) {
            score += 0.1;
        } else if (candidate.mention_count > 3) {
            score += 0.05;
        }

        return Math.min(1.0, score);
    }

    /**
     * Handle single candidate resolution.
     */
    _singleCandidateResult(candidate, mention) {
        const score = candidate.resolutionScore;

        if (score >= this.assumeThreshold) {
            return { tier: 'assume', confidence: score, entity: candidate };
        }

        if (score < this.askThreshold) {
            return {
                tier: 'ask',
                confidence: score,
                candidates: [candidate],
                graphNote: this._buildGraphNote(mention, [candidate])
            };
        }

        return { tier: 'defer', confidence: score, entity: candidate };
    }

    /**
     * Handle multiple candidate resolution.
     */
    _multipleCandidateResult(sorted, mention) {
        const best = sorted[0];
        const second = sorted[1];

        // Check for dominant candidate
        const dominance = best.mention_count / Math.max(1, second.mention_count);
        const daysSince = this._daysSinceLastSeen(best);

        if (dominance > 5 && daysSince < this.recencyBoostDays) {
            return { tier: 'assume', confidence: 0.85, entity: best };
        }

        // Multiple ambiguous candidates — ask
        return {
            tier: 'ask',
            confidence: 0.3,
            candidates: sorted.slice(0, 3),
            graphNote: this._buildGraphNote(mention, sorted.slice(0, 3))
        };
    }

    /**
     * Find candidate entities that might match a mention.
     * Uses prefix matching and alias scanning.
     */
    _findCandidates(mention, agentId) {
        const candidates = [];
        const seen = new Set();
        const normalizedMention = mention.toLowerCase().trim();

        // Prefix match on canonical_name
        const prefixResults = this.store.findEntitiesByPrefix(
            normalizedMention.substring(0, Math.max(3, Math.ceil(normalizedMention.length * 0.6))),
            agentId
        );
        for (const r of prefixResults) {
            if (!seen.has(r.id)) {
                seen.add(r.id);
                candidates.push(r);
            }
        }

        // Also check if mention appears as a substring of any known entity name
        // (handles "Bob" matching "Bob Martinez")
        const allRecent = this.store._recentEntities.all(agentId, 200);
        for (const e of allRecent) {
            if (seen.has(e.id)) continue;
            const canonLower = e.canonical_name.toLowerCase();
            if (canonLower.includes(normalizedMention) || normalizedMention.includes(canonLower)) {
                seen.add(e.id);
                candidates.push(e);
            }
            // Check aliases
            try {
                const aliases = JSON.parse(e.aliases || '[]');
                for (const alias of aliases) {
                    if (alias.toLowerCase().includes(normalizedMention) ||
                        normalizedMention.includes(alias.toLowerCase())) {
                        if (!seen.has(e.id)) {
                            seen.add(e.id);
                            candidates.push(e);
                        }
                        break;
                    }
                }
            } catch { /* skip malformed aliases */ }
        }

        return candidates;
    }

    /**
     * Count co-occurrences between a candidate entity and the current context entities.
     */
    _countContextCooccurrences(candidateId, contextEntities) {
        let total = 0;
        for (const ctxName of contextEntities) {
            const ctxId = this.store.normalizeEntityId(ctxName);
            const sorted = [candidateId, ctxId].sort();
            const coocs = this.store._getCooccurrences.all(sorted[0], sorted[0], 1);
            // Check if this specific pair exists
            for (const c of coocs) {
                if ((c.entity_a === sorted[0] && c.entity_b === sorted[1]) ||
                    (c.entity_a === sorted[1] && c.entity_b === sorted[0])) {
                    total += c.count;
                }
            }
        }
        return total;
    }

    /**
     * Compute days since an entity was last seen.
     */
    _daysSinceLastSeen(entity) {
        if (!entity.last_seen) return 999;
        const lastSeen = new Date(entity.last_seen).getTime();
        const now = Date.now();
        return Math.floor((now - lastSeen) / (24 * 60 * 60 * 1000));
    }

    /**
     * Build a natural [GRAPH NOTE] hint for the agent.
     */
    _buildGraphNote(mention, candidates) {
        const lines = [`"${mention}" was mentioned but isn't clearly matched to a known entity.`];
        lines.push('Known:');
        for (const c of candidates) {
            const daysSince = this._daysSinceLastSeen(c);
            const recency = daysSince < 7 ? 'recent' : daysSince < 30 ? `${daysSince}d ago` : 'not seen recently';
            lines.push(`- ${c.canonical_name} (${c.entity_type.toLowerCase()}, ${c.mention_count} mentions, ${recency})`);
        }
        lines.push('The agent may want to clarify which entity is being discussed.');
        return lines.join('\n');
    }

    /**
     * Merge two entities: keep the dominant canonical name, union aliases,
     * rewrite triples, clear pending_resolution flags.
     *
     * @param {string} keepId - Entity ID to keep
     * @param {string} mergeId - Entity ID to merge into keepId
     * @param {string} agentId
     * @returns {{ triplesUpdated: number }}
     */
    mergeEntities(keepId, mergeId, agentId) {
        const keep = this.store.getEntity(keepId);
        const merge = this.store.getEntity(mergeId);
        if (!keep || !merge) return { triplesUpdated: 0 };

        // Union aliases
        let keepAliases = [];
        let mergeAliases = [];
        try { keepAliases = JSON.parse(keep.aliases || '[]'); } catch { /* */ }
        try { mergeAliases = JSON.parse(merge.aliases || '[]'); } catch { /* */ }

        const allAliases = new Set([...keepAliases, ...mergeAliases, merge.canonical_name]);
        allAliases.delete(keep.canonical_name); // don't alias the canonical name

        // Update keep entity's aliases
        this.store.db.prepare(
            'UPDATE entities SET aliases = ?, mention_count = mention_count + ? WHERE id = ?'
        ).run(JSON.stringify([...allAliases]), merge.mention_count, keepId);

        // Rewrite triples: subject
        const subjectUpdated = this.store.db.prepare(
            'UPDATE triples SET subject = ?, updated_at = datetime(\'now\') WHERE subject = ? AND agent_id = ?'
        ).run(keepId, mergeId, agentId);

        // Rewrite triples: object
        const objectUpdated = this.store.db.prepare(
            'UPDATE triples SET object = ?, updated_at = datetime(\'now\') WHERE object = ? AND agent_id = ?'
        ).run(keepId, mergeId, agentId);

        // Clear pending_resolution on affected triples
        this.store.db.prepare(
            'UPDATE triples SET pending_resolution = 0 WHERE (subject = ? OR object = ?) AND agent_id = ?'
        ).run(keepId, keepId, agentId);

        // Delete merged entity
        this.store.db.prepare('DELETE FROM entities WHERE id = ?').run(mergeId);

        return { triplesUpdated: subjectUpdated.changes + objectUpdated.changes };
    }

    /**
     * Process pending resolution triples.
     * Called during nightshift to batch-resolve deferred entities.
     *
     * @param {string} agentId
     * @returns {{ resolved: number, expired: number }}
     */
    processPending(agentId) {
        const pending = this.store.db.prepare(
            `SELECT DISTINCT subject as entity_id FROM triples
             WHERE pending_resolution = 1 AND agent_id = ?
             UNION
             SELECT DISTINCT object as entity_id FROM triples
             WHERE pending_resolution = 1 AND agent_id = ?`
        ).all(agentId, agentId);

        let resolved = 0;
        let expired = 0;

        for (const row of pending) {
            const entity = this.store.getEntity(row.entity_id);
            if (!entity) continue;

            const daysSince = this._daysSinceLastSeen(entity);

            // If entity has been mentioned again recently, try to resolve
            if (entity.mention_count > 1 || daysSince < this.recencyBoostDays) {
                // Clear pending — accumulated context has strengthened the entity
                this.store.db.prepare(
                    'UPDATE triples SET pending_resolution = 0 WHERE (subject = ? OR object = ?) AND agent_id = ?'
                ).run(row.entity_id, row.entity_id, agentId);
                resolved++;
                continue;
            }

            // Old pending triples — assign to most likely candidate or keep as-is
            if (daysSince > this.pendingMaxAgeDays) {
                // Just clear the flag — entity stands on its own now
                this.store.db.prepare(
                    'UPDATE triples SET pending_resolution = 0 WHERE (subject = ? OR object = ?) AND agent_id = ?'
                ).run(row.entity_id, row.entity_id, agentId);
                expired++;
            }
        }

        return { resolved, expired };
    }

    /**
     * Get pending ask-tier notes for entities mentioned in the current query.
     * Called during before_agent_start to inject [GRAPH NOTE] hints.
     *
     * @param {Array<{name: string, type: string}>} queryEntities - Entities from the query
     * @param {string} agentId
     * @returns {string[]} Array of graph note strings to inject
     */
    getAskNotes(queryEntities, agentId) {
        const notes = [];

        for (const qe of queryEntities) {
            const result = this.resolve(qe.name, agentId,
                queryEntities.filter(e => e.name !== qe.name).map(e => e.name)
            );

            if (result.tier === 'ask' && result.graphNote) {
                notes.push(result.graphNote);
            }
        }

        return notes;
    }
}

module.exports = EntityResolver;
