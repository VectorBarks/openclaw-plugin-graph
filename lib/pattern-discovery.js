/**
 * Pattern Discovery — Find useful multi-hop traversal patterns from graph structure.
 *
 * Runs during nightshift. Analyzes which predicate sequences produce
 * meaningful connections that single-hop search would miss.
 *
 * Conservative by design: three-stage filtering (fanout cap, structural
 * viability, novelty validation) ensures only high-value patterns survive.
 *
 * Phase 5 of the knowledge graph plugin.
 */

class PatternDiscovery {
    /**
     * @param {GraphStore} store
     * @param {Object} config - patternDiscovery config section
     */
    constructor(store, config) {
        this.store = store;
        this.config = config || {};
        this.maxActivePatterns = this.config.maxActivePatterns || 12;
        this.maxStaticPatterns = this.config.maxStaticPatterns || 5;
        this.minYield = this.config.minYield || 3;
        this.maxOverlapRatio = this.config.maxOverlapRatio || 0.9;
        this.maxFanoutPerStep = this.config.maxFanoutPerStep || 50;
        this.maxCandidateLength = this.config.maxCandidateLength || 3;
    }

    /**
     * Run pattern discovery for an agent.
     * Returns summary of what was discovered.
     *
     * @param {string} agentId
     * @returns {{ candidates: number, viable: number, novel: number, saved: number, retired: number }}
     */
    discover(agentId) {
        const aid = agentId || 'main';
        const stats = { candidates: 0, viable: 0, novel: 0, saved: 0, retired: 0 };

        // 1. Get predicate statistics
        const predStats = this.store.getPredicateStats(aid);
        if (predStats.length < 2) return stats;

        // Only consider predicates with enough triples
        const qualifying = predStats.filter(p => p.cnt >= 5);
        if (qualifying.length < 2) return stats;

        // Compute per-predicate fanout
        const fanout = {};
        for (const p of qualifying) {
            fanout[p.predicate] = p.cnt / Math.max(p.unique_subjects, 1);
        }

        // 2. Generate 2-step candidates (ordered permutations)
        const predicateNames = qualifying.map(p => p.predicate);
        const candidates = [];

        for (const p1 of predicateNames) {
            for (const p2 of predicateNames) {
                candidates.push([p1, p2]);
            }
        }

        // Optionally generate 3-step candidates (only if <= 6 qualifying predicates)
        if (predicateNames.length <= 6 && this.maxCandidateLength >= 3) {
            for (const p1 of predicateNames) {
                for (const p2 of predicateNames) {
                    for (const p3 of predicateNames) {
                        candidates.push([p1, p2, p3]);
                    }
                }
            }
        }

        stats.candidates = candidates.length;

        // 3. Fanout cap filter
        const fanoutSurvivors = candidates.filter(preds => {
            // At least one predicate must have low fanout
            const hasLowFanout = preds.some(p => fanout[p] < 10);
            if (!hasLowFanout) return false;

            // Product of fanouts must be reasonable
            const product = preds.reduce((acc, p) => acc * (fanout[p] || 1), 1);
            const maxProduct = Math.pow(this.maxFanoutPerStep, preds.length);
            return product < maxProduct;
        });

        // 4. Skip patterns that are already active (static or discovered)
        const existing = this.store.getActivePatterns(aid);
        const existingKeys = new Set(existing.map(p => JSON.stringify(p.predicates)));

        const newCandidates = fanoutSurvivors.filter(preds => {
            return !existingKeys.has(JSON.stringify(preds));
        });

        // 5. Structural viability + novelty validation (most expensive)
        const scored = [];
        for (const preds of newCandidates) {
            const result = this._evaluatePattern(preds, aid);
            if (!result) continue;

            stats.viable++;

            if (result.overlapRatio <= this.maxOverlapRatio && result.yield >= this.minYield) {
                stats.novel++;
                scored.push({
                    predicates: preds,
                    yield: result.yield,
                    overlapRatio: result.overlapRatio,
                    avgConfidence: result.avgConfidence,
                    // Composite score: yield × novelty × confidence
                    score: result.yield * (1 - result.overlapRatio) * result.avgConfidence
                });
            }
        }

        // 6. Rank and select top N
        scored.sort((a, b) => b.score - a.score);
        const staticCount = existing.filter(p => p.type === 'static').length;
        const discoverySlots = this.maxActivePatterns - staticCount;
        const discoveredCount = existing.filter(p => p.type === 'discovered').length;
        const slotsAvailable = Math.max(0, discoverySlots - discoveredCount);
        const toSave = scored.slice(0, slotsAvailable);

        // 7. Save discovered patterns
        for (const p of toSave) {
            this.store.savePattern(
                aid,
                p.predicates,
                Math.min(p.score, 1.0),  // weight capped at 1.0
                p.yield,
                p.overlapRatio
            );
            stats.saved++;
        }

        return stats;
    }

    /**
     * Evaluate a candidate pattern for structural viability and novelty.
     *
     * @param {string[]} predicates - Predicate sequence to test
     * @param {string} agentId
     * @returns {{ yield: number, overlapRatio: number, avgConfidence: number } | null}
     */
    _evaluatePattern(predicates, agentId) {
        try {
            if (predicates.length === 2) {
                return this._evaluate2Step(predicates, agentId);
            } else if (predicates.length === 3) {
                return this._evaluate3Step(predicates, agentId);
            }
        } catch {
            return null;
        }
        return null;
    }

    _evaluate2Step(preds, agentId) {
        // Find entity pairs connected by this 2-step pattern
        // Recency weighting: paths through recently-updated triples score higher
        const pairRows = this.store.db.prepare(`
            SELECT DISTINCT t1.subject as src, t2.object as dst,
                   t1.confidence * t2.confidence *
                   (1.0 / (1.0 + (julianday('now') - julianday(t1.updated_at)) / 90.0)) *
                   (1.0 / (1.0 + (julianday('now') - julianday(t2.updated_at)) / 90.0))
                   as path_conf
            FROM triples t1
            JOIN triples t2
                ON t2.subject = t1.object
                AND t2.predicate = ?
                AND t2.agent_id = ?
            WHERE t1.predicate = ?
                AND t1.agent_id = ?
            LIMIT 200
        `).all(preds[1], agentId, preds[0], agentId);

        if (pairRows.length < this.minYield) return null;

        // Check how many are directly connected (single-hop)
        let directCount = 0;
        let totalConf = 0;
        for (const pair of pairRows) {
            totalConf += pair.path_conf;
            const direct = this.store.db.prepare(`
                SELECT 1 FROM triples
                WHERE subject = ? AND object = ? AND agent_id = ?
                LIMIT 1
            `).get(pair.src, pair.dst, agentId);
            if (direct) directCount++;
        }

        return {
            yield: pairRows.length,
            overlapRatio: pairRows.length > 0 ? directCount / pairRows.length : 1.0,
            avgConfidence: pairRows.length > 0 ? totalConf / pairRows.length : 0
        };
    }

    _evaluate3Step(preds, agentId) {
        // Recency weighting: paths through recently-updated triples score higher
        const pairRows = this.store.db.prepare(`
            SELECT DISTINCT t1.subject as src, t3.object as dst,
                   t1.confidence * t2.confidence * t3.confidence *
                   (1.0 / (1.0 + (julianday('now') - julianday(t1.updated_at)) / 90.0)) *
                   (1.0 / (1.0 + (julianday('now') - julianday(t2.updated_at)) / 90.0)) *
                   (1.0 / (1.0 + (julianday('now') - julianday(t3.updated_at)) / 90.0))
                   as path_conf
            FROM triples t1
            JOIN triples t2
                ON t2.subject = t1.object
                AND t2.predicate = ?
                AND t2.agent_id = ?
            JOIN triples t3
                ON t3.subject = t2.object
                AND t3.predicate = ?
                AND t3.agent_id = ?
            WHERE t1.predicate = ?
                AND t1.agent_id = ?
            LIMIT 200
        `).all(preds[1], agentId, preds[2], agentId, preds[0], agentId);

        if (pairRows.length < this.minYield) return null;

        let directCount = 0;
        let totalConf = 0;
        for (const pair of pairRows) {
            totalConf += pair.path_conf;
            const direct = this.store.db.prepare(`
                SELECT 1 FROM triples
                WHERE subject = ? AND object = ? AND agent_id = ?
                LIMIT 1
            `).get(pair.src, pair.dst, agentId);
            if (direct) directCount++;
        }

        return {
            yield: pairRows.length,
            overlapRatio: pairRows.length > 0 ? directCount / pairRows.length : 1.0,
            avgConfidence: pairRows.length > 0 ? totalConf / pairRows.length : 0
        };
    }

    /**
     * Re-validate existing discovered patterns.
     * Deactivates patterns whose overlap ratio has risen above threshold
     * (graph has grown, making previously-novel paths now directly reachable).
     *
     * @param {string} agentId
     * @returns {{ validated: number, retired: number }}
     */
    validatePatterns(agentId) {
        const aid = agentId || 'main';
        const patterns = this.store.getActivePatterns(aid);
        let validated = 0;
        let retired = 0;

        for (const pattern of patterns) {
            // Don't touch static patterns
            if (pattern.type === 'static') continue;

            const result = this._evaluatePattern(pattern.predicates, aid);
            if (!result || result.yield < this.minYield || result.overlapRatio > this.maxOverlapRatio) {
                this.store.deactivatePattern(pattern.id);
                retired++;
            } else {
                // Update scores
                this.store.savePattern(
                    aid,
                    pattern.predicates,
                    Math.min(result.yield * (1 - result.overlapRatio) * result.avgConfidence, 1.0),
                    result.yield,
                    result.overlapRatio
                );
                validated++;
            }
        }

        return { validated, retired };
    }
}

module.exports = PatternDiscovery;
