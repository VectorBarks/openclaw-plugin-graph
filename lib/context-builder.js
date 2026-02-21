/**
 * Context Builder — Generate natural-language relationship summaries.
 *
 * Produces the [GRAPH CONTEXT] block injected into agent context.
 * Queries the graph for top relationships of matched entities,
 * converts predicate triples to readable sentences, and caps output.
 *
 * Phase 5 of the knowledge graph plugin.
 */

const PREDICATE_TEMPLATES = {
    created:       (s, o) => `created ${o}`,
    uses:          (s, o) => `uses ${o}`,
    knows:         (s, o) => `knows ${o}`,
    works_on:      (s, o) => `works on ${o}`,
    interested_in: (s, o) => `is interested in ${o}`,
    located_in:    (s, o) => `is in ${o}`,
    part_of:       (s, o) => `is part of ${o}`,
    prefers:       (s, o) => `prefers ${o}`,
    causes:        (s, o) => `leads to ${o}`,
    has_property:  (s, o) => `has ${o}`,
    occurred_at:   (s, o) => `occurred at ${o}`,
    related_to:    (s, o) => `is related to ${o}`
};

class ContextBuilder {
    /**
     * @param {Object} config - contextInjection config section
     */
    constructor(config) {
        this.config = config || {};
        this.maxLines = this.config.maxLines || 5;
        this.minConfidence = this.config.minConfidence || 0.6;
        this.minMentionCount = this.config.minMentionCount || 2;
        this.excludePredicates = new Set(this.config.excludePredicates || ['related_to']);
    }

    /**
     * Build natural-language relationship summaries for query entities.
     *
     * @param {string[]} entityIds - Normalized entity IDs from query
     * @param {string} agentId
     * @param {GraphStore} store
     * @returns {string[]} Array of summary sentences
     */
    buildContext(entityIds, agentId, store) {
        if (!entityIds || entityIds.length === 0) return [];

        const aid = agentId || 'main';
        const placeholders = entityIds.map(() => '?').join(', ');

        // Fetch top triples for matched entities with canonical names
        const sql = `
            SELECT t.subject, t.predicate, t.object, t.confidence,
                   e_sub.canonical_name as sub_name,
                   e_obj.canonical_name as obj_name,
                   e_obj.mention_count as obj_mentions
            FROM triples t
            LEFT JOIN entities e_sub ON e_sub.id = t.subject AND e_sub.agent_id = ?
            LEFT JOIN entities e_obj ON e_obj.id = t.object AND e_obj.agent_id = ?
            WHERE (t.subject IN (${placeholders}) OR t.object IN (${placeholders}))
                AND t.agent_id = ?
                AND t.confidence >= ?
            ORDER BY t.confidence DESC, t.updated_at DESC
            LIMIT 50
        `;

        const params = [
            aid, aid,
            ...entityIds,
            ...entityIds,
            aid,
            this.minConfidence
        ];

        let rows;
        try {
            rows = store.db.prepare(sql).all(...params);
        } catch {
            return [];
        }

        if (rows.length === 0) return [];

        // Filter: exclude noisy predicates, require registered entities
        const filtered = rows.filter(r => {
            if (this.excludePredicates.has(r.predicate)) return false;
            // At least one side must have a canonical name
            if (!r.sub_name && !r.obj_name) return false;
            // Object entity should have minimum mentions (if registered)
            if (r.obj_mentions !== null && r.obj_mentions < this.minMentionCount) return false;
            return true;
        });

        // Deduplicate: keep highest confidence per (subject, predicate, object)
        const seen = new Set();
        const unique = [];
        for (const r of filtered) {
            const key = `${r.subject}|${r.predicate}|${r.object}`;
            if (!seen.has(key)) {
                seen.add(key);
                unique.push(r);
            }
        }

        // Group by subject for compound sentences
        const grouped = new Map();
        for (const r of unique) {
            const subName = r.sub_name || r.subject;
            if (!grouped.has(subName)) {
                grouped.set(subName, []);
            }
            grouped.get(subName).push(r);
        }

        // Build sentences
        const sentences = [];
        for (const [subName, triples] of grouped) {
            const clauses = [];
            for (const t of triples) {
                const objName = t.obj_name || t.object;
                const template = PREDICATE_TEMPLATES[t.predicate];
                if (template) {
                    clauses.push(template(subName, objName));
                }
            }

            if (clauses.length === 0) continue;

            // Compound sentence: "Chris created OpenClaw, uses DeepSeek, and knows Dan"
            let sentence;
            if (clauses.length === 1) {
                sentence = `${subName} ${clauses[0]}`;
            } else if (clauses.length === 2) {
                sentence = `${subName} ${clauses[0]} and ${clauses[1]}`;
            } else {
                const last = clauses.pop();
                sentence = `${subName} ${clauses.join(', ')}, and ${last}`;
            }

            sentences.push(sentence);
            if (sentences.length >= this.maxLines) break;
        }

        return sentences;
    }
}

module.exports = ContextBuilder;
