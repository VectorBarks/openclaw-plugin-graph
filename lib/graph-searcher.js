/**
 * Graph Searcher — Link expansion retrieval + scoring.
 *
 * Given a user query, extracts entities, traverses the graph, and returns
 * ranked exchange IDs for RRF fusion with continuity's semantic and keyword
 * search results.
 *
 * Phase 1: Single-hop link expansion.
 * Phase 5: Multi-hop recursive CTE traversal + meta-path pattern queries.
 */

const extractor = require('./extractor');

class GraphSearcher {
    /**
     * @param {GraphStore} store - Graph store instance
     * @param {Object} config - Retrieval config section
     */
    constructor(store, config) {
        this.store = store;
        this.config = config || {};

        this.maxResults = this.config.maxResults || 20;
        this.minSharedEntities = this.config.minSharedEntities || 1;
        this.cooccurrenceBoost = this.config.cooccurrenceBoost || 0.1;
        this.maxHops = this.config.maxHops || 1;
        this.hopDecay = this.config.hopDecay || 0.7;
        this.minTraversalConfidence = this.config.minTraversalConfidence || 0.6;
        this.metaPathWeight = this.config.metaPathWeight || 0.8;
    }

    /**
     * Run graph search for a query.
     * When maxHops > 1, uses multi-hop CTE + meta-path patterns.
     * Otherwise falls back to single-hop link expansion.
     *
     * @param {string} query - User's query text
     * @param {string} agentId - Agent ID
     * @param {Object} options
     * @param {number} options.limit - Max results
     * @param {Array} options.knownEntities - Known entities for gazetteer
     * @returns {{ exchanges: Array, entities: Array }}
     */
    search(query, agentId, options) {
        const opts = options || {};
        const aid = agentId || 'main';
        const limit = opts.limit || this.maxResults;

        // 1. Extract entities from the query
        const queryEntities = extractor.extractEntities(query, { minExchangeLength: 3 });

        // Also try gazetteer matching
        const gazetteered = extractor.matchGazetteer(query, opts.knownEntities || []);
        const entityNames = new Set(queryEntities.map(e => e.name.toLowerCase()));
        for (const g of gazetteered) {
            if (!entityNames.has(g.name.toLowerCase())) {
                queryEntities.push(g);
                entityNames.add(g.name.toLowerCase());
            }
        }

        if (queryEntities.length === 0) {
            return { exchanges: [], entities: [] };
        }

        // 2. Choose search strategy based on maxHops
        if (this.maxHops <= 1) {
            return this._singleHopSearch(queryEntities, aid, limit);
        }

        // Multi-hop: CTE traversal + meta-path patterns
        const entityIds = queryEntities.map(e => this.store.normalizeEntityId(e.name));

        const hopResults = this.searchMultiHop(entityIds, aid, this.maxHops, this.hopDecay);
        const patterns = this.store.getActivePatterns(aid);
        const metaResults = this.searchMetaPaths(entityIds, aid, patterns);

        // Merge: max(hopScore, metaScore) per exchange
        const merged = new Map();
        for (const [exId, score] of hopResults) {
            merged.set(exId, score);
        }
        for (const [exId, score] of metaResults) {
            const existing = merged.get(exId) || 0;
            merged.set(exId, Math.max(existing, score));
        }

        // Format into standard output
        const results = [];
        for (const [exId, score] of merged) {
            results.push({
                id: exId,
                score,
                sharedEntityCount: entityIds.length,
                sharedEntities: entityIds,
                maxConfidence: score,
                date: null
            });
        }

        results.sort((a, b) => b.score - a.score);

        return {
            exchanges: results.slice(0, limit),
            entities: queryEntities
        };
    }

    /**
     * Original single-hop link expansion (Phase 1).
     * Kept as fallback when maxHops <= 1.
     */
    _singleHopSearch(queryEntities, agentId, limit) {
        const exchangeScores = new Map();

        for (const entity of queryEntities) {
            const triples = this.store.getTriplesFor(entity.name, agentId, 100);

            for (const triple of triples) {
                if (!triple.source_exchange_id) continue;

                const exchangeId = triple.source_exchange_id;
                if (!exchangeScores.has(exchangeId)) {
                    exchangeScores.set(exchangeId, {
                        id: exchangeId,
                        score: 0,
                        sharedEntities: new Set(),
                        maxConfidence: 0,
                        newestDate: null
                    });
                }

                const entry = exchangeScores.get(exchangeId);
                entry.sharedEntities.add(entity.name.toLowerCase());
                entry.score += triple.confidence || 1.0;
                entry.maxConfidence = Math.max(entry.maxConfidence, triple.confidence || 1.0);
                if (!entry.newestDate || triple.source_date > entry.newestDate) {
                    entry.newestDate = triple.source_date;
                }
            }

            // Co-occurrence expansion
            const cooccurrences = this.store.getCooccurrences(entity.name, 5);

            for (const cooc of cooccurrences) {
                const normalizedName = this.store.normalizeEntityId(entity.name);
                const coocEntityId = cooc.entity_a === normalizedName ? cooc.entity_b : cooc.entity_a;
                const coocTriples = this._getTriplesByNormalizedId(coocEntityId, agentId, 20);

                for (const triple of coocTriples) {
                    if (!triple.source_exchange_id) continue;

                    const exchangeId = triple.source_exchange_id;
                    if (!exchangeScores.has(exchangeId)) {
                        exchangeScores.set(exchangeId, {
                            id: exchangeId,
                            score: 0,
                            sharedEntities: new Set(),
                            maxConfidence: 0,
                            newestDate: null
                        });
                    }

                    const entry = exchangeScores.get(exchangeId);
                    entry.score += this.cooccurrenceBoost * (cooc.count || 1);
                    if (!entry.newestDate || triple.source_date > entry.newestDate) {
                        entry.newestDate = triple.source_date;
                    }
                }
            }
        }

        const results = [];
        for (const [, entry] of exchangeScores) {
            if (entry.sharedEntities.size >= this.minSharedEntities) {
                results.push({
                    id: entry.id,
                    score: entry.score,
                    sharedEntityCount: entry.sharedEntities.size,
                    sharedEntities: Array.from(entry.sharedEntities),
                    maxConfidence: entry.maxConfidence,
                    date: entry.newestDate
                });
            }
        }

        results.sort((a, b) => b.score - a.score);

        return {
            exchanges: results.slice(0, limit),
            entities: queryEntities
        };
    }

    /**
     * Multi-hop traversal using recursive CTE.
     * Follows entity chains up to maxHops deep with confidence decay.
     *
     * @param {string[]} entityIds - Normalized entity IDs from query
     * @param {string} agentId
     * @param {number} maxHops
     * @param {number} decay - Score decay per hop (default 0.7)
     * @returns {Map<string, number>} exchangeId → score
     */
    searchMultiHop(entityIds, agentId, maxHops, decay) {
        if (entityIds.length === 0) return new Map();

        const placeholders = entityIds.map(() => '?').join(', ');
        const d = decay || this.hopDecay;
        const minConf = this.minTraversalConfidence;

        // Build the recursive CTE dynamically (variable seed count)
        const sql = `
            WITH RECURSIVE hop(entity, depth, path, source_exchange_id, score) AS (
                -- Seed: triples touching any query entity
                SELECT
                    CASE WHEN t.subject IN (${placeholders}) THEN t.object ELSE t.subject END,
                    1,
                    '/' || t.subject || '/' || t.predicate || '/' || t.object || '/',
                    t.source_exchange_id,
                    t.confidence * ?
                FROM triples t
                WHERE (t.subject IN (${placeholders}) OR t.object IN (${placeholders}))
                    AND t.agent_id = ?
                    AND t.confidence >= ?

                UNION ALL

                -- Recurse: follow outgoing edges with decay
                SELECT
                    CASE WHEN t.subject = h.entity THEN t.object ELSE t.subject END,
                    h.depth + 1,
                    h.path || t.predicate || '/' ||
                        CASE WHEN t.subject = h.entity THEN t.object ELSE t.subject END || '/',
                    t.source_exchange_id,
                    h.score * ?
                FROM triples t
                JOIN hop h ON (t.subject = h.entity OR t.object = h.entity)
                WHERE h.depth < ?
                    AND t.agent_id = ?
                    AND t.confidence >= ?
                    -- Entity quality gate: intermediate must be registered
                    AND EXISTS (SELECT 1 FROM entities e WHERE e.id = h.entity AND e.agent_id = ?)
                    -- Cycle prevention
                    AND h.path NOT LIKE '%/' ||
                        CASE WHEN t.subject = h.entity THEN t.object ELSE t.subject END || '/%'
            )
            SELECT source_exchange_id, SUM(score) as total_score, MIN(depth) as min_depth
            FROM hop
            WHERE source_exchange_id IS NOT NULL
            GROUP BY source_exchange_id
            ORDER BY total_score DESC
            LIMIT ?
        `;

        // Build params: seed entities appear 3 times (subject IN, subject IN, object IN)
        const params = [
            ...entityIds,           // CASE WHEN subject IN
            d,                      // confidence * decay
            ...entityIds,           // WHERE subject IN
            ...entityIds,           // OR object IN
            agentId,                // agent_id
            minConf,                // confidence >= threshold
            d,                      // recursive: score * decay
            maxHops,                // depth < maxHops
            agentId,                // recursive agent_id
            minConf,                // recursive confidence
            agentId,                // entity quality gate agent_id
            this.maxResults * 2     // LIMIT (fetch extra, let merge handle final limit)
        ];

        const results = new Map();
        try {
            const rows = this.store.db.prepare(sql).all(...params);
            for (const row of rows) {
                results.set(row.source_exchange_id, row.total_score);
            }
        } catch (err) {
            // Graceful degradation — if CTE fails, return empty
            // (can happen with unusual graph structures)
        }

        return results;
    }

    /**
     * Meta-path pattern traversal.
     * Each pattern is a sequence of predicates to follow as fixed-depth JOINs.
     *
     * @param {string[]} entityIds - Normalized entity IDs from query
     * @param {string} agentId
     * @param {Array} patterns - Active patterns [{predicates: string[], weight: number}]
     * @returns {Map<string, number>} exchangeId → score
     */
    searchMetaPaths(entityIds, agentId, patterns) {
        const results = new Map();
        if (entityIds.length === 0 || !patterns || patterns.length === 0) return results;

        const placeholders = entityIds.map(() => '?').join(', ');

        for (const pattern of patterns) {
            const preds = pattern.predicates;
            if (!preds || preds.length < 2 || preds.length > 3) continue;

            try {
                let sql, params;

                if (preds.length === 2) {
                    // 2-step: seed → p1 → intermediate → p2 → target
                    sql = `
                        SELECT t2.source_exchange_id,
                               t1.confidence * t2.confidence * ? as score
                        FROM triples t1
                        JOIN triples t2
                            ON t2.subject = t1.object
                            AND t2.predicate = ?
                            AND t2.agent_id = ?
                        WHERE t1.subject IN (${placeholders})
                            AND t1.predicate = ?
                            AND t1.agent_id = ?
                            AND t2.source_exchange_id IS NOT NULL
                        LIMIT ?
                    `;
                    params = [
                        pattern.weight,     // score multiplier
                        preds[1],           // t2 predicate
                        agentId,            // t2 agent_id
                        ...entityIds,       // t1.subject IN
                        preds[0],           // t1 predicate
                        agentId,            // t1 agent_id
                        this.maxResults     // LIMIT
                    ];
                } else {
                    // 3-step: seed → p1 → int1 → p2 → int2 → p3 → target
                    sql = `
                        SELECT t3.source_exchange_id,
                               t1.confidence * t2.confidence * t3.confidence * ? as score
                        FROM triples t1
                        JOIN triples t2
                            ON t2.subject = t1.object
                            AND t2.predicate = ?
                            AND t2.agent_id = ?
                        JOIN triples t3
                            ON t3.subject = t2.object
                            AND t3.predicate = ?
                            AND t3.agent_id = ?
                        WHERE t1.subject IN (${placeholders})
                            AND t1.predicate = ?
                            AND t1.agent_id = ?
                            AND t3.source_exchange_id IS NOT NULL
                        LIMIT ?
                    `;
                    params = [
                        pattern.weight,     // score multiplier
                        preds[1],           // t2 predicate
                        agentId,            // t2 agent_id
                        preds[2],           // t3 predicate
                        agentId,            // t3 agent_id
                        ...entityIds,       // t1.subject IN
                        preds[0],           // t1 predicate
                        agentId,            // t1 agent_id
                        this.maxResults     // LIMIT
                    ];
                }

                const rows = this.store.db.prepare(sql).all(...params);
                for (const row of rows) {
                    if (!row.source_exchange_id) continue;
                    const existing = results.get(row.source_exchange_id) || 0;
                    results.set(row.source_exchange_id, Math.max(existing, row.score));
                }
            } catch {
                // Skip patterns that fail (e.g., no matching triples)
            }
        }

        return results;
    }

    /**
     * Look up triples by already-normalized entity ID.
     */
    _getTriplesByNormalizedId(normalizedId, agentId, limit) {
        const aid = agentId || 'main';
        const lim = limit || 50;
        const sql = `
            SELECT * FROM triples
            WHERE (subject = ? OR object = ?) AND agent_id = ?
            ORDER BY updated_at DESC
            LIMIT ?
        `;
        return this.store.db.prepare(sql).all(normalizedId, normalizedId, aid, lim);
    }

    /**
     * Get entity details + surrounding graph context.
     * Used by the graph.getEntity gateway method.
     */
    getEntityContext(entityName, agentId) {
        const aid = agentId || 'main';
        const id = this.store.normalizeEntityId(entityName);
        const entity = this.store.getEntity(id);
        if (!entity) return null;

        const triples = this.store.getTriplesFor(entityName, aid, 50);
        const cooccurrences = this.store.getCooccurrences(entityName, 10);

        const relationships = {};
        for (const t of triples) {
            if (!relationships[t.predicate]) {
                relationships[t.predicate] = [];
            }
            relationships[t.predicate].push({
                subject: t.subject,
                object: t.object,
                confidence: t.confidence,
                date: t.source_date
            });
        }

        return {
            entity,
            relationships,
            cooccurrences: cooccurrences.map(c => ({
                entity: c.entity_a === id ? c.entity_b : c.entity_a,
                count: c.count,
                lastSeen: c.last_seen
            })),
            tripleCount: triples.length
        };
    }
}

module.exports = GraphSearcher;
