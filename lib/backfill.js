/**
 * Archive Backfill — Build graph from existing conversation archives.
 *
 * Reads continuity plugin's daily JSON archives and runs them through
 * the graph extractor. Processes in batches to avoid blocking.
 *
 * Two modes:
 *   - Fast path: compromise.js extraction (< 1ms per exchange)
 *   - Slow path: LLM enrichment (queued for nightshift)
 *
 * Tracks progress in a backfill-log.json file so it can resume
 * after restarts and skip already-processed dates.
 *
 * Phase 4 of the knowledge graph plugin.
 */

const fs = require('fs');
const path = require('path');

class ArchiveBackfill {
    /**
     * @param {Object} opts
     * @param {Object} opts.store - GraphStore instance
     * @param {Object} opts.extractor - extractor module (lib/extractor.js)
     * @param {Object} opts.extractionConfig - extraction config section
     * @param {string} opts.agentId - agent identifier
     * @param {string} opts.archiveDir - path to continuity archive directory
     * @param {string} opts.dataDir - graph plugin data directory (for backfill-log.json)
     * @param {Function} opts.logger - { info, warn, error, debug } logging methods
     */
    constructor(opts) {
        this.store = opts.store;
        this.extractor = opts.extractor;
        this.extractionConfig = opts.extractionConfig || {};
        this.agentId = opts.agentId;
        this.archiveDir = opts.archiveDir;
        this.dataDir = opts.dataDir;
        this.logger = opts.logger || console;

        this.logPath = path.join(this.dataDir, 'backfill-log.json');
        this.isRunning = false;
        this.stats = { processed: 0, skipped: 0, errors: 0, totalTriples: 0 };
    }

    /**
     * Get the set of dates already backfilled.
     * @returns {Set<string>}
     */
    getProcessedDates() {
        try {
            if (fs.existsSync(this.logPath)) {
                const log = JSON.parse(fs.readFileSync(this.logPath, 'utf8'));
                return new Set(log.dates || []);
            }
        } catch { /* start fresh */ }
        return new Set();
    }

    /**
     * Mark a date as processed in the backfill log.
     * @param {string} date - YYYY-MM-DD
     */
    _markProcessed(date) {
        let log = { dates: [], lastBackfill: null };
        try {
            if (fs.existsSync(this.logPath)) {
                log = JSON.parse(fs.readFileSync(this.logPath, 'utf8'));
            }
        } catch { /* start fresh */ }

        if (!log.dates) log.dates = [];
        if (!log.dates.includes(date)) {
            log.dates.push(date);
            log.dates.sort();
        }
        log.lastBackfill = new Date().toISOString();
        fs.writeFileSync(this.logPath, JSON.stringify(log, null, 2), 'utf8');
    }

    /**
     * Get list of archive dates available in the continuity archive dir.
     * @returns {string[]}
     */
    getAvailableDates() {
        try {
            if (!fs.existsSync(this.archiveDir)) return [];
            return fs.readdirSync(this.archiveDir)
                .filter(f => f.endsWith('.json'))
                .map(f => f.replace('.json', ''))
                .sort();
        } catch {
            return [];
        }
    }

    /**
     * Get dates that need backfilling (available but not yet processed).
     * @returns {string[]}
     */
    getUnprocessedDates() {
        const processed = this.getProcessedDates();
        return this.getAvailableDates().filter(d => !processed.has(d));
    }

    /**
     * Check if backfill is needed.
     * @returns {{ needed: boolean, unprocessed: number, available: number, graphTriples: number }}
     */
    checkStatus() {
        const available = this.getAvailableDates();
        const unprocessed = this.getUnprocessedDates();
        const stats = this.store.getStats(this.agentId);
        return {
            needed: unprocessed.length > 0,
            unprocessed: unprocessed.length,
            available: available.length,
            graphTriples: stats.tripleCount
        };
    }

    /**
     * Load and parse a single archive file.
     * @param {string} date - YYYY-MM-DD
     * @returns {{ messages: Array }|null}
     */
    _loadArchive(date) {
        const filePath = path.join(this.archiveDir, `${date}.json`);
        if (!fs.existsSync(filePath)) return null;
        try {
            return JSON.parse(fs.readFileSync(filePath, 'utf8'));
        } catch (err) {
            this.logger.warn(`[Backfill:${this.agentId}] Failed to read ${date}.json: ${err.message}`);
            return null;
        }
    }

    /**
     * Pair messages into user→agent exchanges (same logic as continuity indexer).
     * @param {Array} messages - archive messages
     * @returns {Array<{user: Object|null, agent: Object|null}>}
     */
    _pairExchanges(messages) {
        const exchanges = [];
        let current = { user: null, agent: null };

        for (const msg of messages) {
            if (msg.sender === 'user') {
                if (current.user) {
                    exchanges.push(current);
                    current = { user: null, agent: null };
                }
                current.user = msg;
            } else if (msg.sender === 'agent') {
                current.agent = msg;
                exchanges.push(current);
                current = { user: null, agent: null };
            }
        }

        if (current.user || current.agent) {
            exchanges.push(current);
        }

        return exchanges;
    }

    /**
     * Process a single day's archive through the fast-path extractor.
     * @param {string} date - YYYY-MM-DD
     * @returns {{ exchanges: number, entities: number, triples: number }}
     */
    processDay(date) {
        const archive = this._loadArchive(date);
        if (!archive || !archive.messages || archive.messages.length === 0) {
            return { exchanges: 0, entities: 0, triples: 0 };
        }

        const exchanges = this._pairExchanges(archive.messages);
        let totalEntities = 0;
        let totalTriples = 0;

        const knownEntities = (() => {
            try {
                return this.store._recentEntities.all(this.agentId, 200);
            } catch { return []; }
        })();

        for (let i = 0; i < exchanges.length; i++) {
            const exchange = exchanges[i];
            const userText = exchange.user?.text || '';
            const agentText = exchange.agent?.text || '';

            // Skip very short exchanges
            if ((userText.length + agentText.length) < (this.extractionConfig.minExchangeLength || 20)) {
                continue;
            }

            // Build messages array matching what extractFromExchange expects
            const messages = [];
            if (exchange.user) {
                messages.push({ role: 'user', content: userText });
            }
            if (exchange.agent) {
                messages.push({ role: 'assistant', content: agentText });
            }

            try {
                const extraction = this.extractor.extractFromExchange({
                    messages,
                    config: this.extractionConfig,
                    knownEntities
                });

                if (extraction.entities.length === 0) continue;

                const exchangeId = `exchange_${date}_${i}`;
                const tripleIds = this.store.writeExchange({
                    entities: extraction.entities,
                    triples: extraction.triples,
                    cooccurrences: extraction.cooccurrences,
                    agentId: this.agentId,
                    sourceExchangeId: exchangeId,
                    sourceDate: date
                });

                totalEntities += extraction.entities.length;
                totalTriples += tripleIds.length;
            } catch (err) {
                this.logger.warn(
                    `[Backfill:${this.agentId}] Error on ${date} exchange ${i}: ${err.message}`
                );
                this.stats.errors++;
            }
        }

        this._markProcessed(date);
        return { exchanges: exchanges.length, entities: totalEntities, triples: totalTriples };
    }

    /**
     * Run fast-path backfill for a batch of dates.
     * Non-blocking: processes `batchSize` dates then yields.
     *
     * @param {number} batchSize - dates to process per call (default 10)
     * @returns {{ processed: number, remaining: number, entities: number, triples: number }}
     */
    processBatch(batchSize = 10) {
        if (this.isRunning) {
            return { processed: 0, remaining: 0, entities: 0, triples: 0, skipped: true };
        }

        this.isRunning = true;
        const unprocessed = this.getUnprocessedDates();
        const batch = unprocessed.slice(0, batchSize);

        let totalEntities = 0;
        let totalTriples = 0;

        for (const date of batch) {
            const result = this.processDay(date);
            totalEntities += result.entities;
            totalTriples += result.triples;
            this.stats.processed++;
            this.stats.totalTriples += result.triples;
        }

        this.isRunning = false;

        return {
            processed: batch.length,
            remaining: unprocessed.length - batch.length,
            entities: totalEntities,
            triples: totalTriples
        };
    }

    /**
     * Run full fast-path backfill for all unprocessed dates.
     * Synchronous — suitable for startup or nightshift.
     *
     * @returns {{ processed: number, entities: number, triples: number, duration: number }}
     */
    processAll() {
        if (this.isRunning) {
            return { processed: 0, entities: 0, triples: 0, duration: 0 };
        }

        this.isRunning = true;
        const start = Date.now();
        const unprocessed = this.getUnprocessedDates();

        let totalEntities = 0;
        let totalTriples = 0;

        for (const date of unprocessed) {
            const result = this.processDay(date);
            totalEntities += result.entities;
            totalTriples += result.triples;
            this.stats.processed++;
            this.stats.totalTriples += result.triples;
        }

        this.isRunning = false;
        const duration = Date.now() - start;

        return {
            processed: unprocessed.length,
            entities: totalEntities,
            triples: totalTriples,
            duration
        };
    }

    /**
     * Clear backfill log and all triples — for full rebuild.
     * @returns {{ triplesDeleted: number, entitiesDeleted: number }}
     */
    reset() {
        // Delete backfill log
        try {
            if (fs.existsSync(this.logPath)) {
                fs.unlinkSync(this.logPath);
            }
        } catch { /* ok */ }

        // Delete all triples and entities for this agent
        const triplesDeleted = this.store.db.prepare(
            'DELETE FROM triples WHERE agent_id = ?'
        ).run(this.agentId).changes;

        const entitiesDeleted = this.store.db.prepare(
            'DELETE FROM entities WHERE agent_id = ?'
        ).run(this.agentId).changes;

        // Clear co-occurrences (shared table, but these are all from this agent's entities)
        this.store.db.prepare('DELETE FROM cooccurrences').run();

        this.stats = { processed: 0, skipped: 0, errors: 0, totalTriples: 0 };

        return { triplesDeleted, entitiesDeleted };
    }

    /**
     * Get backfill queue items for LLM slow-path enrichment.
     * Returns exchanges that were fast-path processed but haven't been LLM-enriched.
     *
     * @param {number} limit - max items to return
     * @returns {Array<{ exchangeId, userText, agentText, date }>}
     */
    getEnrichmentQueue(limit = 10) {
        const processed = this.getProcessedDates();
        const items = [];

        for (const date of processed) {
            const archive = this._loadArchive(date);
            if (!archive || !archive.messages) continue;

            const exchanges = this._pairExchanges(archive.messages);
            for (let i = 0; i < exchanges.length; i++) {
                const exchange = exchanges[i];
                const userText = exchange.user?.text || '';
                const agentText = exchange.agent?.text || '';

                if ((userText.length + agentText.length) < 20) continue;

                items.push({
                    exchangeId: `exchange_${date}_${i}`,
                    userText,
                    agentText,
                    date
                });

                if (items.length >= limit) return items;
            }
        }

        return items;
    }
}

module.exports = ArchiveBackfill;
