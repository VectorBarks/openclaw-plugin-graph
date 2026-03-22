/**
 * LLM Entity Extractor — Structured extraction via local Ollama model.
 *
 * Phase 3 slow path: runs async during nightshift idle time.
 * Extracts entities + relationships with coreference resolution.
 * Higher quality than Phase 1's compromise.js fast path.
 *
 * Uses the same Ollama endpoint as metabolism's processor.
 *
 * Phase 6: Retry with exponential backoff + persistent failed-extraction queue.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

const EXTRACTION_PROMPT = `Extract knowledge graph triples from a conversation between Vector (the user) and Saphira (the AI assistant/dragon).

IMPORTANT: The "User:" speaker IS Vector. The "Agent:" speaker IS Saphira. Always resolve accordingly.

Extract facts about:
- Activities: what Vector does, eats, visits, exercises, plans, buys
- Health: weight, fitness, diet, supplements, symptoms
- People: who is mentioned, relationships
- Places: locations visited or referenced
- Projects/Events: Furvaria, Fenrir, work tasks, plans
- Preferences: likes, dislikes, choices
- Saphira's decisions or rules about their dynamic

Return JSON with entities and relationships. Use these predicates:
knows, visits, weighs, eats, exercises, plans, uses, works_on, located_in, part_of, interested_in, prefers, has_property, created, does, buys, feels, owns, meets, member_of, travels_to, takes, organizes, wears, controls, suffers_from, dislikes, invested_in, delegates_to, lives_with, likes, enjoys, hates, loves, wants, avoids, fears, trusts, sleeps, drinks

Prefer specific predicates. If no specific predicate fits, use "has_property". Never use "related_to".

Return EMPTY arrays ONLY if the text is:
- Pure code/shell output with zero personal content
- System messages (heartbeats, cron outputs, stability contexts)
- Pure technical documentation without personal facts

Keep entities short (1-3 words). Resolve "ich/I" to "Vector" (if user) or "Saphira" (if agent).

Return ONLY valid JSON:
{"entities":[{"name":"X","type":"PERSON|PLACE|ORGANIZATION|THING|CONCEPT|EVENT","aliases":[]}],"relationships":[{"subject":"X","predicate":"Y","object":"Z","confidence":0.9}]}

Exchange:
`;

const VALID_PREDICATES = new Set([
    'knows', 'created', 'uses', 'located_in', 'part_of',
    'interested_in', 'prefers', 'works_on',
    'has_property', 'occurred_at', 'causes',
    'visits', 'weighs', 'eats', 'exercises', 'plans',
    'does', 'buys', 'feels', 'owns', 'meets', 'member_of',
    'travels_to', 'takes', 'organizes', 'wears', 'controls',
    'suffers_from', 'dislikes', 'invested_in', 'delegates_to',
    'lives_with', 'likes', 'enjoys', 'hates', 'loves', 'wants',
    'avoids', 'fears', 'trusts', 'sleeps', 'drinks'
]);

const VALID_TYPES = new Set([
    'PERSON', 'ORGANIZATION', 'PLACE', 'CONCEPT', 'THING', 'DATE', 'EVENT'
]);

class LLMExtractor {
    constructor(config) {
        this.ollamaHost = config?.ollamaHost || 'localhost';
        this.ollamaPort = config?.ollamaPort || 3000;
        this.model = config?.model || 'claude-haiku-4-5';
        this.temperature = config?.temperature ?? 0.3; // low temp for structured output
        this.maxTokens = config?.maxTokens || 1000;
        this.timeoutMs = config?.timeoutMs || 45000;
        this.maxRetries = config?.maxRetries || 3;
        this._logger = null; // Set by index.js after construction
    }

    /**
     * Set logger for [Graph:WARN] alerts.
     */
    setLogger(logger) {
        this._logger = logger;
    }

    /**
     * Get the failed-extractions.json path for an agent.
     */
    _failedQueuePath(agentId) {
        const id = agentId || 'main';
        const dir = id === 'main'
            ? path.join(__dirname, '..', 'data')
            : path.join(__dirname, '..', 'data', 'agents', id);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        return path.join(dir, 'failed-extractions.json');
    }

    /**
     * Read failed-extraction queue from disk.
     */
    readFailedQueue(agentId) {
        const filePath = this._failedQueuePath(agentId);
        try {
            if (fs.existsSync(filePath)) {
                return JSON.parse(fs.readFileSync(filePath, 'utf8'));
            }
        } catch { /* corrupted file — return empty */ }
        return [];
    }

    /**
     * Write failed-extraction queue to disk.
     */
    _writeFailedQueue(agentId, queue) {
        const filePath = this._failedQueuePath(agentId);
        fs.writeFileSync(filePath, JSON.stringify(queue, null, 2), 'utf8');
    }

    /**
     * Append a failed extraction to the persistent queue.
     */
    _appendToFailedQueue(agentId, entry) {
        const queue = this.readFailedQueue(agentId);
        queue.push(entry);
        this._writeFailedQueue(agentId, queue);
        if (this._logger) {
            this._logger.warn(
                `[Graph:WARN] Extraction failed after ${entry.retries} retries for ${entry.exchangeId}: ${entry.error}`
            );
        }
    }

    /**
     * Remove successfully retried entries from the failed queue.
     */
    removeFromFailedQueue(agentId, exchangeIds) {
        const queue = this.readFailedQueue(agentId);
        const filtered = queue.filter(e => !exchangeIds.includes(e.exchangeId));
        this._writeFailedQueue(agentId, filtered);
    }

    /**
     * Check failed queue size and log warning if threshold exceeded.
     */
    checkFailedQueueOnStartup(agentId) {
        const queue = this.readFailedQueue(agentId);
        if (queue.length > 5 && this._logger) {
            this._logger.warn(
                `[Graph] ${queue.length} failed extractions in queue — run graph.retryFailed`
            );
        }
        return queue.length;
    }

    /**
     * Extract entities + relationships from an exchange via LLM.
     * Uses retry with exponential backoff (2s → 4s → 8s).
     *
     * @param {string} userText - User's message text
     * @param {string} agentText - Agent's response text
     * @returns {Promise<{entities: Array, relationships: Array}>}
     */
    async extract(userText, agentText) {
        const exchangeText = `User: ${(userText || '').substring(0, 3000)}\nAgent: ${(agentText || '').substring(0, 3000)}`;
        const prompt = EXTRACTION_PROMPT + exchangeText;

        const raw = await this._callWithRetry(prompt);
        return this._parseResponse(raw);
    }

    /**
     * Extract with retry + failed-queue integration.
     * On permanent failure, writes to failed-extractions.json.
     *
     * @param {string} userText
     * @param {string} agentText
     * @param {string} exchangeId - For failed-queue tracking
     * @param {string} agentId - For per-agent failed queue
     * @param {string} date - Source date
     * @returns {Promise<{entities: Array, relationships: Array}>}
     */
    async extractWithFailedQueue(userText, agentText, exchangeId, agentId, date) {
        const exchangeText = `User: ${(userText || '').substring(0, 3000)}\nAgent: ${(agentText || '').substring(0, 3000)}`;
        const prompt = EXTRACTION_PROMPT + exchangeText;

        try {
            const raw = await this._callWithRetry(prompt);
            return this._parseResponse(raw);
        } catch (err) {
            // All retries exhausted — write to failed queue
            this._appendToFailedQueue(agentId, {
                exchangeId: exchangeId || `exchange_${Date.now()}`,
                date: date || new Date().toISOString().split('T')[0],
                failedAt: new Date().toISOString(),
                error: err.message || err.code || String(err),
                retries: this.maxRetries,
                userTextPreview: (userText || '').substring(0, 100)
            });
            throw err;
        }
    }

    /**
     * Call LLM with exponential backoff retry (2s → 4s → 8s).
     * Only retries on timeout/network errors, not parse failures.
     */
    async _callWithRetry(prompt) {
        for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
            try {
                return await this._callOllama(prompt);
            } catch (err) {
                const errText = `${err.message || ''} ${err.code || ''}`;
                const isRetryable = /timeout|ECONNREFUSED|ECONNRESET|EPIPE|EAI_AGAIN|socket hang up/i.test(errText);
                if (!isRetryable || attempt === this.maxRetries) throw err;

                const delay = Math.pow(2, attempt) * 1000; // 2s, 4s, 8s
                if (this._logger) {
                    this._logger.info(
                        `[Graph] LLM retry ${attempt}/${this.maxRetries} after ${delay}ms: ${err.message}`
                    );
                }
                await new Promise(r => setTimeout(r, delay));
            }
        }
    }

    /**
     * Call Ollama's /api/generate endpoint (non-streaming).
     * Uses raw http to avoid requiring axios as a dependency.
     */
    _callOllama(prompt) {
        return new Promise((resolve, reject) => {
            const body = JSON.stringify({
                model: this.model,
                messages: [{ role: 'user', content: prompt }],
                temperature: this.temperature,
                max_tokens: this.maxTokens,
                stream: false
            });

            const req = http.request({
                hostname: this.ollamaHost,
                port: this.ollamaPort,
                path: '/v1/chat/completions',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(body)
                },
                timeout: this.timeoutMs
            }, (res) => {
                let data = '';
                res.on('data', chunk => { data += chunk; });
                res.on('end', () => {
                    try {
                        const parsed = JSON.parse(data);
                        resolve(parsed?.choices?.[0]?.message?.content || '');
                    } catch {
                        reject(new Error(`Invalid MLX response: ${data.substring(0, 200)}`));
                    }
                });
            });

            req.on('error', reject);
            req.on('timeout', () => {
                req.destroy();
                reject(new Error('Ollama request timed out'));
            });

            req.write(body);
            req.end();
        });
    }

    /**
     * Parse LLM response into validated entities + relationships.
     * Handles malformed JSON gracefully.
     */
    _parseResponse(raw) {
        const empty = { entities: [], relationships: [] };
        if (!raw || raw.length < 10) return empty;

        // Strip markdown code fences (Haiku often wraps JSON in ```json blocks)
        let cleaned = raw;
        const fenceMatch = cleaned.match(/```(?:json)?\s*([\s\S]*?)```/);
        if (fenceMatch) {
            cleaned = fenceMatch[1].trim();
        }

        // Try to find JSON in the response (LLM may include preamble text)
        let json;
        const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
        if (!jsonMatch) return empty;

        try {
            json = JSON.parse(jsonMatch[0]);
        } catch {
            // Try fixing common JSON issues: trailing commas, single quotes
            try {
                const fixed = jsonMatch[0]
                    .replace(/,\s*([}\]])/g, '$1')
                    .replace(/'/g, '"');
                json = JSON.parse(fixed);
            } catch {
                return empty;
            }
        }

        // Validate entities (handle name/id variations from LLM)
        const entities = [];
        for (const e of (json.entities || [])) {
            const rawName = e.name || e.id || e.canonical_name;
            if (!rawName || typeof rawName !== 'string') continue;
            const name = rawName.trim();
            if (name.length < 2) continue;

            // Normalize type: "Person" → "PERSON", "Object" → "THING", etc.
            const typeMap = { 'person': 'PERSON', 'object': 'THING', 'ai assistant': 'PERSON',
                'location': 'PLACE', 'activity': 'EVENT', 'supplement': 'THING',
                'body part': 'THING', 'condition': 'CONCEPT' };
            const rawType = (e.type || 'CONCEPT').toUpperCase();
            const type = VALID_TYPES.has(rawType) ? rawType
                : (typeMap[(e.type || '').toLowerCase()] || 'CONCEPT');
            const aliases = Array.isArray(e.aliases)
                ? e.aliases.filter(a => typeof a === 'string' && a.trim().length > 0)
                : [];

            entities.push({ name, type, aliases });
        }

        // Validate relationships (handle subject/source and object/target variations)
        const relationships = [];
        for (const r of (json.relationships || [])) {
            const rawSubject = r.subject || r.source || r.from;
            const rawObject = r.object || r.target || r.to;
            if (!rawSubject || !r.predicate || !rawObject) continue;
            const subject = String(rawSubject).trim();
            const predicate = VALID_PREDICATES.has(r.predicate) ? r.predicate : 'has_property';
            const object = String(rawObject).trim();

            if (subject.length < 2 || object.length < 2) continue;
            if (subject.toLowerCase() === object.toLowerCase()) continue;

            const confidence = typeof r.confidence === 'number'
                ? Math.max(0, Math.min(1, r.confidence))
                : 0.8;

            relationships.push({ subject, predicate, object, confidence });
        }

        return { entities, relationships };
    }
}

module.exports = LLMExtractor;
