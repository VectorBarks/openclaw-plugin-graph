/**
 * LLM Entity Extractor — Structured extraction via local Ollama model.
 *
 * Phase 3 slow path: runs async during nightshift idle time.
 * Extracts entities + relationships with coreference resolution.
 * Higher quality than Phase 1's compromise.js fast path.
 *
 * Uses the same Ollama endpoint as metabolism's processor.
 */

const http = require('http');

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
knows, visits, weighs, eats, exercises, plans, uses, works_on, located_in, part_of, interested_in, prefers, has_property, related_to, created, does, buys, feels

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
    'interested_in', 'prefers', 'works_on', 'related_to',
    'has_property', 'occurred_at', 'causes',
    'visits', 'weighs', 'eats', 'exercises', 'plans',
    'does', 'buys', 'feels'
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
    }

    /**
     * Extract entities + relationships from an exchange via LLM.
     *
     * @param {string} userText - User's message text
     * @param {string} agentText - Agent's response text
     * @returns {Promise<{entities: Array, relationships: Array}>}
     */
    async extract(userText, agentText) {
        const exchangeText = `User: ${(userText || '').substring(0, 3000)}\nAgent: ${(agentText || '').substring(0, 3000)}`;
        const prompt = EXTRACTION_PROMPT + exchangeText;

        const raw = await this._callOllama(prompt);
        return this._parseResponse(raw);
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

        // Validate entities
        const entities = [];
        for (const e of (json.entities || [])) {
            if (!e.name || typeof e.name !== 'string') continue;
            const name = e.name.trim();
            if (name.length < 2) continue;

            const type = VALID_TYPES.has(e.type) ? e.type : 'CONCEPT';
            const aliases = Array.isArray(e.aliases)
                ? e.aliases.filter(a => typeof a === 'string' && a.trim().length > 0)
                : [];

            entities.push({ name, type, aliases });
        }

        // Validate relationships
        const relationships = [];
        for (const r of (json.relationships || [])) {
            if (!r.subject || !r.predicate || !r.object) continue;
            const subject = String(r.subject).trim();
            const predicate = VALID_PREDICATES.has(r.predicate) ? r.predicate : 'related_to';
            const object = String(r.object).trim();

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
