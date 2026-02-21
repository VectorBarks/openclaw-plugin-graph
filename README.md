# openclaw-plugin-graph

**Memory Graph** — a knowledge graph that learns the relationships in your conversations.

Extracts entities and relationships from every conversation, stores them as triples in a graph database, and uses that graph to surface contextually relevant connections the agent wouldn't otherwise recall. When a user mentions "Chris," the agent doesn't just search for exchanges containing that name — it traverses the graph to find what Chris created, who Chris knows, and what those people work on.

## What it does

- **Entity extraction**: Pulls people, places, organizations, dates, URLs, and concepts from conversation text using compromise.js NLP + regex patterns. Builds a gazetteer of known entities that improves extraction over time.
- **Relationship triples**: Stores `subject → predicate → object` triples with confidence scores, exchange provenance, and mention counts. 12 canonical predicates: `knows`, `created`, `uses`, `works_on`, `interested_in`, `located_in`, `part_of`, `prefers`, `related_to`, `has_property`, `occurred_at`, `causes`.
- **Multi-hop traversal**: Recursive CTE walks the graph up to N hops (default 2), finding indirect connections with score decay per hop. "Chris knows Dan, Dan created the analytics dashboard" surfaces in a query about Chris even though Chris never mentioned that dashboard.
- **Meta-path patterns**: Predicate-sequence patterns like `[knows, works_on]` or `[created, part_of]` find structurally meaningful paths. Ships with 5 static defaults; more are discovered automatically.
- **Pattern discovery**: During nightshift, the plugin evaluates candidate predicate sequences against the actual graph data. Filters by fanout cap, structural viability (minimum yield), and novelty (overlap with single-hop results). Only patterns that surface genuinely new connections survive.
- **Graph-aware context injection**: Before each conversation, injects a `[GRAPH CONTEXT]` block with natural-language relationship summaries — "Chris created the analytics dashboard, uses React, and knows Dan" — so the agent has relational awareness without being told.
- **LLM enrichment**: Queues exchanges for deeper extraction via LLM during nightshift. Produces cleaner, more nuanced triples than the fast-path NLP extractor.
- **Conversational entity resolution**: When the graph encounters ambiguous entities ("is this the same Bob?"), it uses a 3-tier approach — high confidence matches are assumed, low confidence ones prompt the agent to ask the user naturally, medium confidence ones are deferred for later resolution.
- **Archive backfill**: On first run, scans existing continuity archives and builds the graph retroactively. No conversations are lost just because the plugin was installed later.
- **RRF fusion with continuity**: Graph search results feed into continuity's Reciprocal Rank Fusion as a third ranked list alongside semantic and keyword search. Exchanges connected by graph relationships get boosted in recall.

## How it works

### Data flow

```
User message arrives
        │
        ▼
before_agent_start (priority 8)
        │
        ├── Extract entities from user message
        │     └── compromise.js NER + gazetteer matching
        │
        ├── Graph search (multi-hop + meta-path)
        │     ├── Recursive CTE traversal (bidirectional, N hops)
        │     ├── Meta-path pattern matching (static + discovered)
        │     └── Merge: max(hopScore, metaScore) per exchange
        │
        ├── Build [GRAPH CONTEXT] block
        │     └── Predicate-to-language templates → compound sentences
        │
        ├── Build [GRAPH NOTE] (entity resolution hints)
        │
        └── Publish results to global bus → continuity RRF
              └── global.__ocGraph.lastResults[agentId]

Model processes message + generates response

agent_end
        │
        ├── Fast-path extraction (compromise.js)
        │     ├── Entity extraction + triple generation
        │     └── Co-occurrence detection
        │
        ├── Queue for LLM enrichment (if enabled)
        │     └── Nightshift picks up queue → deeper extraction
        │
        └── Entity resolution (3-tier)
              ├── Assume (confidence > 0.8)
              ├── Ask (confidence < 0.4) → [GRAPH NOTE] next turn
              └── Defer (0.4–0.8) → pending_resolution

nightshift (graph-enrichment task, priority 35)
        │
        ├── LLM extraction on queued exchanges
        ├── Entity resolution on pending merges
        ├── Pattern discovery (every 24h)
        │     └── Candidate generation → fanout filter → viability → novelty
        └── Gap detection (under-connected entities)
```

### Storage

| Component | Location | Format |
|-----------|----------|--------|
| Graph database | `data/agents/{agentId}/graph.db` | SQLite (entities, triples, co-occurrences, meta-patterns) |
| Entity gazetteer | In-memory, built from DB | Maps normalized names → entity records |

Each agent gets its own isolated graph database. The plugin supports multiple agents on the same gateway without data mixing.

## Installation

### Prerequisites

- OpenClaw installed and running (gateway active)
- Node.js >= 18

### Install dependencies

```bash
cd /path/to/openclaw-plugin-graph
npm install
```

This installs:
- `better-sqlite3` — synchronous SQLite driver
- `compromise` — NLP library for entity extraction

### Configure OpenClaw

Add the plugin to your `~/.openclaw/openclaw.json`:

```json
{
  "plugins": {
    "load": {
      "paths": [
        "/path/to/openclaw-plugin-graph"
      ]
    },
    "entries": {
      "openclaw-plugin-graph": {
        "enabled": true,
        "config": {}
      }
    }
  }
}
```

### Restart the gateway

```bash
openclaw gateway restart
```

Verify the plugin loaded:

```bash
openclaw logs | grep "Graph plugin"
```

You should see:
```
Graph plugin registered — Phases 1-5: extraction + multi-hop traversal + meta-paths + context injection active
```

## Architecture

### Modules

```
index.js                     Main plugin — hook registration, orchestration
├── lib/
│   ├── extractor.js         compromise.js NER + regex + heuristic relationships
│   ├── graph-store.js       SQLite schema, prepared statements, CRUD
│   ├── graph-searcher.js    Single-hop, multi-hop CTE, meta-path queries
│   ├── context-builder.js   [GRAPH CONTEXT] natural-language generation
│   ├── pattern-discovery.js Nightshift pattern candidate evaluation
│   ├── entity-resolver.js   3-tier conversational resolution + merge
│   ├── llm-extractor.js     Ollama LLM extraction for deeper triples
│   └── backfill.js          Retroactive extraction from continuity archives
└── config.default.json      Default configuration
```

### Hooks registered

| Hook | Priority | Purpose |
|------|----------|---------|
| `before_agent_start` | 8 | Entity extraction, graph search, context injection, RRF publishing |
| `agent_end` | — | Fast-path extraction, enrichment queuing, entity resolution |
| `session_end` | — | Log session graph stats |

### Gateway methods

| Method | Purpose |
|--------|---------|
| `graph.getState` | Entity count, triple count, active patterns, last backfill |
| `graph.search` | Execute graph search (params: text, limit) |
| `graph.getEntity` | Look up a specific entity by name |
| `graph.getTriples` | Get triples for a subject or object |
| `graph.listAgents` | List all agents with graph data |
| `graph.detectGaps` | Find under-connected or stale entities |
| `graph.resolveEntity` | Manually resolve a pending entity |
| `graph.mergeEntities` | Merge two entities into one |
| `graph.backfillStatus` | Check archive backfill progress |
| `graph.rebuild` | Rebuild graph from scratch (destructive) |
| `graph.getPatterns` | List all active meta-path patterns (static + discovered) |
| `graph.discoverPatterns` | Trigger pattern discovery manually |

### Integration with other plugins

The graph plugin communicates with sibling plugins via the global bus pattern — no npm dependencies, just runtime wiring through `global` objects:

- **→ continuity**: Publishes search results to `global.__ocGraph.lastResults[agentId]`. Continuity's searcher picks these up for 3-way RRF fusion (semantic + keyword + graph).
- **→ nightshift**: Registers a `graph-enrichment` task runner and queues enrichment tasks for off-hours processing.
- **→ metabolism**: Subscribes to `global.__ocMetabolism.gapListeners` to receive knowledge gaps for enrichment, and fires discovered gaps back to metabolism listeners.

All integrations degrade gracefully — if a sibling plugin isn't loaded, the graph plugin continues without it.

## Configuration

All configuration is optional. The plugin ships with sensible defaults in `config.default.json`.

```json
{
  "extraction": {
    "minExchangeLength": 20,
    "skipHeartbeats": true,
    "maxEntitiesPerExchange": 15,
    "confidenceThreshold": 0.5,
    "canonicalPredicates": [
      "knows", "created", "uses", "located_in", "part_of",
      "interested_in", "prefers", "works_on", "related_to",
      "has_property", "occurred_at", "causes"
    ]
  },

  "retrieval": {
    "maxHops": 2,
    "maxResults": 20,
    "hopDecay": 0.7,
    "minTraversalConfidence": 0.6,
    "metaPathWeight": 0.8
  },

  "metaPaths": {
    "static": [
      { "predicates": ["interested_in", "interested_in"], "weight": 0.8 },
      { "predicates": ["created", "part_of"], "weight": 0.9 },
      { "predicates": ["knows", "works_on"], "weight": 0.85 },
      { "predicates": ["uses", "created"], "weight": 0.7 },
      { "predicates": ["works_on", "part_of"], "weight": 0.75 }
    ]
  },

  "patternDiscovery": {
    "enabled": true,
    "maxActivePatterns": 12,
    "minYield": 3,
    "maxOverlapRatio": 0.9,
    "maxFanoutPerStep": 50,
    "discoveryIntervalHours": 24
  },

  "contextInjection": {
    "enabled": true,
    "maxLines": 5,
    "minConfidence": 0.6,
    "minMentionCount": 2,
    "excludePredicates": ["related_to"]
  },

  "storage": {
    "dbFile": "graph.db",
    "maxTriplesPerEntity": 500,
    "retentionDays": 365
  },

  "entityResolution": {
    "method": "conversational",
    "assumeThreshold": 0.8,
    "askThreshold": 0.4
  },

  "llmExtraction": {
    "model": "deepseek-v3.1:671b-cloud",
    "temperature": 0.3,
    "timeoutMs": 45000
  },

  "backfill": {
    "enabled": true,
    "continuityDir": "../openclaw-plugin-continuity/data"
  }
}
```

Override any values in your OpenClaw config under `plugins.entries.openclaw-plugin-graph.config`.

## Part of the Meta-Cognitive Suite

This plugin is one of seven that form a complete meta-cognitive loop for OpenClaw agents:

1. **[stability](https://github.com/CoderofTheWest/openclaw-plugin-stability)** — Entropy monitoring, confabulation detection, principle alignment
2. **[continuity](https://github.com/CoderofTheWest/openclaw-plugin-continuity)** — Cross-session memory, context budgeting, conversation archiving
3. **[graph](https://github.com/CoderofTheWest/openclaw-plugin-graph)** — Knowledge graph, entity extraction, multi-hop traversal, relationship-aware context *(this plugin)*
4. **[metabolism](https://github.com/CoderofTheWest/openclaw-plugin-metabolism)** — Conversation processing, implication extraction, knowledge gaps
5. **[nightshift](https://github.com/CoderofTheWest/openclaw-plugin-nightshift)** — Off-hours scheduling for heavy processing
6. **[contemplation](https://github.com/CoderofTheWest/openclaw-plugin-contemplation)** — Multi-pass inquiry from knowledge gaps
7. **[crystallization](https://github.com/CoderofTheWest/openclaw-plugin-crystallization)** — Growth vectors become permanent character traits

See [openclaw-metacognitive-suite](https://github.com/CoderofTheWest/openclaw-metacognitive-suite) for the full picture.

## License

MIT
