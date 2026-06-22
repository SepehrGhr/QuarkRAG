# QuarkRAG — Architecture

> This document is the authoritative reference for every design decision, communication pattern, resilience mechanism, and data-flow in the QuarkRAG platform.

---

## Table of Contents

- [System Overview](#system-overview)
- [High-Level Architecture](#high-level-architecture)
- [Design Principles](#design-principles)
- [Service Decomposition](#service-decomposition)
  - [Ingestion Service](#1-ingestion-service)
  - [Embedding Service](#2-embedding-service)
  - [Query Service](#3-query-service)
  - [LLM Provider Service](#4-llm-provider-service)
- [Communication Patterns](#communication-patterns)
  - [Why Kafka for Ingestion, HTTP for Queries](#why-kafka-for-ingestion-http-for-queries)
  - [Kafka Topic Topology](#kafka-topic-topology)
- [Data Flow Walkthroughs](#data-flow-walkthroughs)
  - [Document Ingestion Flow](#document-ingestion-flow)
  - [Query Answering Flow](#query-answering-flow)
  - [Document Deletion Flow](#document-deletion-flow)
- [Data Stores](#data-stores)
  - [PostgreSQL](#postgresql)
  - [Qdrant](#qdrant)
  - [Redis](#redis)
- [Resilience Patterns](#resilience-patterns)
  - [Circuit Breaker (3-State Machine)](#circuit-breaker-3-state-machine)
  - [Retry & Backoff (tenacity)](#retry--backoff-tenacity)
  - [Dead-Letter Queue (DLQ)](#dead-letter-queue-dlq)
  - [Embedding Consistency Enforcement](#embedding-consistency-enforcement)
- [Cache Strategy](#cache-strategy)
  - [Tier 1 — Exact-Match Cache (Implemented)](#tier-1--exact-match-cache-implemented)
  - [Tier 2 — Semantic Cache (Extension Point)](#tier-2--semantic-cache-extension-point)
- [Multi-Tenancy](#multi-tenancy)
- [Embedding Provider Abstraction](#embedding-provider-abstraction)
- [LLM Provider Abstraction](#llm-provider-abstraction)
- [API Gateway (Traefik)](#api-gateway-traefik)
- [Observability](#observability)
  - [OpenTelemetry Instrumentation](#opentelemetry-instrumentation)
  - [Prometheus Metrics](#prometheus-metrics)
  - [Jaeger Distributed Tracing](#jaeger-distributed-tracing)
  - [Grafana Dashboards](#grafana-dashboards)
  - [Alerting](#alerting)
- [Security & Network Policies](#security--network-policies)
- [Deployment](#deployment)
  - [Docker Compose (Development)](#docker-compose-development)
  - [Kubernetes (Production)](#kubernetes-production)
  - [Health Probes](#health-probes)
- [Database Migrations (Alembic)](#database-migrations-alembic)
- [CI/CD Pipeline](#cicd-pipeline)
- [Future Evolution](#future-evolution)

---

## System Overview

QuarkRAG is an event-driven, microservice-based platform that implements the complete Retrieval-Augmented Generation (RAG) pipeline. Documents enter the system through an HTTP API, get chunked and embedded asynchronously via Kafka, and are stored as vectors in Qdrant. When a user poses a question, the query is embedded, matched against stored vectors, and the retrieved context is sent to an LLM for answer synthesis — all within a single synchronous HTTP request.

The platform is built around four principles: **async-everywhere Python** (FastAPI, asyncpg, aiokafka), **resilience by design** (circuit breaker, DLQ, startup validation), **observable by default** (OpenTelemetry in every service from day one), and **environment-driven configuration** (pydantic-settings, no hardcoded values).

---

## High-Level Architecture

```
                                ┌─────────────────────────────────────────────────┐
                                │              Traefik v3 API Gateway             │
                                │           (Rate Limiting  ·  Routing)           │
                                └──────────┬──────────────────┬───────────────────┘
                                           │                  │
                                      /documents            /query
                                           │                  │
                           ┌───────────────▼──┐        ┌──────▼────────────┐
                           │    Ingestion     │        │   Query Service   │
                           │    Service       │        │                   │
                           └──┬──────────┬────┘        └─┬──────┬─────┬───┘
                              │          │               │      │     │
                       Store  │  Publish │        Search │ Cache│     │ HTTP
                     metadata │  chunks  │       vectors │  R/W │     │ (sync)
                              │          │               │      │     │
                      ┌───────▼──┐  ┌────▼────┐  ┌──────▼─┐  ┌─▼───┐ │
                      │PostgreSQL│  │  Kafka  │  │ Qdrant │  │Redis│ │
                      └───────▲──┘  └────┬────┘  └───▲────┘  └─────┘ │
                              │          │           │               │
                       Update │   docs.raw /         │               │
                       status │   docs.delete   Store│               │
                              │          │      vectors              │
                           ┌──┴──────────▼──┐        │       ┌──────▼──────────────┐
                           │   Embedding    ├────────┘       │  LLM Provider       │
                           │   Service      │                │  Service            │
                           └────────────────┘                │  (Circuit Breaker)  │
                                                             └───────┬─────────────┘
                                                                     │
                                                              OpenAI / Ollama

          ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
           Observability:  Prometheus + Grafana (Metrics)  │  Jaeger (Traces)
```

---

## Design Principles

| Principle | Implementation |
|---|---|
| **Async everywhere** | All services use `async def` endpoints, `asyncpg` via SQLAlchemy async engine, `aiokafka` async producer/consumer, `httpx.AsyncClient` for inter-service HTTP. The event loop is never blocked. |
| **12-Factor configuration** | Every configurable value flows from environment variables through `pydantic_settings.BaseSettings`. No hardcoded credentials, URLs, or thresholds. |
| **Structured observability** | JSON logging via stdlib `logging` (never `print()`), OpenTelemetry instrumentation from day one, named Prometheus metrics with meaningful labels. |
| **Fail loud, not silent** | Embedding dimension mismatches cause a fatal startup exit. Circuit breaker state transitions are logged and metriced. DLQ captures every unprocessable message. |
| **Right tool for each communication pattern** | Kafka for fire-and-forget async workflows (ingestion). HTTP for synchronous user-facing interactions (query answering). |

---

## Service Decomposition

### 1. Ingestion Service

**Responsibility:** Accept document uploads, split text into chunks, persist metadata in PostgreSQL, and publish chunks to Kafka for asynchronous embedding.

| Aspect | Detail |
|---|---|
| **Framework** | FastAPI with async lifespan |
| **Port** | 8001 (internal), exposed at `8000/documents` via Traefik |
| **Data store** | PostgreSQL (SQLAlchemy 2.0 async ORM) |
| **Message broker** | Kafka producer → `docs.raw`, `docs.delete` topics |

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| `POST` | `/documents/upload` | Upload file (PDF/txt/md), chunk it, publish to Kafka |
| `GET` | `/documents` | List documents (filterable by `namespace`, `status`) |
| `GET` | `/documents/{id}` | Get single document metadata |
| `DELETE` | `/documents/{id}` | Delete document + publish delete event |

**Chunking Strategies (factory pattern):**

- **`recursive`** (default) — Uses `langchain_text_splitters.RecursiveCharacterTextSplitter` with `chunk_size=512`, `chunk_overlap=64`. The 64-token overlap prevents semantic information loss at chunk boundaries — critical for retrieval quality.
- **`markdown`** — Uses `langchain_text_splitters.MarkdownTextSplitter`, splitting on Markdown headers to preserve document structure and section context.

The strategy is selected at upload time and stored alongside the document metadata.

**Document Lifecycle (state machine):**

```
uploaded ──▶ chunking ──▶ embedding ──▶ ready
                                   ╲
                                    ▶ failed (→ DLQ)
```

Status transitions are made by both the ingestion service (uploaded → chunking → embedding) and the embedding service (embedding → ready or failed). This cross-service state machine makes the async pipeline visible to API clients via `GET /documents/{id}`.

**Kafka Message Schema (`docs.raw`):**

```json
{
  "document_id": "uuid",
  "chunk_index": 0,
  "text": "chunk content...",
  "namespace": "default",
  "total_chunks": 12
}
```

---

### 2. Embedding Service

**Responsibility:** Consume raw chunks from Kafka, generate vector embeddings, upsert into Qdrant, and update document status in PostgreSQL.

| Aspect | Detail |
|---|---|
| **Framework** | FastAPI with background `asyncio.Task` consumers |
| **Port** | 8002 (internal only — not exposed via Traefik) |
| **Data stores** | Qdrant (write), PostgreSQL (status updates) |
| **Message broker** | Kafka consumer ← `docs.raw`, `docs.delete`; producer → `docs.embedded`, `dlq` |

**Kafka Consumers (background tasks):**

The service starts two `asyncio.Task` consumers during the lifespan startup:

1. **`DocsRawConsumer`** — Consumes `docs.raw` messages, embeds text via the configured provider, upserts vectors into Qdrant, and transitions the document to `ready` when all chunks are processed. Uses `tenacity` for retry with exponential backoff (max 3 attempts, 2–10s backoff). On exhaustion, publishes to the DLQ and sets status to `failed`.

   **Distributed chunk tracking via Redis:** The consumer uses Redis for cross-instance coordination of multi-chunk documents:
   - On the first chunk of a document, sets a Redis key `doc:{document_id}:started` (NX guard, 1hr TTL) and transitions the document status to `embedding`.
   - After each chunk is embedded and upserted, increments `doc:{document_id}:processed_chunks` atomically.
   - When `processed_count == total_chunks`, transitions to `ready`, publishes to `docs.embedded`, and cleans up Redis keys.
   
   This design enables multiple consumer instances to process chunks from the same document concurrently without race conditions.

2. **`DocsDeleteConsumer`** — Consumes `docs.delete` messages and deletes all Qdrant points matching the `document_id` and `namespace` payload filters.

**Embedding Providers (runtime-switchable):**

| Provider | Library | Model | Dimension |
|---|---|---|---|
| `local` | `sentence-transformers` | `all-MiniLM-L6-v2` | 384 | ⚠️ Disabled (raises `NotImplementedError`) |
| `openai` | `openai.AsyncOpenAI` | `text-embedding-3-small` (configurable) | 1536 (configurable) |
| `ollama` | `openai.AsyncOpenAI` (Ollama's OpenAI-compatible endpoint) | `nomic-embed-text` (configurable) | 768 (configurable) |

All embedders implement the same abstract interface via `BaseEmbedder(ABC)`: `embed_text(text) -> list[float]`, `embed_batch(texts) -> list[list[float]]`, plus `dimension` and `model_name` properties.

> **Note:** The `LocalEmbedder` is currently disabled to reduce Docker image size and memory footprint. It raises `NotImplementedError` on instantiation. The Ollama embedder uses Ollama's OpenAI-compatible endpoint (`{OLLAMA_URL}/v1`) with a dummy API key, so both OpenAI and Ollama embedders share the same `openai.AsyncOpenAI` client interface.

**Qdrant Collection Initialization:**

On startup, `init_qdrant_collection()` creates the collection if it doesn't exist, using `VectorParams(size=embedder.dimension, distance=COSINE)`, and stores metadata:

```python
metadata = {
    "embedding_provider": settings.EMBEDDING_PROVIDER,
    "model_name": embedder.model_name,
    "dimension": embedder.dimension
}
```

If the collection already exists, the function performs a **consistency check** — it reads stored metadata and compares against the current configuration. On mismatch, it raises `RuntimeError` to prevent vector space corruption.

This metadata is critical for the embedding consistency enforcement described in [Resilience Patterns](#embedding-consistency-enforcement).

**Deterministic Point IDs:** Vectors are upserted with deterministic IDs using `uuid.uuid5(UUID(document_id), f"chunk_{chunk_index}")`. This makes upserts idempotent — re-processing the same chunk produces the same point ID, preventing duplicates.

**Health Endpoints:**
- `GET /health` — Always returns healthy (for liveness probes)
- `GET /readiness` — Checks Qdrant connectivity and embedder loading status. Returns 503 if either fails. This distinction is critical for Kubernetes probes.

**Startup Probe Requirement:** Loading embedding models can take significant time on CPU. Kubernetes must use a `startupProbe` (not liveness) to prevent the pod from being killed during model initialization (`failureThreshold: 24`, `periodSeconds: 5` = 120s timeout).

---

### 3. Query Service

**Responsibility:** Accept user questions, check cache, embed the question, perform vector similarity search in Qdrant, call the LLM provider for answer synthesis, and return the result.

| Aspect | Detail |
|---|---|
| **Framework** | FastAPI with startup validation |
| **Port** | 8003 (internal), exposed at `8000/query` via Traefik |
| **Data stores** | Qdrant (read), Redis (cache R/W) |
| **Inter-service** | HTTP → LLM Provider Service |
| **Message broker** | Kafka producer → `query.events` |

**Endpoint:**

```
POST /query
{
  "question": "What is the refund policy?",
  "namespace": "default",
  "top_k": 5
}
```

**Request Flow:**

```
1. Generate cache key: SHA256(question + namespace + top_k)
2. Check Redis cache
   ├── HIT  → return cached answer immediately (< 5ms)
   └── MISS → continue
3. Embed question using configured provider
4. Search Qdrant (ANN with namespace filter, top_k results)
5. POST /generate to LLM Provider Service with question + context chunks
6. Cache the response in Redis (TTL configurable via QUERY_CACHE_TTL_SECONDS)
7. Publish event to query.events Kafka topic
8. Return answer + sources + metadata
```

**Startup Validation:** On boot, calls Qdrant to read collection metadata and validates that the configured `EMBEDDING_PROVIDER` matches what was used to embed the stored vectors. On mismatch, logs a FATAL error and exits with `SystemExit` — the service refuses to silently operate with dimensionally incompatible vectors. See [Embedding Consistency Enforcement](#embedding-consistency-enforcement).

**Prometheus Metrics:**

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `quarkrag_query_duration_seconds` | Histogram | `cache_hit` | End-to-end query latency |
| `quarkrag_cache_hit_total` | Counter | — | Total cache hits |
| `quarkrag_cache_miss_total` | Counter | — | Total cache misses |

---

### 4. LLM Provider Service

**Responsibility:** Internal-only service that accepts prompt + context chunks, calls the configured LLM, and returns a generated answer. Manages provider failover via a hand-rolled circuit breaker.

| Aspect | Detail |
|---|---|
| **Framework** | FastAPI |
| **Port** | 8004 (internal only — **not exposed via Traefik**) |
| **Access** | Only reachable by Query Service via internal network |
| **Resilience** | Custom 3-state `CircuitBreaker` + `tenacity` retry within providers |

**Endpoint:**

```
POST /generate  (internal only)
{
  "question": "What is the refund policy?",
  "context": ["chunk1 text...", "chunk2 text..."],
  "namespace": "default"
}
```

**Request Flow:**

```
1. Receive request from Query Service
2. Update Prometheus gauge with current breaker state
3. Call breaker.before_call() → returns "primary" or "fallback"
   ├── "primary" (CLOSED or HALF_OPEN probe)
   │     ├── Success → breaker.record_success(), return {answer, provider: "openai"}
   │     └── Failure → breaker.record_failure(), immediately fall back to Ollama
   └── "fallback" (OPEN)
         └── Call Ollama directly, return {answer, provider: "ollama"}
4. On both providers failing → HTTP 502
```

The key design choice: when the primary fails during a CLOSED or HALF_OPEN state, the service **immediately falls back to the secondary within the same request** — the user never sees a failure.

**LLM Providers:**

| Provider | Library | Model | Config |
|---|---|---|---|
| OpenAI | `openai.AsyncOpenAI` | `gpt-4o-mini` (configurable) | Supports custom `api_base` (OpenRouter compatible), `temperature=0.0` |
| Ollama | `httpx.AsyncClient` | `llama3` (configurable) | Local inference via native `/api/chat` endpoint (not OpenAI-compatible), `temperature=0.0` |

Both providers implement the same interface via `BaseLLMProvider(ABC)`: `async generate(prompt: str, context: list[str]) -> str`. Both use `tenacity` retry decorators (3 attempts, exponential backoff 2–10s) for transient failure resilience.

**Why this service is isolated:** Separating LLM calling into its own service allows the circuit breaker to be stateful within a single process, provider configuration to be independent of query logic, and the service to be scaled or replaced without touching the query pipeline.

---

## Communication Patterns

### Why Kafka for Ingestion, HTTP for Queries

This is one of the most important architectural decisions in the system, and it is **deliberate, not accidental**:

**Document ingestion is fire-and-forget.** The user uploads a document and walks away. The system chunks, embeds, and indexes asynchronously. Kafka is the correct tool because:
- The user does not wait for the result.
- Processing can take seconds to minutes (especially with local embedding models).
- Failed messages can be retried without user involvement.
- Backpressure is handled naturally by consumer lag.

**Query answering is a synchronous user interaction.** The user submits a question and is blocked, waiting for an answer. Introducing Kafka here would require polling, webhooks, or subscriptions — adding latency and complexity with zero benefit. Synchronous HTTP is the correct tool because:
- The user expects an immediate response.
- The full latency budget is ~2–5 seconds (embed + search + LLM call).
- Error handling is straightforward (HTTP status codes).

This is not an inconsistency — it is using the right communication pattern for each workflow type.

### Kafka Topic Topology

| Topic | Producer | Consumer | Payload | Purpose |
|---|---|---|---|---|
| `docs.raw` | Ingestion Service | Embedding Service | `{document_id, chunk_index, text, namespace, total_chunks}` | Raw chunks awaiting embedding |
| `docs.embedded` | Embedding Service | (analytics) | `{document_id, chunk_index, vector_id}` | Confirms chunks are indexed in Qdrant |
| `docs.delete` | Ingestion Service | Embedding Service | `{document_id, namespace}` | Triggers vector deletion from Qdrant |
| `query.events` | Query Service | (analytics) | `{question, namespace, cache_hit, duration, timestamp}` | Query logs for monitoring/analytics |
| `dlq` | Embedding Service | (ops) | Original payload + error + timestamp | Failed messages after retries exhausted |

All topics are created explicitly by the `kafka-setup` init container at startup. `AUTO_CREATE_TOPICS_ENABLE` is set to `false` in the Kafka broker configuration. The init container creates all five topics before any application service starts.

---

## Data Flow Walkthroughs

### Document Ingestion Flow

```
User                    Ingestion Service        PostgreSQL       Kafka         Embedding Service      Qdrant
 │                           │                      │               │                 │                  │
 │  POST /documents/upload   │                      │               │                 │                  │
 │ ─────────────────────────▶│                      │               │                 │                  │
 │                           │  INSERT document     │               │                 │                  │
 │                           │  status=UPLOADED     │               │                 │                  │
 │                           │ ────────────────────▶│               │                 │                  │
 │                           │                      │               │                 │                  │
 │                           │  Chunk text (RecursiveCharacterTextSplitter)            │                  │
 │                           │  UPDATE status=CHUNKING              │                 │                  │
 │                           │ ────────────────────▶│               │                 │                  │
 │                           │                      │               │                 │                  │
 │                           │  Publish N chunks    │               │                 │                  │
 │                           │  UPDATE status=EMBEDDING             │                 │                  │
 │                           │ ────────────────────▶│               │                 │                  │
 │                           │ ─────────────────────────────────────▶│  (docs.raw)     │                  │
 │  ◀─ 200 UploadResponse ──│                      │               │                 │                  │
 │                           │                      │               │                 │                  │
 │                           │                      │               │  Consume chunk  │                  │
 │                           │                      │               │ ───────────────▶│                  │
 │                           │                      │               │                 │  Embed text      │
 │                           │                      │               │                 │  Upsert vector   │
 │                           │                      │               │                 │ ────────────────▶│
 │                           │                      │               │                 │                  │
 │                           │                      │  UPDATE       │                 │                  │
 │                           │                      │  status=READY │                 │                  │
 │                           │                      │◀──────────────────────────────── │                  │
```

### Query Answering Flow

```
User              Traefik        Query Service       Redis        Qdrant       LLM Provider       OpenAI/Ollama
 │                  │                 │                 │            │               │                  │
 │  POST /query     │                 │                 │            │               │                  │
 │ ────────────────▶│ ───────────────▶│                 │            │               │                  │
 │                  │                 │  GET cache      │            │               │                  │
 │                  │                 │ ───────────────▶│            │               │                  │
 │                  │                 │  ◀── MISS ──── │            │               │                  │
 │                  │                 │                 │            │               │                  │
 │                  │                 │  Embed question │            │               │                  │
 │                  │                 │  (local/OpenAI/Ollama)       │               │                  │
 │                  │                 │                 │            │               │                  │
 │                  │                 │  ANN search     │            │               │                  │
 │                  │                 │ (namespace filter)           │               │                  │
 │                  │                 │ ────────────────────────────▶│               │                  │
 │                  │                 │  ◀── top_k results ──────── │               │                  │
 │                  │                 │                 │            │               │                  │
 │                  │                 │  POST /generate │            │               │                  │
 │                  │                 │ ────────────────────────────────────────────▶│                  │
 │                  │                 │                 │            │               │  LLM completion  │
 │                  │                 │                 │            │               │ ────────────────▶│
 │                  │                 │                 │            │               │  ◀── answer ──── │
 │                  │                 │  ◀── answer ────────────────────────────────│                  │
 │                  │                 │                 │            │               │                  │
 │                  │                 │  SET cache      │            │               │                  │
 │                  │                 │ ───────────────▶│            │               │                  │
 │                  │                 │                 │            │               │                  │
 │  ◀── answer ────│ ◀──────────────│                 │            │               │                  │
```

### Document Deletion Flow

```
User              Ingestion Service      PostgreSQL        Kafka          Embedding Service        Qdrant
 │                      │                    │               │                   │                   │
 │  DELETE /docs/{id}   │                    │               │                   │                   │
 │ ────────────────────▶│                    │               │                   │                   │
 │                      │  DELETE document   │               │                   │                   │
 │                      │ ──────────────────▶│               │                   │                   │
 │                      │  Publish delete    │               │                   │                   │
 │                      │ ──────────────────────────────────▶│  (docs.delete)    │                   │
 │  ◀── 200 OK ────────│                    │               │                   │                   │
 │                      │                    │               │  Consume delete   │                   │
 │                      │                    │               │ ─────────────────▶│                   │
 │                      │                    │               │                   │  Delete vectors   │
 │                      │                    │               │                   │  by document_id   │
 │                      │                    │               │                   │ ─────────────────▶│
```

---

## Data Stores

### PostgreSQL

**Role:** Document metadata and pipeline state.

**Schema — `documents` table:**

| Column | Type | Constraints | Purpose |
|---|---|---|---|
| `id` | UUID | PK, `gen_random_uuid()` | Unique document identifier |
| `filename` | TEXT | NOT NULL | Original filename |
| `namespace` | TEXT | NOT NULL, default `'default'` | Multi-tenant isolation key |
| `chunk_count` | INTEGER | nullable | Number of chunks produced |
| `chunking_strategy` | TEXT | — | Strategy used (`recursive`, `markdown`) |
| `embedding_provider` | TEXT | nullable | Provider used when embedded |
| `status` | ENUM | NOT NULL, default `uploaded` | Pipeline state machine value |
| `uploaded_at` | TIMESTAMP | server default `now()` | Upload timestamp |
| `updated_at` | TIMESTAMP | onupdate `now()` | Last modification timestamp |

**Access pattern:** SQLAlchemy 2.0 async ORM with `create_async_engine` + `asyncpg` driver. Managed via Alembic migrations — `create_all()` is never called in production.

### Qdrant

**Role:** Vector storage and approximate nearest neighbor (ANN) search.

**Why Qdrant over pgvector or Weaviate:** Qdrant is Rust-based, purpose-built for vector search, and provides a modern gRPC/REST API. It outperforms pgvector at scale and avoids coupling vector operations to the relational database.

**Collection design:**
- One collection per deployment (`QDRANT_COLLECTION_NAME`, default: `quarkrag_documents`)
- Collection metadata stores `embedding_provider`, `model_name`, and `dimension` for consistency enforcement
- Each point stores: `vector`, `document_id`, `chunk_index`, `text`, `namespace`, `filename`
- Namespace-based filtering via payload `FieldCondition` during search

### Redis

**Role:** Dual-purpose caching and coordination layer.

**1. Query Response Cache (Query Service):**
- **Key schema:** `SHA256("{question}:{namespace}:{top_k}")` → cached answer string
- **TTL:** 1 hour default, configurable
- **Failure handling:** Cache errors are caught and swallowed — a Redis outage degrades performance but does not break the query pipeline. Returns `None` on any `RedisError`, causing a graceful fallback to the full query path.

**2. Distributed Chunk Tracking (Embedding Service):**
- **First-chunk detection:** `SET doc:{document_id}:started NX` (1hr TTL) — only the first consumer to process a chunk triggers the status update to `embedding`
- **Completion counter:** `INCR doc:{document_id}:processed_chunks` (1hr TTL) — atomically tracks how many chunks have been embedded
- **Cleanup:** Both keys are deleted when all chunks are processed

This dual usage means Redis is a dependency for both the Query Service (cache) and the Embedding Service (coordination).

---

## Resilience Patterns

### Circuit Breaker (3-State Machine)

The circuit breaker in the LLM Provider Service is the most important resilience component in the system. It is **hand-rolled** (not a library) to demonstrate understanding of the pattern and to have full control over state transitions and observability.

**State Machine:**

```
                    success
            ┌────────────────────┐
            │                    │
            ▼                    │
       ┌─────────┐    failure_count     ┌──────────┐
       │ CLOSED  │ ──── >= threshold ──▶│  OPEN    │
       │ (0)     │                      │  (1)     │
       └─────────┘                      └────┬─────┘
            ▲                                │
            │                     reset_timeout elapsed
            │                                │
            │         ┌───────────┐          │
            └── probe │ HALF_OPEN │◀─────────┘
           succeeds   │ (2)       │
                      └─────┬─────┘
                            │
                       probe fails
                            │
                            ▼
                       back to OPEN
```

**Implementation details:**

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=30):
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.failure_threshold = failure_threshold  # failures before OPEN
        self.last_failure_time = None
        self.reset_timeout = reset_timeout          # seconds before HALF_OPEN
        self._lock = asyncio.Lock()                 # async-safe transitions
```

- **CLOSED → OPEN:** `failure_count` reaches `failure_threshold` (default: 5 consecutive failures).
- **OPEN → HALF_OPEN:** `reset_timeout` seconds (default: 30) have elapsed since last failure.
- **HALF_OPEN → CLOSED:** Probe call succeeds — primary provider has recovered.
- **HALF_OPEN → OPEN:** Probe call fails — primary provider is still down.

All state transitions are protected by `asyncio.Lock` to prevent race conditions in concurrent requests.

**API surface:**
- `before_call() -> "primary" | "fallback"` — routing decision based on current state. Automatically transitions OPEN → HALF_OPEN when timeout elapses. Uses an `is_probing` flag to prevent multiple concurrent probe requests in HALF_OPEN.
- `record_success()` — resets to CLOSED, clears failure count.
- `record_failure()` — increments failure count, triggers state transitions.

**Fallback behavior:** When the breaker routes to `"fallback"`, the generate endpoint calls Ollama directly. When the primary fails during a CLOSED or HALF_OPEN state, the endpoint records the failure *and* immediately falls back to Ollama within the same request — the user never sees a failure.

**Why hand-rolled instead of using a library:** To demonstrate understanding of the circuit breaker pattern at the implementation level, to control exactly which Prometheus metrics are emitted on each state transition, and to keep the fallback routing logic tightly coupled with the breaker state.

### Retry & Backoff (tenacity)

`tenacity` is used **separately** from the circuit breaker, and only for retry logic within a single provider or consumer attempt:

- **Embedding Service:** Kafka consumer uses `tenacity` with exponential backoff (max 3 retries, 2–10s backoff) on embedding/upsert failures.
- **LLM providers:** Both OpenAI and Ollama provider `generate()` methods are decorated with `@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)` for transient errors.

The circuit breaker and `tenacity` are two separate tools for two separate concerns: the breaker manages **provider-level health**, while tenacity handles **request-level transient failures**.

### Dead-Letter Queue (DLQ)

After all retries are exhausted (tenacity), failed messages are published to the `dlq` Kafka topic with:

```json
{
  "original_topic": "docs.raw",
  "original_payload": { ... },
  "error": "Embedding failed: connection timeout",
  "timestamp": "2026-06-21T20:00:00Z",
  "retry_count": 3
}
```

The corresponding document's status is set to `failed` in PostgreSQL, making the failure visible via `GET /documents/{id}`.

### Embedding Consistency Enforcement

**Problem:** If the embedding provider is changed after documents are already indexed (e.g., switching from `local` 384-dim to `openai` 1536-dim), the query service would attempt to search with vectors of a different dimensionality, producing nonsensical results — or worse, silently returning wrong answers.

**Solution — dual startup validation:**

1. **Embedding Service:** When `init_qdrant_collection()` runs, if the collection already exists, it reads stored metadata and compares `embedding_provider` and `model_name` against current settings. On mismatch, raises `RuntimeError` — the service refuses to start.
2. **Query Service:** When `validate_embedding_consistency()` runs at startup, it reads collection metadata from Qdrant and compares it against `settings.EMBEDDING_PROVIDER` and `embedder.model_name`. On mismatch, logs `CRITICAL` and calls `sys.exit(1)` — hard crash.
3. **On first boot (no collection exists):** Both services skip validation gracefully.

**Re-indexing procedure (if provider is intentionally changed):**

1. Drop the Qdrant collection.
2. Delete all document records from PostgreSQL.
3. Re-ingest all documents.

This operational procedure is documented in `docs/operations.md`.

---

## Cache Strategy

### Tier 1 — Exact-Match Cache (Implemented)

The Query Service implements an exact-match cache using Redis:

| Aspect | Detail |
|---|---|
| **Key** | `SHA256(question + namespace + top_k)` |
| **Value** | Serialized JSON response (answer, sources, metadata) |
| **TTL** | Configurable via `QUERY_CACHE_TTL_SECONDS` env var |
| **Bypass** | On cache hit, the entire pipeline is bypassed — no embedding, no Qdrant search, no LLM call |

**Performance impact:** A cache hit returns in < 5ms compared to 2–5 seconds for a full query. This is directly measurable via the `quarkrag_query_duration_seconds{cache_hit="true"}` Prometheus metric.

**Failure mode:** Redis errors are caught and swallowed. If Redis is down, every request goes through the full pipeline — degraded performance, but no service outage.

### Tier 2 — Semantic Cache (Extension Point)

> **Status:** Documented extension point, not implemented in v1.

The natural evolution of the cache layer:

1. Embed the incoming question.
2. Search a dedicated Qdrant collection of previous query embeddings.
3. If cosine similarity > 0.95 with a previous query, return the cached answer.

This handles semantically equivalent questions: "What is the refund policy?" and "Tell me about refunds" would be a cache hit, even though they differ lexically.

**Implementation sketch:**

```
New Qdrant collection: quarkrag_query_cache
  Point = {
    vector: embed(question),
    payload: { question, namespace, top_k, answer, timestamp }
  }

On query:
  1. Embed question
  2. Search quarkrag_query_cache with threshold 0.95
  3. HIT → return cached answer
  4. MISS → full pipeline → upsert into quarkrag_query_cache
```

This is the natural v2 evolution — it reduces LLM costs for semantically redundant questions without increasing infrastructure complexity significantly (Qdrant is already deployed).

---

## Multi-Tenancy

Every document and query carries a `namespace` field (default: `"default"`). Isolation is enforced at every layer:

| Layer | Implementation |
|---|---|
| **Ingestion** | `namespace` is stored in the PostgreSQL `documents` table. All list/filter queries are namespace-scoped. |
| **Embedding** | `namespace` is stored as a payload field in every Qdrant point. |
| **Query** | Qdrant searches always include a `Filter` with `FieldCondition(key="namespace", match=MatchValue(value=request.namespace))`. |
| **Kafka** | Every message includes `namespace` in the payload, carried through the entire pipeline. |

**Current isolation level:** Logical isolation via payload filtering. A single Qdrant collection holds all namespaces.

**Future evolution:** Per-namespace Qdrant collections for physical data isolation. This eliminates any theoretical risk of a misconfigured query searching across namespaces, but adds collection management overhead (creation, deletion, health monitoring per tenant). This trade-off is appropriate for a multi-tenant SaaS evolution but overkill for the current portfolio-project scope.

---

## Embedding Provider Abstraction

All embedding providers implement the same abstract base class:

```python
class BaseEmbedder(ABC):
    @abstractmethod
    async def embed_text(self, text: str) -> list[float]: ...
    @abstractmethod
    async def embed_batch(self, texts: list[str]) -> list[list[float]]: ...
    @property
    @abstractmethod
    def dimension(self) -> int: ...
    @property
    @abstractmethod
    def model_name(self) -> str: ...
```

A factory function (`get_embedder()`) returns a singleton instance based on `EMBEDDING_PROVIDER`:

| Value | Class | Model | Dimensions | Library |
|---|---|---|---|---|
| `local` | `LocalEmbedder` | `all-MiniLM-L6-v2` | 384 | ⚠️ Disabled (`NotImplementedError`) |
| `openai` | `OpenAIEmbedder` | configurable | configurable | `openai.AsyncOpenAI` (supports custom `api_base`) |
| `ollama` | `OllamaEmbedder` | configurable | configurable | `openai.AsyncOpenAI` → `{OLLAMA_URL}/v1` (OpenAI-compatible) |

The `openai` provider supports any OpenAI-compatible API by setting `OPENAI_API_BASE` (e.g., OpenRouter: `https://openrouter.ai/api/v1`).

The embedding provider abstraction exists in **both** the Embedding Service and the Query Service — they must use the same provider and model to produce dimensionally compatible vectors. This is enforced by the [Embedding Consistency Enforcement](#embedding-consistency-enforcement) mechanism.

---

## LLM Provider Abstraction

Both LLM providers implement the same abstract interface:

```python
class BaseLLMProvider(ABC):
    @abstractmethod
    async def generate(self, prompt: str, context: list[str]) -> str: ...
```

| Value | Class | Model | Library |
|---|---|---|---|
| `openai` | `OpenAIProvider` | `gpt-4o-mini` (configurable) | `openai.AsyncOpenAI` (supports custom `api_base`) |
| `ollama` | `OllamaProvider` | `llama3` (configurable) | `httpx.AsyncClient` → `/api/chat` (native Ollama API) |

The primary provider is always OpenAI; the fallback is always Ollama. The circuit breaker manages the failover logic. Both providers construct identical system + user prompts: context chunks are joined by `---` separators, appended with the user question, and sent with `temperature=0.0`.

---

## API Gateway (Traefik)

Traefik v3 serves as the single entry point for all external HTTP traffic:

| Aspect | Detail |
|---|---|
| **Entrypoint** | Port `8000` (`web`) |
| **Provider** | Docker (reads labels from containers; `exposedByDefault: false`) |
| **Dashboard** | Port `8088` (insecure mode, development only) |
| **Routing** | `Host(\`localhost\`) && PathPrefix(\`/documents\`)` → ingestion-service |
|  | `Host(\`localhost\`) && PathPrefix(\`/query\`)` → query-service |
| **Rate limiting** | 100 requests/s average, 50 burst (configured via Traefik middleware labels) |

**Why Traefik over Nginx or Kong:** Configuration-driven (Docker labels, no custom code), built-in dashboard, automatic service discovery via Docker provider, and native Let's Encrypt support for production HTTPS. For a portfolio project, the dashboard provides immediate visual proof of routing configuration.

**What is NOT exposed:** The LLM Provider Service and Embedding Service have no Traefik labels — they are internal-only services, reachable only from other services on the Docker/Kubernetes internal network. This is defense-in-depth: even without network policies, these services are not routable from outside.

---

## Observability

### OpenTelemetry Instrumentation

Every service is instrumented with OTel from startup using `FastAPIInstrumentor`:

```python
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
FastAPIInstrumentor().instrument_app(app)
```

This provides automatic span creation for all HTTP endpoints, propagating trace context across service boundaries. Traces are exported via OTLP to Jaeger (`OTEL_EXPORTER_OTLP_ENDPOINT`).

### Prometheus Metrics

All services expose `/metrics`. The complete metric registry:

```
# Embedding Service
quarkrag_embedding_duration_seconds{provider, status}    histogram
quarkrag_embeddings_total{provider, status}               counter

# Query Service
quarkrag_query_duration_seconds{cache_hit}               histogram
quarkrag_cache_hit_total                                  counter
quarkrag_cache_miss_total                                 counter

# LLM Provider Service
quarkrag_circuit_breaker_state{provider}                  gauge (0=closed, 1=open, 2=half-open)
quarkrag_llm_provider_active{provider}                    gauge (1=active, 0=inactive)
quarkrag_llm_request_duration_seconds{provider, status}   histogram
```

Prometheus scrapes query-service and llm-provider-service every 5 seconds. Scrape targets are defined in `infra/prometheus/prometheus.yml`.

### Jaeger Distributed Tracing

A single `POST /query` request produces a distributed trace spanning multiple services:

```
Traefik (entry span)
  └── query-service
        ├── span: redis-cache-lookup
        ├── span: embed-question
        ├── span: qdrant-vector-search
        └── llm-provider-service
              └── span: llm-completion (openai or ollama)
```

This single trace screenshot communicates the full system architecture more effectively than any diagram. Every span includes timing, making bottlenecks immediately visible.

**Access:** Jaeger UI at `http://localhost:16686`.

### Grafana Dashboards

Pre-built dashboards are provisioned automatically from `infra/grafana/provisioning/dashboards/quarkrag_dashboard.json`:

| Panel | Query | Demo Scenario |
|---|---|---|
| **Circuit Breaker State** | `quarkrag_circuit_breaker_state` | Color-coded stat panel: Green=CLOSED, Red=OPEN, Orange=HALF_OPEN |
| **Active LLM Provider** | `quarkrag_llm_provider_active` | Shows which provider (OpenAI/Ollama) is currently active |
| **Cache Hit Rate** | `quarkrag_cache_hit_total / (hit + miss)` | Gauge: 0–100% with red/yellow/green thresholds at 50%/80% |
| **Query Latency (P95)** | `histogram_quantile(0.95, quarkrag_query_duration_seconds_bucket)` | Time series with P95 and average latency |

**Access:** Grafana at `http://localhost:3000` (default credentials: `admin/admin`).

### Alerting

A Prometheus alert rule fires when the circuit breaker remains OPEN for more than 60 seconds:

```yaml
# infra/prometheus/alert.rules.yml
- alert: OpenAICircuitBreakerOpen
  expr: quarkrag_circuit_breaker_state{provider="openai"} == 1
  for: 60s
  labels:
    severity: critical
  annotations:
    description: "OpenAI requests are failing and traffic is falling back to local Ollama model"
```

Observability without alerting is incomplete. This alert transforms passive metrics into actionable operational signals.

---

## Security & Network Policies

### Kubernetes NetworkPolicy Matrix

Default policy: **deny-all** ingress and egress applied to the entire `quarkrag` namespace. Explicit allow policies are additive:

| From | To | Port | Allowed | Policy File |
|---|---|---|---|---|
| Traefik | ingestion-service | 8000 | ✅ | `allow-traefik-to-ingestion.yaml` |
| Traefik | query-service | 8000 | ✅ | `allow-traefik-to-query.yaml` |
| Traefik | llm-provider-service | any | ❌ | (covered by deny-all) |
| query-service | llm-provider-service | 8000 | ✅ | `allow-query-to-llm.yaml` |
| query-service | Qdrant | 6333 | ✅ | `allow-query-to-qdrant.yaml` |
| query-service | Redis | 6379 | ✅ | `allow-query-to-redis.yaml` |
| embedding-service | Qdrant | 6333 | ✅ | `allow-embedding-to-qdrant.yaml` |
| ingestion-service | PostgreSQL | 5432 | ✅ | `allow-ingestion-to-postgres.yaml` |
| embedding-service | PostgreSQL | 5432 | ✅ | `allow-embedding-to-postgres.yaml` |
| ingestion-service | Kafka | 9092 | ✅ | `allow-ingestion-to-kafka.yaml` |
| embedding-service | Kafka | 9092 | ✅ | `allow-embedding-to-kafka.yaml` |
| query-service | Kafka | 9092 | ✅ | `allow-query-to-kafka.yaml` |
| Any other | PostgreSQL | 5432 | ❌ | (covered by deny-all) |
| Any other | Qdrant | 6333 | ❌ | (covered by deny-all) |
| Any other | Kafka | 9092 | ❌ | (covered by deny-all) |
| External | any | any | ❌ | (covered by deny-all) |

Each policy file includes comments explaining the business reason for the rule (e.g., "Query service needs read access to Qdrant for vector similarity search"). This makes the network policies a **security design artifact**, not just boilerplate YAML.

### Defense-in-Depth Layers

1. **Traefik routing:** Only ingestion and query endpoints are exposed externally.
2. **Docker/K8s network isolation:** LLM Provider and Embedding services have no external routes.
3. **NetworkPolicy deny-all:** Even within the cluster, only explicitly allowed traffic flows.
4. **No hardcoded secrets:** All credentials are in `.env` (Docker Compose) or K8s Secrets (production).

---

## Deployment

### Docker Compose (Development)

All containers (4 services + 5 infrastructure + 2 init containers) are orchestrated via `docker-compose.yml`:

```
docker compose up -d --build
```

**Startup dependency chain (health-based):**

```
PostgreSQL healthy
    └── ingestion-service
Kafka healthy (KRaft mode — no Zookeeper)
    ├── ingestion-service
    ├── embedding-service
    └── query-service
Qdrant healthy
    ├── embedding-service
    └── query-service
Redis healthy
    └── query-service
    └── embedding-service
```

**Init Containers:**
- **`kafka-setup`** — Creates all five Kafka topics (`docs.raw`, `docs.embedded`, `docs.delete`, `query.events`, `dlq`) before application services start.
- **`db-migrate`** — Runs `alembic upgrade head` using the ingestion service image, ensuring the PostgreSQL schema is ready.

KRaft mode (`KAFKA_ENABLE_KRAFT=yes` on `bitnami/kafka:3.5`) eliminates the Zookeeper container entirely — one fewer container, reduced memory pressure, simpler healthchecks. KRaft has been stable since Kafka 3.5.

**Observability stack** is a separate compose file to keep the core platform lean:

```
docker compose -f docker-compose.obs.yml up -d
```

### Kubernetes (Production)

Production manifests in `infra/k8s/` include:

| Resource Type | Files |
|---|---|
| Namespace | `namespace.yaml` (creates `quarkrag` namespace) |
| ConfigMaps | `configmap.yaml` (uses `envsubst` for templating) |
| Secrets | `secrets.yaml` (uses `envsubst` for templating, `stringData` format) |
| Deployments | 8 combined YAML files (each includes Deployment + Service) |
| Services | ClusterIP for all services; LoadBalancer for Traefik |
| NetworkPolicies | 12 files (deny-all + 11 allows) |
| Job | `db-migrate-job.yaml` (Alembic migration) |

Applied via `infra/k8s/apply.sh` which loads `.env`, uses `envsubst` for config/secret templating, and applies manifests in dependency order.

### Health Probes

| Service | startupProbe | readinessProbe | livenessProbe |
|---|---|---|---|
| ingestion-service | — | Can reach PostgreSQL + Kafka | HTTP 200 `/health` |
| embedding-service | 120s timeout (`failureThreshold: 24`, `periodSeconds: 5`) | `GET /readiness` (Qdrant + embedder) | HTTP 200 `/health` |
| query-service | — | `/health` | HTTP 200 `/health` |
| llm-provider-service | — | `/health` | HTTP 200 `/health` |

The `startupProbe` on the embedding service is **mandatory** — loading embedding models can take significant time. Without a startup probe, the liveness probe will kill the pod before it finishes starting.

The embedding service distinguishes between `/health` (liveness — is the process alive?) and `/readiness` (readiness — can the service handle requests? checks Qdrant connectivity and embedder loading status).

---

## Database Migrations (Alembic)

Schema changes are managed via Alembic — `create_all()` is never called in production:

```
migrations/
├── env.py              # Async migration runner using create_async_engine
└── versions/
    └── 001_create_documents_table.py   # Initial documents table
```

The migration environment (`env.py`) is configured for async execution using `async_engine_from_config` with `asyncpg` driver and `NullPool`. It imports `Base.metadata` from `services.ingestion.database` for autogenerate support, and loads `DATABASE_URL` from the `.env` file at runtime.

**Running migrations:**

```bash
alembic upgrade head
```

---

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/ci-cd.yml`):

| Job | Steps | Purpose |
|---|---|---|
| **lint** | `ruff check .` on all Python files | Code quality enforcement |
| **build-images** | Build all 4 Docker images | Verify Dockerfiles are valid |

**Triggers:** Push to `main`, pull requests to `main`.

Both jobs run in parallel on Ubuntu latest with Python 3.12. No image push or deployment step — build-only validation.

---

## Future Evolution

| Area | Current State | Next Step |
|---|---|---|
| **Cache** | Tier 1 exact-match (Redis) | Tier 2 semantic cache (Qdrant similarity on query embeddings) |
| **Multi-tenancy** | Logical isolation (payload filter) | Per-namespace Qdrant collections for physical isolation |
| **Auth** | Traefik rate limiting | JWT authentication with per-tenant API keys |
| **Scaling** | Single replica per service | Horizontal pod autoscaling based on Kafka consumer lag |
| **Embedding** | Synchronous per-chunk | Batch embedding with dynamic batching for throughput |
| **Monitoring** | Prometheus + Grafana | PagerDuty/Slack integration for circuit breaker alerts |
| **Testing** | Unit tests only | End-to-end integration tests as CI merge gate |
