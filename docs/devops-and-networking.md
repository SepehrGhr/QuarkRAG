# QuarkRAG — DevOps, Networking & Infrastructure

This document covers every infrastructure decision in QuarkRAG: why the system is built as microservices, how containers are wired together, what each piece of software does at the network level, and how the Kubernetes layer takes all of that further. It is written as a companion to the architecture document, with the depth needed for a computer networks presentation.

---

## Table of Contents

1. [Why Microservices?](#1-why-microservices)
2. [How Services Talk to Each Other](#2-how-services-talk-to-each-other)
3. [Kafka — The Async Backbone](#3-kafka--the-async-backbone)
4. [FastAPI — The HTTP Layer Inside Each Service](#4-fastapi--the-http-layer-inside-each-service)
5. [Traefik — The Reverse Proxy and API Gateway](#5-traefik--the-reverse-proxy-and-api-gateway)
6. [Docker Compose — Container-by-Container Breakdown](#6-docker-compose--container-by-container-breakdown)
7. [Observability Stack — Prometheus, Grafana, Jaeger](#7-observability-stack--prometheus-grafana-jaeger)
8. [Kubernetes — Taking It to Production](#8-kubernetes--taking-it-to-production)

---

## 1. Why Microservices?

### The Monolith Alternative

Imagine the entire QuarkRAG system as a single Python process. One `main.py` that imports `fastapi`, `sqlalchemy`, `aiokafka`, `qdrant-client`, `openai`, `sentence-transformers`, and a circuit breaker, all running together. It would work — but it would create a number of serious networking and operational problems.

| Concern | Monolith | QuarkRAG Microservices |
|---|---|---|
| **Blast radius** | One crash kills everything | One service crashes, others keep running |
| **Scaling** | Must scale everything together | Scale only the bottleneck (e.g. embedding) |
| **Dependency isolation** | All dependencies share the same process and port | Each service has its own network identity, port, and process |
| **Attack surface** | All code reachable from one network endpoint | Each service is only reachable from specific other services |
| **Deployment** | Entire system redeploys for any change | Each service deploys independently |
| **Resource contention** | Model loading (1 GB+) blocks HTTP handlers | Embedding model loads in its own container, isolated CPU/RAM |

The most dramatic networking difference is **who can reach what**. In a monolith, the function that generates an LLM response and the function that writes to the database are in the same process — there is no network boundary between them. In a microservice design, to call an LLM you must make an HTTP request to a different address. That physical separation creates a natural enforcement point for security and observability.

### The Actual Decomposition in QuarkRAG

```
External User
     │
     ▼
[ Traefik :8000 ]   ← Only public-facing entry point
     │
     ├──▶ [ ingestion-service :8000 ]  → write path: accepts docs, writes to DB, publishes to Kafka
     │
     └──▶ [ query-service :8000 ]      → read path: embeds query, searches Qdrant, calls LLM
                                               │
                                               └──▶ [ llm-provider-service :8000 ]  ← internal only

[ embedding-service ]  ← no HTTP exposure to outside, pure Kafka consumer
     │
     ├── subscribes to Kafka topics
     ├── writes vectors to Qdrant
     └── writes status back to Postgres
```

The `embedding-service` is the clearest example: it has no Traefik label, no public port, and receives no inbound HTTP from users. It is only reachable by subscribing to a Kafka topic. This is a networking isolation decision, not just a code organization one.

### Security Benefits of Service Isolation

- **Credential scoping**: The `query-service` does not have a `DATABASE_URL` in its environment — it has no reason to talk to Postgres, so it never gets the credential. If the query service is compromised, the attacker cannot reach the database.
- **Network segmentation**: In the Kubernetes deployment this is enforced at the kernel level with `NetworkPolicy` objects (covered in section 8). In Docker Compose it is enforced by not giving containers the other service's address.
- **Reduced attack surface per service**: Each service runs only the code it needs. A vulnerability in the document-chunking library cannot be exploited to reach the LLM API key, because those two pieces of code live in different containers.

---

## 2. How Services Talk to Each Other

This is one of the most important networking questions: **do services know about each other? Can they call each other directly?**

The answer is: mostly no, and deliberately so.

### Two Communication Patterns

**Pattern 1 — Kafka (async, decoupled)**

The `ingestion-service` and `embedding-service` never talk to each other directly. The ingestion service publishes a message to the `docs.raw` Kafka topic. It then completes its HTTP response and forgets about it. The embedding service is subscribed to that topic and picks up the message independently, potentially milliseconds or hours later. Neither service knows the other's IP address. Neither makes a TCP connection to the other. The only thing they share is a message schema — the shape of the JSON inside the Kafka message.

This is called **temporal decoupling**: the producer and consumer do not need to be online at the same time.

**Pattern 2 — Direct HTTP (sync, point-to-point)**

The `query-service` calls the `llm-provider-service` directly via HTTP. This is necessary because the user is waiting for a response — the request/response cycle must complete before returning. The URL is injected as an environment variable: `LLM_PROVIDER_SERVICE_URL=http://llm-provider-service:8000`. The service name `llm-provider-service` is resolved by Docker's internal DNS (or Kubernetes DNS in k8s mode).

**What Docker Internal DNS Does**

Docker Compose creates a virtual network called `quarkrag_net`. Inside this network, every container gets a DNS hostname matching its service name. When `query-service` resolves `llm-provider-service`, Docker's embedded DNS server returns that container's private IP address on the `quarkrag_net` bridge network. This means:

- Services never need to hardcode IP addresses.
- The resolution only works inside the Docker network — `llm-provider-service` is not reachable from outside the Docker network at all, because it has no `ports:` mapping in Docker Compose and no Traefik label.

### Service Communication Matrix

| From → To | ingestion | embedding | query | llm-provider | postgres | kafka | qdrant | redis |
|---|---|---|---|---|---|---|---|---|
| **ingestion** | — | via Kafka | — | — | ✓ direct | ✓ produce | — | — |
| **embedding** | — | — | — | — | ✓ direct | ✓ consume/produce | ✓ direct | ✓ direct |
| **query** | — | — | — | ✓ HTTP | — | ✓ produce | ✓ direct | ✓ direct |
| **llm-provider** | — | — | — | — | — | — | — | — |
| **traefik** | ✓ HTTP | — | ✓ HTTP | — | — | — | — | — |

The `llm-provider-service` has no outbound connections to internal services at all — it only calls external APIs (OpenAI) or the local host's Ollama process.

---

## 3. Kafka — The Async Backbone

### What Kafka Is

Apache Kafka is a **distributed event streaming platform**. It functions as a persistent, ordered log of messages that producers write to and consumers read from. Unlike a typical message queue (where a message is deleted after being consumed), Kafka retains messages for a configurable period — allowing consumers to re-read, replay, or catch up from any point in the log.

### Why Kafka Instead of a Direct HTTP Call

The alternative to Kafka for document ingestion would be: the ingestion service makes an HTTP `POST` to the embedding service and waits for it to finish. This has several problems:

1. **The user waits**: Embedding a 50-page document takes seconds. The HTTP connection stays open the whole time.
2. **Tight coupling**: The ingestion service must know the embedding service's address, and both must be running simultaneously.
3. **No retry semantics**: If the embedding service crashes mid-processing, the message is lost.
4. **No backpressure**: If documents arrive faster than they can be embedded, the ingestion service has nowhere to buffer them.

Kafka solves all of these. The ingestion service puts a message in the queue and immediately returns `HTTP 202 Accepted` to the user. The embedding service processes at its own rate. If it crashes, Kafka remembers which messages it had already acknowledged and which it had not — so processing resumes exactly where it left off.

### The Kafka Topics in QuarkRAG

```
docs.raw       — chunks of raw text ready to be embedded
                 Producer: ingestion-service
                 Consumer: embedding-service

docs.embedded  — notification that a document finished embedding
                 Producer: embedding-service
                 Consumer: (available for future consumers)

docs.delete    — request to delete a document's vectors from Qdrant
                 Producer: ingestion-service
                 Consumer: embedding-service

query.events   — log of queries made, for analytics/audit
                 Producer: query-service
                 Consumer: (available for future consumers)

dlq            — Dead Letter Queue: messages that failed processing after all retries
                 Producer: embedding-service
                 Consumer: (operator reads this for investigation)
```

### KRaft Mode — No Zookeeper

The Kafka instance runs in **KRaft mode** (`KAFKA_ENABLE_KRAFT: 'yes'`). Traditionally, Kafka required a separate Zookeeper cluster to manage its metadata (which broker is the leader, where partitions live, etc.). KRaft is Kafka's built-in consensus protocol (based on the Raft algorithm) that eliminates this dependency. The single Kafka container in this project plays both the `broker` role (serving producers/consumers) and the `controller` role (managing cluster metadata), configured by:

```yaml
KAFKA_CFG_PROCESS_ROLES: 'broker,controller'
```

This runs two logical listeners on two ports:
- `:9092` — the `PLAINTEXT` listener for producers and consumers
- `:9093` — the `CONTROLLER` listener for internal Raft consensus (not reachable from outside)

### `KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092`

This is the address Kafka tells clients to connect to after the initial connection. When a producer first connects to Kafka, Kafka responds with metadata including the "advertised" address where data connections should be made. Setting this to `kafka:9092` (the Docker service name) ensures that all clients inside the Docker network connect using the internal DNS name, not a host IP that might change.

### `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: 'false'`

This forces all topics to be explicitly created before they can be used. The `kafka-setup` container handles this by running `kafka-topics.sh --create` for each topic at startup. The reason to disable auto-creation is to catch misconfiguration: if a service tries to publish to a topic with a typo in its name, it fails immediately with an error rather than silently creating an unmonitored topic.

### `KAFKA_CFG_MESSAGE_MAX_BYTES: 1048576`

Maximum Kafka message size is 1 MB. This matters for the `docs.raw` topic, which carries text chunks. A very large document chunk that somehow exceeds 1 MB will be rejected, rather than crashing the broker or silently truncating.

### How the Embedding Service Consumes

```python
consumer = AIOKafkaConsumer(
    settings.RAW_TOPIC,                         # "docs.raw"
    bootstrap_servers=settings.KAFKA_BROKER_URL, # "kafka:9092"
    group_id="embedding-group",                  # consumer group name
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest"                 # on first start, read from beginning
)
```

The `group_id` is significant: Kafka tracks which messages a consumer group has already processed using **offsets**. If the embedding service restarts, it reconnects with the same `group_id` and Kafka tells it: "you last committed offset 47 on partition 0 — start from 48." No messages are lost or reprocessed.

The `auto_offset_reset="earliest"` setting means that the very first time this consumer group connects (before any offsets exist), it reads all messages from the beginning of the topic — so no document that was ingested before the embedding service started gets missed.

---

## 4. FastAPI — The HTTP Layer Inside Each Service

### What FastAPI Is

FastAPI is a Python web framework for building HTTP APIs. Each QuarkRAG service (`ingestion`, `query`, `llm-provider`, `embedding`) is an ASGI application built on FastAPI. ASGI (Asynchronous Server Gateway Interface) is Python's standard for async web servers — it allows thousands of concurrent network connections to be handled on a single thread using Python's `asyncio` event loop.

FastAPI is specifically chosen here (over Flask, Django, etc.) for three reasons relevant to networking and systems design:

1. **Async-native**: All route handlers use `async def`. When a handler awaits a Kafka send or a Postgres query, the event loop can serve other requests in the meantime. There is no thread pool to exhaust.
2. **Automatic OpenAPI spec**: FastAPI generates a `/docs` endpoint with a full interactive API documentation — making it easy to understand each service's interface without reading source code.
3. **Pydantic validation**: Request bodies are validated against a schema before the handler runs. Malformed input is rejected at the network boundary with a `422 Unprocessable Entity` before touching any database.

### The Lifespan Pattern

Every service uses FastAPI's `lifespan` context manager:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    await kafka_producer.start()   # open TCP connection to Kafka broker
    yield                          # serve requests
    # --- shutdown ---
    await kafka_producer.stop()    # cleanly close TCP connections
    await engine.dispose()         # return database connections to the pool
```

This ensures that connections are opened before the first request arrives and closed cleanly on SIGTERM. Without clean shutdown, a Kafka producer might have messages buffered in memory that never get flushed to the broker.

### Health and Readiness Endpoints

Each service exposes HTTP endpoints that the container orchestrator uses to manage traffic:

- **`/health`** — liveness probe. "Is this process alive?" A simple `{"status": "healthy"}` is sufficient. If this returns non-200, the container is restarted.
- **`/readiness`** — readiness probe. "Is this service ready to handle traffic?" For `embedding-service`, this checks both Qdrant connectivity and whether the embedding model finished loading. Until the model is ready, this returns `503` and no traffic is routed to the container.

The distinction matters for `embedding-service` specifically: loading a sentence transformer model from disk can take 30–60 seconds. The service is alive (the process is running) but not ready. Kubernetes and Docker Compose both respect this distinction.

### The `/metrics` Endpoint (Prometheus)

The `query-service` and `llm-provider-service` mount a Prometheus metrics endpoint:

```python
from prometheus_client import make_asgi_app
app.mount("/metrics", make_asgi_app())
```

This exposes a plain-text HTTP page at `/metrics` that Prometheus scrapes every 5 seconds. The page contains lines like:

```
quarkrag_circuit_breaker_state{provider="openai"} 0
http_requests_total{method="POST",path="/query"} 1234
```

These are the raw numbers that Prometheus collects and Grafana visualizes.

---

## 5. Traefik — The Reverse Proxy and API Gateway

### What Traefik Is

Traefik is a **reverse proxy and load balancer** designed for containerized environments. A reverse proxy sits in front of all services and is the only thing that receives traffic from the outside world. It then forwards requests to the appropriate internal service based on rules.

Without Traefik, every service would need its own public-facing port (e.g., `ingestion-service` on `:8001`, `query-service` on `:8002`). The client would need to know which port to use for which operation, and there would be no central point for rate limiting, TLS termination, or authentication.

With Traefik, the client sees exactly one address: `http://localhost:8000`. Traefik inspects the URL path and routes:
- `/documents/*` → `ingestion-service:8000`
- `/query/*` → `query-service:8000`

### Traefik Configuration: `infra/traefik/traefik.yml`

```yaml
api:
  dashboard: true   # Enable the Traefik web dashboard
  insecure: true    # Dashboard accessible without authentication (dev only)

entryPoints:
  web:
    address: ":8000"  # Traefik listens for all incoming traffic on port 8000

providers:
  docker:
    endpoint: "unix:///var/run/docker.sock"  # Read container metadata from Docker
    exposedByDefault: false                  # Do NOT route to containers unless opted in
```

The `providers.docker` section is the key feature: Traefik mounts the Docker socket (`/var/run/docker.sock`) and watches for containers starting and stopping. When it sees a container with the label `traefik.enable=true`, it automatically creates a routing rule for it — no manual configuration file editing required. This is called **service discovery**.

`exposedByDefault: false` means that containers without the `traefik.enable=true` label are completely invisible to Traefik. The `embedding-service`, `kafka`, `postgres`, `qdrant`, and `redis` containers do not have this label, so they receive zero external traffic regardless of whether they're running.

### How Traefik Reads Service Labels

In `docker-compose.yml`, the `ingestion-service` has:

```yaml
labels:
  - "traefik.enable=true"
  # Router: name "ingestion", matches requests where URL path starts with /documents
  - "traefik.http.routers.ingestion.rule=PathPrefix(\"/documents\")"
  # Router: use the "web" entrypoint (port 8000)
  - "traefik.http.routers.ingestion.entrypoints=web"
  # Router: apply the "rate-limit" middleware before forwarding
  - "traefik.http.routers.ingestion.middlewares=rate-limit"
  # Service: forward to port 8000 inside the container
  - "traefik.http.services.ingestion.loadbalancer.server.port=8000"
  # Middleware definition: allow 100 requests/second average
  - "traefik.http.middlewares.rate-limit.ratelimit.average=100"
  # Middleware definition: allow burst of 50 requests
  - "traefik.http.middlewares.rate-limit.ratelimit.burst=50"
```

Traefik reads these labels and constructs:
1. A **Router** called `ingestion` that matches incoming requests with path starting `/documents`
2. A **Service** that knows to forward to port 8000 of the `ingestion-service` container
3. A **Middleware** called `rate-limit` applied before forwarding

### Rate Limiting Explained

The `ratelimit.average=100` / `ratelimit.burst=50` configuration implements a **token bucket** algorithm:
- The bucket refills at 100 tokens/second.
- The burst size is 50 — meaning up to 50 requests can arrive simultaneously and all be served.
- If the bucket is empty, Traefik responds `429 Too Many Requests` without forwarding to the service at all.

This protects the internal services from being overwhelmed by a flood of requests (DoS protection) without the services themselves needing any rate limiting code.

### Port Mapping in Docker Compose

```yaml
traefik:
  ports:
    - "8000:8000"   # External port 8000 → Traefik's web entrypoint (port 8000 inside container)
    - "8088:8080"   # External port 8088 → Traefik's dashboard (port 8080 inside container)
```

Only Traefik has its ports exposed to the host machine. All other service containers (`ingestion-service`, `query-service`, etc.) have no `ports:` mapping — they are only reachable inside the Docker network.

### The Docker Socket Mount

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock:ro
```

This mounts the host's Docker socket into the Traefik container in read-only (`:ro`) mode. The Docker socket is a Unix domain socket — a file-based IPC mechanism. Traefik uses the Docker API over this socket to watch for container start/stop events and read label metadata. The `:ro` mount means Traefik can only read container information, not create or destroy containers.

### Traefik Dashboard

The Traefik dashboard is accessible at `http://localhost:8088` and shows all detected routers, services, and middlewares in real time. This is a useful debugging tool: if a request is not being routed correctly, the dashboard shows exactly what Traefik knows and what rules it has built.

---

## 6. Docker Compose — Container-by-Container Breakdown

### Network Architecture

All containers defined in `docker-compose.yml` and `docker-compose.obs.yml` share the same Docker virtual network:

```yaml
networks:
  default:
    name: quarkrag_net
```

`quarkrag_net` is a **bridge network** — a virtual Layer 2 switch inside the Docker host. Every container on this network gets:
- A private IP address (e.g. `172.20.0.x`)
- A DNS hostname matching its service name (e.g. `kafka`, `postgres`, `redis`)
- Connectivity to all other containers on the same network

Traffic between containers never leaves the host machine. It flows through the Linux kernel's virtual networking stack at near-wire speed.

---

### Container: `postgres`

```yaml
postgres:
  image: postgres:15-alpine           # Official PostgreSQL 15, Alpine Linux base (small image)
  environment:
    POSTGRES_USER: ${POSTGRES_USER:-quarkrag}        # DB user, from .env or default
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-password123}  # DB password
    POSTGRES_DB: ${POSTGRES_DB:-quarkrag}            # Database name to create on first start
  ports:
    - "5432:5432"                     # Expose to host (for local dev tools like pgAdmin)
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U quarkrag -d quarkrag"]  # Check DB accepts connections
    interval: 5s                      # Run check every 5 seconds
    timeout: 5s                       # Fail if no response in 5 seconds
    retries: 5                        # Mark unhealthy after 5 consecutive failures
```

PostgreSQL is the relational database storing document metadata (ID, filename, status, namespace). The `healthcheck` is used by dependent services (via `condition: service_healthy`) to delay their own startup until Postgres is actually accepting connections — not just "the process started."

**Who connects to it**: `ingestion-service` (to write documents), `embedding-service` (to update document status), `db-migrate` (to run schema migrations).

**Who does NOT connect to it**: `query-service`, `llm-provider-service`. They have no reason to touch the document metadata table, so the credential is never given to them.

---

### Container: `kafka`

```yaml
kafka:
  image: bitnami/kafka:3.5
  environment:
    KAFKA_ENABLE_KRAFT: 'yes'                        # Use KRaft (no Zookeeper needed)
    KAFKA_CFG_PROCESS_ROLES: 'broker,controller'     # This node is both data broker and metadata controller
    KAFKA_CFG_CONTROLLER_LISTENER_NAMES: 'CONTROLLER' # Name of the controller listener
    KAFKA_CFG_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093'  # Two listeners on two ports
    KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT'  # No TLS (dev)
    KAFKA_CFG_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092'   # Tell clients: connect here
    KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: '1@127.0.0.1:9093'    # Single-node Raft quorum
    ALLOW_PLAINTEXT_LISTENER: 'yes'                  # Explicitly allow unencrypted traffic
    KAFKA_CFG_NODE_ID: '1'                           # Unique broker ID in the cluster
    KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: 'false'     # Require explicit topic creation
    KAFKA_CFG_MESSAGE_MAX_BYTES: 1048576             # Max message size = 1 MB
  ports:
    - "9092:9092"                     # Expose broker port to host (for dev tools)
  healthcheck:
    test: ["CMD", "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"]  # Can we list topics?
    interval: 10s
    timeout: 5s
    retries: 5
```

---

### Container: `kafka-setup`

```yaml
kafka-setup:
  image: bitnami/kafka:3.5            # Re-use the same image (has kafka-topics.sh built in)
  depends_on:
    kafka:
      condition: service_healthy      # Only run after Kafka is fully ready
  command: >
    bash -c "
      kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 \
        --partitions 1 --replication-factor 1 --topic docs.raw &&
      kafka-topics.sh --create --if-not-exists ... --topic docs.embedded &&
      kafka-topics.sh --create --if-not-exists ... --topic docs.delete &&
      kafka-topics.sh --create --if-not-exists ... --topic query.events &&
      kafka-topics.sh --create --if-not-exists ... --topic dlq
    "
```

This is a **one-shot init container**. It connects to the Kafka broker, creates all five required topics, then exits. The `--if-not-exists` flag makes it idempotent — safe to run multiple times. All application services depend on this container completing successfully (`condition: service_completed_successfully`) before they start, ensuring topics always exist before a producer tries to publish to them.

`--partitions 1 --replication-factor 1`: A single partition means messages are strictly ordered, and a replication factor of 1 means there is only one copy (suitable for a single-node dev deployment; production would use higher values).

---

### Container: `qdrant`

```yaml
qdrant:
  image: qdrant/qdrant:latest         # Qdrant vector database
  ports:
    - "6333:6333"                     # REST/gRPC API port
  healthcheck:
    test: ["CMD", "bash", "-c", "cat < /dev/null > /dev/tcp/127.0.0.1/6333"]  # TCP port probe
    interval: 5s
    timeout: 5s
    retries: 5
```

Qdrant stores the vector embeddings — high-dimensional float arrays (1536 dimensions for OpenAI `text-embedding-3-small`). It provides approximate nearest-neighbor search, which is the core of the RAG retrieval step. The healthcheck uses a pure TCP connect test (`/dev/tcp/host/port` is a bash built-in) since Qdrant does not ship with a dedicated health binary.

**Who connects to it**: `embedding-service` (to write vectors), `query-service` (to search vectors).

---

### Container: `redis`

```yaml
redis:
  image: redis:7-alpine               # Redis 7, minimal Alpine base
  ports:
    - "6379:6379"                     # Redis protocol port
  healthcheck:
    test: ["CMD", "redis-cli", "ping"] # Redis responds "PONG" if healthy
    interval: 5s
    timeout: 5s
    retries: 5
```

Redis serves two roles:

1. **Chunk coordination counter** (embedding-service): When a document is split into N chunks, each chunk is embedded independently. Redis tracks `doc:{id}:processed_chunks` — an atomic counter that increments as each chunk completes. When the counter reaches N, the service knows all chunks are done and marks the document as `ready`. Without Redis, this race condition between concurrent chunk processors would require complex database locking.

2. **Query result cache** (query-service): Identical queries (same text, same namespace) return cached results from Redis, avoiding a round trip to Qdrant and the LLM provider for repeated questions.

---

### Container: `traefik`

(Covered in detail in section 5. Summary of the Docker Compose block:)

```yaml
traefik:
  image: traefik:v3.0
  command:
    - "--configFile=/etc/traefik/traefik.yml"  # Load static config from mounted file
  ports:
    - "8000:8000"   # The single public entry point for all API traffic
    - "8088:8080"   # Traefik admin dashboard
  volumes:
    - ./infra/traefik/traefik.yml:/etc/traefik/traefik.yml:ro  # Static config (read-only)
    - /var/run/docker.sock:/var/run/docker.sock:ro              # Docker service discovery
  depends_on:
    postgres:   { condition: service_healthy }
    kafka:      { condition: service_healthy }
    qdrant:     { condition: service_healthy }
    redis:      { condition: service_healthy }
```

Traefik waits for all infrastructure to be healthy before starting, because it will immediately begin routing traffic once running.

---

### Container: `db-migrate`

```yaml
db-migrate:
  build:
    context: ./services/ingestion     # Build using ingestion service Dockerfile
    dockerfile: Dockerfile
  command: alembic upgrade head       # Run all pending database migrations
  environment:
    - DATABASE_URL=...                # DB connection string
  volumes:
    - ./migrations:/app/services/ingestion/migrations  # Mount migration scripts
    - ./alembic.ini:/app/services/ingestion/alembic.ini
  depends_on:
    postgres:
      condition: service_healthy
```

This is another one-shot container. Alembic is a Python database migration tool. `alembic upgrade head` applies all pending SQL migrations to bring the schema to the latest version. Application services depend on this completing (`condition: service_completed_successfully`) so they never start with a mismatched database schema.

The migration files are mounted as volumes (not baked into the image) so migrations can be added without rebuilding the Docker image.

---

### Containers: `ingestion-service`, `embedding-service`, `query-service`, `llm-provider-service`

All four application services follow the same pattern:

```yaml
build:
  context: ./services/<name>    # Build image from the service's own directory
  dockerfile: Dockerfile
environment:                    # Inject config as environment variables
  - DATABASE_URL=...
  - KAFKA_BROKER_URL=kafka:9092
  - ...
depends_on:                     # Wait for dependencies to be ready
  kafka-setup: { condition: service_completed_successfully }
  postgres:    { condition: service_healthy }
  ...
healthcheck:                    # Container orchestrator checks /health endpoint
  test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]
  interval: 10s
  timeout: 5s
  retries: 3
```

The `extra_hosts: ["host.docker.internal:host-gateway"]` entry on `embedding-service`, `query-service`, and `llm-provider-service` injects a special DNS record that resolves `host.docker.internal` to the host machine's IP address. This is needed for Ollama, which runs on the host machine (not in Docker) and is accessed via `http://host.docker.internal:11434`.

---

## 7. Observability Stack — Prometheus, Grafana, Jaeger

The observability stack runs in a separate compose file (`docker-compose.obs.yml`) and joins the same `quarkrag_net` network, which means its containers can reach the application services by name without any additional networking configuration.

### The Three Pillars of Observability

| Pillar | Tool | What it answers |
|---|---|---|
| **Metrics** | Prometheus + Grafana | "How many requests per second? What is the error rate? Is the circuit breaker open?" |
| **Traces** | Jaeger (OpenTelemetry) | "Where exactly did this specific request spend its 2 seconds?" |
| **Logs** | structlog (within each service) | "What happened inside a service for this request?" |

---

### Prometheus — Metrics Collection

```yaml
prometheus:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"                     # Prometheus web UI and query API
  volumes:
    - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    - ./infra/prometheus/alert.rules.yml:/etc/prometheus/alert.rules.yml:ro
  command:
    - "--config.file=/etc/prometheus/prometheus.yml"
```

Prometheus works by **scraping** — it periodically makes an HTTP `GET` request to a target's `/metrics` endpoint and parses the response. This is a pull model: Prometheus decides when to collect, not the service.

**`prometheus.yml`:**

```yaml
global:
  scrape_interval: 5s       # Poll every 5 seconds
  evaluation_interval: 5s   # Evaluate alert rules every 5 seconds

scrape_configs:
  - job_name: "query-service"
    static_configs:
      - targets: ["query-service:8000"]     # Prometheus resolves this via Docker DNS

  - job_name: "llm-provider-service"
    static_configs:
      - targets: ["llm-provider-service:8000"]
```

Every 5 seconds, Prometheus opens a TCP connection to `query-service:8000`, makes `GET /metrics`, reads the text response, and stores all numeric values in its time-series database. The target address `query-service:8000` works because Prometheus is on the same Docker network and Docker's DNS resolves it.

**Alert Rules (`alert.rules.yml`):**

```yaml
groups:
  - name: circuit-breaker-alerts
    rules:
      - alert: OpenAICircuitBreakerOpen
        expr: quarkrag_circuit_breaker_state{provider="openai"} == 1
        for: 60s            # Only fire if condition is true for 60 consecutive seconds
        labels:
          severity: critical
        annotations:
          summary: "OpenAI LLM Circuit Breaker is OPEN"
          description: "Requests are failing back to local Ollama model."
```

The `expr` is a PromQL query. `quarkrag_circuit_breaker_state{provider="openai"}` reads a metric that the `llm-provider-service` publishes (a gauge set to `1` when the circuit breaker is open, `0` when closed). Prometheus evaluates this expression every 5 seconds and fires the alert if it stays `== 1` for 60 seconds straight.

---

### Grafana — Metrics Visualization

```yaml
grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"                     # Web UI
  volumes:
    - ./infra/grafana/provisioning:/etc/grafana/provisioning:ro  # Auto-configure on startup
  environment:
    - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-admin}
  depends_on:
    - prometheus
```

**Datasource provisioning (`infra/grafana/provisioning/datasources/datasource.yml`):**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090    # Grafana connects to Prometheus inside Docker network
    isDefault: true
```

`access: proxy` means the Grafana **server** makes the requests to Prometheus, not the user's browser. This is important because the user's browser cannot reach `prometheus:9090` (it's inside Docker) — but the Grafana container can, since both are on `quarkrag_net`.

This datasource file is loaded at Grafana startup, so the Prometheus connection is pre-configured without manual setup.

---

### Jaeger — Distributed Tracing

```yaml
jaeger:
  image: jaegertracing/all-in-one:latest
  ports:
    - "16686:16686"   # Jaeger web UI (trace browser)
    - "4317:4317"     # OTLP gRPC receiver (services send traces here)
    - "4318:4318"     # OTLP HTTP receiver
```

**How tracing works:**

Each service has this code in its startup:

```python
provider = TracerProvider()
processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True)
    # endpoint = "http://jaeger:4317"
)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

FastAPIInstrumentor.instrument_app(app)
```

`FastAPIInstrumentor` automatically wraps every incoming HTTP request in a **span** — a timed record of the request with metadata (HTTP method, path, status code). When `query-service` makes an outbound HTTP call to `llm-provider-service`, the instrumentation propagates a **trace ID** via HTTP headers (`traceparent`). Jaeger collects all spans with the same trace ID and assembles them into a timeline showing exactly how long each service spent on each step.

The `OTLPSpanExporter` opens a gRPC connection to `jaeger:4317` and sends spans in batches. This uses the OpenTelemetry Protocol (OTLP) — a vendor-neutral standard, meaning the same service code would work with any OTLP-compatible backend (Grafana Tempo, Datadog, AWS X-Ray, etc.) just by changing the endpoint.

---

## 8. Kubernetes — Taking It to Production

### Why Kubernetes Instead of Docker Compose?

Docker Compose is a single-host tool: it runs all containers on one machine. Kubernetes (k8s) is an orchestration system designed for clusters of machines. The differences that matter most from a networking perspective:

| Aspect | Docker Compose | Kubernetes |
|---|---|---|
| **Host** | Single machine | Cluster of nodes |
| **Service discovery** | Docker DNS (`container-name`) | kube-dns (ClusterIP Services) |
| **External traffic** | Port bindings on host | Ingress controllers, LoadBalancer Services |
| **Network policy** | Trust all containers on the same network | Explicit `NetworkPolicy` per pod |
| **Health management** | Restart on failure (basic) | Liveness + Readiness + Startup probes with fine-grained control |
| **Config management** | `.env` files | `ConfigMap` + `Secret` objects |
| **Scaling** | `docker-compose up --scale` (manual) | `Deployment.replicas` with auto-scaling |

The QuarkRAG Kubernetes manifests implement the same topology as Docker Compose but with these production-grade additions.

---

### The Namespace: `infra/k8s/namespace.yaml`

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: quarkrag
  labels:
    name: quarkrag
```

A Kubernetes namespace is a virtual cluster within a physical cluster. All QuarkRAG resources live in the `quarkrag` namespace. This provides:

- **Isolation**: Resources in `quarkrag` cannot accidentally conflict with resources in other namespaces (e.g., another application's `postgres` service).
- **RBAC scope**: Kubernetes Role-Based Access Control policies can be applied at the namespace level.
- **NetworkPolicy scope**: Network policies in Kubernetes are namespace-scoped — the `deny-all.yaml` policy in the `quarkrag` namespace does not affect other namespaces.

---

### Configuration: `configmap.yaml` and `secrets.yaml`

**ConfigMap:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: quarkrag-config
  namespace: quarkrag
data:
  QDRANT_HOST: "$QDRANT_HOST"
  KAFKA_BROKER_URL: "$KAFKA_BROKER_URL"
  LLM_PROVIDER_SERVICE_URL: "$LLM_PROVIDER_SERVICE_URL"
  EMBEDDING_PROVIDER: "$EMBEDDING_PROVIDER"
  ...
```

A ConfigMap stores non-sensitive configuration key-value pairs. The `$VARIABLE` placeholders are filled in by `envsubst` during the `apply.sh` deployment — the actual values come from the `.env` file. Pods reference the ConfigMap with `envFrom: configMapRef` and Kubernetes injects all keys as environment variables.

**Secret:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: quarkrag-secrets
  namespace: quarkrag
type: Opaque
stringData:
  DATABASE_URL: "$DATABASE_URL"
  OPENAI_API_KEY: "$OPENAI_API_KEY"
  POSTGRES_PASSWORD: "$POSTGRES_PASSWORD"
```

A Secret is similar to a ConfigMap but Kubernetes stores its values base64-encoded and marks them as sensitive. In a production cluster, Secrets would be backed by a vault (AWS Secrets Manager, HashiCorp Vault) rather than file-based values. The `type: Opaque` means this is an arbitrary collection of bytes, not a typed credential like a TLS certificate.

The critical networking implication: because Secrets are separate objects, only pods that explicitly reference `quarkrag-secrets` can read the OpenAI API key. A misconfigured pod that only references `quarkrag-config` has no access to `OPENAI_API_KEY` even if they're in the same namespace.

---

### The Deployment Pattern

Every application service follows the same two-resource pattern: a `Deployment` and a `Service`.

**Deployment** (example: `infra/k8s/ingestion-service.yaml`):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion-service
  namespace: quarkrag
spec:
  replicas: 1                           # Run one pod
  selector:
    matchLabels:
      app: ingestion-service            # Manage pods with this label
  template:
    metadata:
      labels:
        app: ingestion-service          # This label is how NetworkPolicies identify the pod
    spec:
      containers:
        - name: ingestion-service
          image: quarkrag-ingestion-service:latest
          imagePullPolicy: IfNotPresent # Use local image if available (for local dev)
          ports:
            - containerPort: 8000
          envFrom:
            - configMapRef:
                name: quarkrag-config   # Inject all ConfigMap keys as env vars
            - secretRef:
                name: quarkrag-secrets  # Inject all Secret keys as env vars
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
```

**Service** (ClusterIP, default type):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ingestion-service
  namespace: quarkrag
spec:
  selector:
    app: ingestion-service            # Route to pods with this label
  ports:
    - protocol: TCP
      port: 8000
      targetPort: 8000
```

A Kubernetes `Service` creates a stable virtual IP (ClusterIP) that load-balances across all matching pods. This is the Kubernetes equivalent of Docker's internal DNS. When `traefik` pod connects to `ingestion-service:8000`, kube-dns resolves `ingestion-service` to the ClusterIP, and `kube-proxy` routes the traffic to one of the healthy backing pods. If `ingestion-service` has 3 replicas, each connection is round-robin distributed across all 3.

The ClusterIP is only reachable from within the cluster — not from outside. This provides the same isolation that Docker Compose achieves by not exposing ports.

---

### Kubernetes Probes vs Docker Compose Healthchecks

The `embedding-service` demonstrates the three-probe system:

```yaml
startupProbe:
  httpGet:
    path: /readiness
    port: 8000
  failureThreshold: 24
  periodSeconds: 5          # Total: 24 × 5s = 120 seconds to start up
```

The `startupProbe` fires repeatedly during container startup. Until it succeeds, Kubernetes does not start the `livenessProbe` or `readinessProbe`. This gives the embedding model up to 120 seconds to load without the liveness probe killing the container for being "too slow."

After startup succeeds:
- `livenessProbe` (`/health`): If this fails, Kubernetes restarts the container.
- `readinessProbe` (`/readiness`): If this fails, Kubernetes removes the pod from the Service's backend list — traffic stops being sent to it, but the container is not killed.

---

### Infrastructure Services in Kubernetes

**Postgres (`postgres.yaml`):**

Unlike Docker Compose (where environment variables are set inline), in Kubernetes the credentials come from the Secret:

```yaml
env:
  - name: POSTGRES_USER
    valueFrom:
      secretKeyRef:
        name: quarkrag-secrets
        key: POSTGRES_USER        # Read this specific key from the Secret
```

This means the Postgres password is never written in any manifest file — it only exists in the Secret object, which Kubernetes can protect with RBAC.

**Kafka (`kafka.yaml`):**

The same KRaft configuration as Docker Compose, but expressed as Kubernetes environment variable entries instead of a YAML `environment:` block. The advertised listener remains `kafka:9092` — in Kubernetes, this resolves via kube-dns to the Kafka ClusterIP Service.

**Traefik (`traefik.yaml`):**

```yaml
spec:
  type: LoadBalancer          # Kubernetes-managed external IP
  ports:
    - port: 8000
      name: web
    - port: 8088
      name: dashboard
```

In Kubernetes, Traefik runs as a `LoadBalancer` service instead of relying on host port bindings. On a cloud provider (GKE, EKS, AKS), this automatically provisions a cloud load balancer with an external IP. On a local cluster (minikube, kind), it may require `minikube tunnel` to expose the IP.

Traefik's Kubernetes deployment uses `--providers.kubernetesingress=true` instead of `--providers.docker`. Instead of reading Docker labels, Traefik watches Kubernetes `Ingress` resources to discover routing rules.

---

### The Database Migration Job: `db-migrate-job.yaml`

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migrate
  namespace: quarkrag
spec:
  template:
    spec:
      containers:
        - name: db-migrate
          image: quarkrag-ingestion-service:latest
          command: ["alembic", "upgrade", "head"]    # Run migrations
          envFrom:
            - configMapRef: { name: quarkrag-config }
            - secretRef:    { name: quarkrag-secrets }
          volumeMounts:
            - name: migrations
              mountPath: /app/services/ingestion/migrations
            - name: alembic-config
              mountPath: /app/services/ingestion/alembic.ini
      restartPolicy: OnFailure                       # Retry if migration fails
      volumes:
        - name: migrations
          hostPath:
            path: /home/sepehr/QuarkRAG/migrations   # Mount from host filesystem
        - name: alembic-config
          hostPath:
            path: /home/sepehr/QuarkRAG/alembic.ini
```

A Kubernetes `Job` runs a pod to completion and tracks success/failure. Unlike a `Deployment` (which keeps pods running forever), a `Job` is considered complete when the pod exits with code 0. `restartPolicy: OnFailure` means if `alembic upgrade head` fails (e.g., Postgres is not ready yet), the pod is restarted rather than a new pod being created.

---

### The Deployment Script: `apply.sh`

```bash
#!/bin/bash
# Load .env variables into the shell's environment
export $(grep -v '^#' "${ENV_FILE}" | xargs)

# Apply namespace first (must exist before any other resource)
kubectl apply -f namespace.yaml

# Substitute $VARIABLE placeholders in config files with real values from env
envsubst < configmap.yaml | kubectl apply -f -
envsubst < secrets.yaml | kubectl apply -f -

# Apply remaining manifests in dependency order
for manifest in postgres.yaml kafka.yaml qdrant.yaml redis.yaml traefik.yaml \
                db-migrate-job.yaml ingestion-service.yaml embedding-service.yaml \
                query-service.yaml llm-provider-service.yaml; do
  kubectl apply -f "${manifest}"
done
```

`envsubst` is a Linux utility that reads a file and replaces every `$VARIABLE` token with the corresponding shell environment variable. This keeps secrets out of version control: the `.yaml` files in Git contain only `$OPENAI_API_KEY`, never the actual key value.

`kubectl apply -f -` reads the substituted YAML from stdin and applies it to the cluster. `apply` is idempotent — it creates the resource if it does not exist, or patches it if it does, making the script safe to run multiple times.

---

### Network Policies — Zero-Trust Networking in Kubernetes

Network Policies are the most significant networking feature in the Kubernetes deployment that has no direct equivalent in Docker Compose. They are enforced at the kernel level by the cluster's CNI plugin (e.g., Calico, Cilium) and cannot be bypassed by application code.

#### The Default: Deny Everything

```yaml
# infra/k8s/network-policies/deny-all.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: quarkrag
spec:
  podSelector: {}            # Match ALL pods in the namespace
  policyTypes:
    - Ingress                # Block all inbound connections
    - Egress                 # Block all outbound connections
  ingress: []                # No ingress allowed by default
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53           # Allow DNS — pods can still resolve hostnames
        - protocol: TCP
          port: 53
```

This policy alone makes every pod in the `quarkrag` namespace completely network-isolated. No pod can send or receive any network traffic except DNS queries to kube-system. Every allowed connection must then be explicitly opened with a separate NetworkPolicy. This is the **zero-trust** (or "deny by default, allow by exception") security model.

The DNS exception is critical: without it, pods cannot resolve Kubernetes Service names (`kafka`, `postgres`, etc.) via kube-dns, breaking all inter-service communication even after the allow policies are applied.

#### Allow Policies: Opening Specific Paths

Each allow policy comes in a pair: one policy on the **receiver** allowing ingress, and one on the **sender** allowing egress.

**Example: `allow-traefik-to-ingestion.yaml`**

```yaml
# Policy 1: ingestion-service accepts traffic from traefik pods
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-traefik-to-ingestion
spec:
  podSelector:
    matchLabels:
      app: ingestion-service   # This policy applies to ingestion-service pods
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: traefik     # Only from pods labeled app=traefik
      ports:
        - protocol: TCP
          port: 8000
---
# Policy 2: traefik pods are allowed to send to ingestion-service pods
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: egress-traefik-to-ingestion
spec:
  podSelector:
    matchLabels:
      app: traefik
  policyTypes:
    - Egress
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: ingestion-service
      ports:
        - protocol: TCP
          port: 8000
```

Both policies must exist for the connection to work:
- The ingress policy on `ingestion-service` says "I accept traffic from `traefik`."
- The egress policy on `traefik` says "I am allowed to send traffic to `ingestion-service`."

If only one exists, traffic is still blocked (the CNI plugin enforces both sides independently).

#### Complete Network Policy Map

```
deny-all.yaml                           → blocks everything by default, allows DNS

allow-traefik-to-ingestion.yaml         → traefik ↔ ingestion-service :8000
allow-traefik-to-query.yaml             → traefik ↔ query-service :8000

allow-ingestion-to-postgres.yaml        → ingestion-service ↔ postgres :5432
allow-embedding-to-postgres.yaml        → embedding-service ↔ postgres :5432

allow-embedding-to-qdrant.yaml          → embedding-service ↔ qdrant :6333
allow-query-to-qdrant.yaml              → query-service ↔ qdrant :6333

allow-query-to-redis.yaml               → query-service ↔ redis :6379

allow-query-to-llm.yaml                 → query-service ↔ llm-provider-service :8000

allow-ingestion-to-kafka.yaml           → ingestion-service ↔ kafka :9092
allow-embedding-to-kafka.yaml           → embedding-service ↔ kafka :9092
allow-query-to-kafka.yaml               → query-service ↔ kafka :9092
```

**What is NOT allowed by these policies:**

- `ingestion-service` cannot reach `qdrant` (it never needs to — embedding handles that).
- `query-service` cannot reach `postgres` (it never reads document metadata).
- `embedding-service` cannot reach `llm-provider-service`.
- `llm-provider-service` cannot initiate connections to any other internal service.
- No service except Traefik can accept inbound connections from the external world.
- No pod can reach any other pod in a different namespace.

This means a compromised `ingestion-service` pod cannot pivot to `qdrant` or the LLM provider — the kernel drops those packets before they ever arrive, regardless of what code is running in the pod.

---

### Summary: Docker Compose vs Kubernetes Networking

| Feature | Docker Compose | Kubernetes |
|---|---|---|
| DNS | Docker embedded DNS, container names | kube-dns, Service names |
| External ingress | Traefik with host port binding | Traefik with LoadBalancer Service |
| Network isolation | Containers on same bridge can reach each other | All traffic blocked by default (`deny-all.yaml`) |
| Allowed connections | Implicit (no label = no Traefik, but Docker network is open) | Explicit NetworkPolicy for each pair of services |
| Config injection | `.env` file → `environment:` in compose | ConfigMap + Secret → `envFrom:` in pod spec |
| Health management | `healthcheck:` + `depends_on: condition:` | Three separate probes (startup, liveness, readiness) |
| Scaling | Manual `--scale` flag | `replicas:` field, supports HPA |
| Secrets storage | Plaintext in `.env` file | Kubernetes Secret objects (can be backed by vault) |
| Migration job | `service_completed_successfully` dependency | `Job` with `restartPolicy: OnFailure` |
