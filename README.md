# Data Enrichment Service

[![CI](https://github.com/mkazarian-1/Data-Enrichment-Service/actions/workflows/ci.yml/badge.svg)](https://github.com/mkazarian-1/Data-Enrichment-Service/actions/workflows/ci.yml)

A Spring Boot service that asynchronously consumes messages from RabbitMQ, enriches them via an
external REST API, persists the combined result to PostgreSQL, and — only for successfully
committed data — publishes a result event back to RabbitMQ. Reliability is the design focus:
the service is built around a **transactional outbox**, **retry + dead-letter queue** error
handling, and **idempotent processing**, so no message is lost, duplicated in effect, or able to
poison the queue.

**Incoming message** (queue `enrichment.incoming.queue`):

```json
{
  "messageId": "123e4567-e89b-12d3-a456-426614174000",
  "userId": 12345678,
  "action": "request",
  "timestamp": "2026-07-01 10:00:00.0"
}
```

**Outgoing event** (topic exchange `enrichment.result.exchange`, routing key `enrichment.result`;
`logId` is the primary key of the persisted `result` row):

```json
{
  "logId": 654321,
  "messageId": "123e4567-e89b-12d3-a456-426614174000",
  "result": true
}
```

## Technology stack

| Concern | Choice |
|---|---|
| Language / runtime | Java 21 |
| Framework | Spring Boot 4.1 (Spring AMQP, Spring Data JPA, Spring Web `RestClient`) |
| Build | Maven (wrapper included); Surefire for unit tests, Failsafe for `*IT` integration tests |
| Database | PostgreSQL 16, schema managed by Flyway |
| Broker | RabbitMQ 3.13 (management plugin enabled) |
| External API stub | WireMock (container for local runs, embedded server in tests) |
| Boilerplate | Lombok (JPA entities only — records everywhere else), MapStruct (DTO → entity mapping) |
| Testing | JUnit 5, Mockito, Testcontainers (PostgreSQL, RabbitMQ), WireMock, Awaitility |
| Formatting / CI | Spotless with palantir-java-format, GitHub Actions |

## Architecture

Layered-by-feature packages, one responsibility per class:

```
config/       AppProperties (typed, validated config), RabbitConfig (topology, JSON converter,
              retry interceptor, error handler), RestClientConfig (timeouts), SchedulingConfig
messaging/    IncomingMessageListener (consumer edge), fatal-vs-transient exception strategy, DTOs
client/       EnrichmentClient (RestClient wrapper with error classification), DTOs
service/      MessageProcessingService (orchestration), ResultPersister (transactional boundary)
persistence/  ResultEntity, ResultRepository, MapStruct mapper
outbox/       OutboxEntity/Repository, OutboxWriter (same-TX insert), OutboxRelay (scheduled + on-demand publisher)
```

Every queue name, timeout, and retry number lives in exactly one place: `application.properties`,
bound to the validated `AppProperties` record — misconfiguration fails at startup, not at runtime.
The messaging topology (exchanges, queues, bindings, dead-letter wiring) is declared idempotently
by the service itself at startup.

## Error handling & transaction management

This is the core of the design.

### Transaction boundary

```
[AMQP listener]                       (no TX)
   ├─ deserialize + validate          (no TX)
   ├─ call external API               (no TX — never hold a DB TX over HTTP)
   └─ ResultPersister.persist(...)    ← @Transactional
        ├─ INSERT INTO result
        └─ INSERT INTO outbox
   ├─ OutboxRelay.publishNow(id)      (post-commit, best-effort — errors swallowed)
   └─ container ACKs the message      (only after the listener returns normally)
```

- **The external HTTP call happens *before* the database transaction opens.** Holding a DB
  connection across a network call is a classic high-load anti-pattern (pool exhaustion under a
  slow upstream); the enrichment call needs no DB state, so it runs first.
- The `result` insert and the `outbox` insert share **one transaction**: either both commit or
  neither does. Any exception rolls back both and NACKs the message, handing it to the retry
  policy. There is no state in which a result row exists without its outbox event, or vice versa.
- The transactional boundary lives in a dedicated Spring-proxied bean (`ResultPersister`), not in
  the orchestrating service — `@Transactional` on a self-invoked method is silently ignored by
  Spring AOP, so the split is a correctness decision, not a style one.
- Acknowledge mode is `AUTO`: Spring ACKs on normal return and NACKs on exception. Manual acking
  would add surface area with no benefit here.

### Transactional outbox

The result event is **never published directly**. Publishing inside the transaction would be a
lie (the broker doesn't participate in the DB commit), and publishing after it leaves a crash
window where committed data never gets announced. Instead:

1. The serialized `ResultMessage` is inserted into the `outbox` table in the same transaction as
   the business row.
2. A scheduled relay polls `PENDING` rows with `FOR UPDATE SKIP LOCKED`, publishes each with
   **publisher confirms**, and marks a row `SENT` only after the broker acknowledges it.
   Nacks, unroutable returns, timeouts, and exceptions leave the row `PENDING` for the next tick
   (with an `attempts` counter; an `ERROR` log past a threshold serves as the alerting hook).
3. The relay publishes the **raw stored JSON verbatim** (bypassing the message converter) — the
   outbox row is the wire contract, and re-serializing a domain object could silently change it.
4. So the happy path doesn't wait up to a poll interval, the flow also asks the relay to publish
   that one row **immediately after the transaction commits** (`publishNow`). It claims the row with
   the *same* `FOR UPDATE SKIP LOCKED` lock as the poller, so the two never publish it twice, and it
   is best-effort: any failure is swallowed and the row is left `PENDING` for the scheduled poller —
   never propagated, since the message is already committed and throwing would wrongly retry/DLQ it.

The scheduled poller is the durability guarantee; the immediate publish is only a latency
optimization on top of it. A crash at any point between commit and publish loses nothing: the row is
still `PENDING` after restart. The consequence is **at-least-once** delivery downstream — consumers
are expected to dedupe on `messageId`/`logId`.

### Failure classification, retry & DLQ

Failures split into two classes, and the split is enforced by two dedicated exception types
(`EnrichmentTransientException` / `EnrichmentFatalException`) plus a custom
`FatalExceptionStrategy` shared by the retry policy and the container error handler:

| Failure | Class | Behavior |
|---|---|---|
| Malformed JSON / failed validation | fatal | No retry — reject without requeue → DLQ |
| External API `4xx` | fatal | Retrying cannot fix our own bad request → DLQ (response body logged) |
| External API `5xx` / timeout / connect error | transient | Exponential-backoff retry, then DLQ |
| DB unavailable / transient SQL error | transient | Same retry path, then DLQ |
| Duplicate `messageId` | expected | Not an error: `WARN` + ACK, never retried, never DLQ'd |
| Outbox publish failure / missing confirm | transient | Row stays `PENDING`; relay retries next tick |

Retry is an in-process stateless interceptor on the listener container: **4 attempts** (1 delivery
+ 3 retries), exponential backoff 1 s → ×2 → capped at 10 s, all externalized under
`app.rabbit.retry.*`. Exhausted or fatal messages are rejected without requeue and dead-letter via
the queue's `x-dead-letter-exchange` into `enrichment.incoming.dlq`, preserving the original body
and `x-death` diagnostics for inspection and manual replay. Compared to broker-side TTL retry
queues, in-process retry keeps the topology minimal; the trade-off (a retrying message occupies
one consumer slot during backoff) is acceptable at this scale.

### Idempotency

RabbitMQ delivery is at-least-once, so duplicates are a matter of *when*, not *if*. Two layers:

1. A cheap `existsByMessageId` pre-check inside the transaction (skips enrichment-result
   persistence and logs a `WARN`).
2. The authoritative backstop: a `UNIQUE` constraint on `result.message_id`. If two deliveries
   race past the pre-check, one insert fails with a constraint violation that is caught and
   treated as "already processed" — ACK, no row, no second outgoing event.

### Delivery guarantees summary

| Leg | Guarantee | Mechanism |
|---|---|---|
| Broker → service | at-least-once | AUTO ack after successful commit; redelivery on failure |
| Service → DB | effectively-once | unique `message_id` + duplicate swallowing |
| DB → result exchange | at-least-once | transactional outbox + publisher confirms |

### Other deliberate decisions

- **`BIGSERIAL` primary keys** instead of `SERIAL`: an `INT` key on an append-only message log is
  a production time bomb.
- **Multi-instance safe by construction** (though single-instance is the target): `SKIP LOCKED`
  prevents relay instances from double-claiming outbox rows, and constraint-based idempotency
  makes concurrent redeliveries across instances harmless.
- **Concurrent consumption** (2–8 listener threads, prefetch 10): safe for the same reason —
  ordering is not part of the contract, and the unique constraint arbitrates any race between
  threads processing duplicates.
- **`open-in-view` disabled** — a message-driven service has no view to render; OSIV would only
  hold connections longer.
- **Flyway owns the schema** (`ddl-auto=validate`): entities are validated against the migrated
  schema at startup; Hibernate never generates DDL.

## Configuration

All tunables live under the `app.*` prefix in `application.properties`, bound to the validated
`AppProperties` record:

| Key | Default | Purpose |
|---|---|---|
| `app.enrichment.base-url` | `http://localhost:8081` | External enrichment API (WireMock locally) |
| `app.enrichment.connect-timeout` / `read-timeout` | `2s` / `5s` | Explicit HTTP timeouts |
| `app.rabbit.incoming-*`, `dlx`, `dlq`, `result-*` | `enrichment.*` names | Full messaging topology |
| `app.rabbit.retry.*` | 4 attempts, 1 s ×2.0, max 10 s | Listener retry policy |
| `app.outbox.poll-interval` / `batch-size` / `confirm-timeout` | `500ms` / `100` / `5s` | Outbox relay |

## Run locally

Prerequisites: Docker Desktop, JDK 21.

```bash
# 1. Start infrastructure: PostgreSQL, RabbitMQ (+ management UI), WireMock stub of the enrichment API
docker compose up -d

# 2. Run the application
./mvnw spring-boot:run
```

| Service | Address |
|---|---|
| PostgreSQL | `localhost:5434`, db/user/password `enrichment` (host port 5434 to avoid clashing with a locally installed PostgreSQL) |
| RabbitMQ | `localhost:5540` (host port 5540: the default 5672 falls inside a Windows-reserved TCP range) |
| RabbitMQ management UI | <http://localhost:15672> (guest/guest) |
| Enrichment API stub (WireMock) | <http://localhost:8081> (`POST /enrich`) |

### Send a test message

Via the management UI: **Exchanges → `enrichment.incoming.exchange` → Publish message**, routing
key `enrichment.request`, property `content_type` = `application/json`, payload:

```json
{"messageId": "123e4567-e89b-12d3-a456-426614174000", "userId": 12345678, "action": "request", "timestamp": "2026-07-01 10:00:00.0"}
```

Or one line from the terminal:

```bash
docker exec enrichment-rabbitmq rabbitmqadmin publish \
  exchange=enrichment.incoming.exchange routing_key=enrichment.request \
  properties='{"content_type":"application/json"}' \
  payload='{"messageId":"123e4567-e89b-12d3-a456-426614174000","userId":12345678,"action":"request","timestamp":"2026-07-01 10:00:00.0"}'
```

### Observe the outcome

```bash
# Persisted result + outbox state
docker exec enrichment-postgres psql -U enrichment -c "SELECT * FROM result;"
docker exec enrichment-postgres psql -U enrichment -c "SELECT id, message_id, status, attempts, sent_at FROM outbox;"

# Outgoing event: management UI → Queues → enrichment.result.queue → Get messages
# Dead letters:   management UI → Queues → enrichment.incoming.dlq  → Get messages
```

Publishing the same `messageId` twice yields one row and one event (a `WARN` in the log for the
duplicate). Stopping WireMock (`docker compose stop wiremock`) before sending shows the retry
cycle in the log and the message landing in the DLQ afterwards.

## Tests

Docker Desktop must be running (integration tests start PostgreSQL and RabbitMQ via
Testcontainers — no local infrastructure needed):

```bash
./mvnw clean verify
```

| Layer | Approach |
|---|---|
| Service, listener, relay, outbox writer | Pure Mockito unit tests, no Spring context |
| Enrichment client | Unit tests against an embedded WireMock server (status codes, timeouts, error mapping) |
| Repositories | `@DataJpaTest` slices against containerized Postgres (unique constraint, `SKIP LOCKED` query, `jsonb` mapping) |
| Retry/DLQ wiring, relay confirms, transactional atomicity | Focused `@SpringBootTest` + Testcontainers ITs |
| Whole pipeline | `EndToEndFlowIT`: happy path with exact outgoing JSON contract, duplicate delivery (one row, one event), enrichment failure (DLQ, zero partial effects) |

All async assertions use Awaitility with timeouts — no `Thread.sleep` anywhere.

CI (GitHub Actions) runs on every push and pull request: `spotless:check` gates formatting, then
the full `mvn clean verify` — including all Testcontainers-based integration tests — must pass.

## What could be improved

Deliberately left out to keep the test-task scope honest, but next on the list for production:

- **Observability:** Spring Boot Actuator (health/readiness probes), Micrometer metrics — outbox
  lag and `PENDING` depth, DLQ depth, retry counts; structured logging with `messageId` in MDC and
  distributed tracing (OpenTelemetry) across the broker hop.
- **Resilience:** a circuit breaker (Resilience4j) around the enrichment call so a dying upstream
  fails fast instead of burning retry cycles; broker-side TTL retry queues if backoff during
  in-process retry ever blocks too much consumer capacity.
- **Outbox housekeeping:** archiving/purging `SENT` rows, and parking rows that exceed the
  attempts threshold into a `FAILED` status instead of only logging — today they retry forever.
- **DLQ operations:** a replay tool (or a small admin endpoint) instead of manual shovels in the
  management UI.
- **Contract testing** (e.g., Pact or schema registry) for the incoming and outgoing message
  formats, which are currently guarded only by this service's own tests.
- **Security:** TLS for broker/DB connections, credentials via secrets management, auth on the
  enrichment API call.