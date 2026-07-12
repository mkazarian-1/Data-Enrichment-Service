# Data Enrichment Service

Spring Boot service that consumes messages from RabbitMQ, enriches them via an external REST API,
persists results to PostgreSQL, and publishes result events through a transactional outbox.

*(Full documentation — architecture, error handling, transaction management — arrives with the final step.)*

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
| RabbitMQ | `localhost:5672` |
| RabbitMQ management UI | <http://localhost:15672> (guest/guest) |
| Enrichment API stub (WireMock) | <http://localhost:8081> (`POST /enrich`) |

Smoke-check the enrichment stub:

```bash
curl -X POST localhost:8081/enrich -H "Content-Type: application/json" -d '{"userId": 12345678, "action": "user_updated"}'
# -> {"userId": 12345678, "result": true}
```

## Run tests

Docker Desktop must be running (integration tests use Testcontainers):

```bash
./mvnw clean verify
```
