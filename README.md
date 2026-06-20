# kafka-db-transaction-poc

Quarkus POC comparing two ways to write a row to PostgreSQL **and** publish an `item-created`
event to Kafka as one all-or-nothing unit: `201 Created` if both commit, `500` if anything fails —
**no after-the-fact compensation**. Same flow, two implementations exposed at different paths.

## The two approaches

### A — `POST /items/pooled` (pool of transactional producers)
- [ItemResource](src/main/java/com/noethex/ItemResource.java) → [ItemCoordinator](src/main/java/com/noethex/ItemCoordinator.java) + [KafkaTransactionalProducerPool](src/main/java/com/noethex/KafkaTransactionalProducerPool.java)
- Opens a Kafka transaction around a fresh JTA/DB transaction (`QuarkusTransaction.requiringNew()`). The Kafka send is awaited *inside* the DB transaction (so a broker failure rolls the DB back); DB commits first, Kafka second.
- A transactional producer can only run **one** transaction at a time, so each request borrows its own producer from a pool. `item.pooled.kafka.tx.pool-size` = max concurrent transactions → **scales with concurrency**.
- Topic: `item-created-pooled`.

### B — `POST /items/chained` (SmallRye `KafkaTransactions`)
- [ItemResource](src/main/java/com/noethex/ItemResource.java) → [ItemTxCoordinator](src/main/java/com/noethex/ItemTxCoordinator.java)
- The documented Quarkus pattern: `KafkaTransactions` emitter + `@Transactional`, chaining the Kafka and Hibernate ORM transactions.
- It wraps a **single** producer, so `@Bulkhead(1)` serializes access — only one transaction at a time. Correct, but under load most concurrent requests are rejected fast → **does not scale**.
- Topic: `item-created-chained`.

### Consistency caveat (both)
There is **no true 2PC** with Kafka (not an XA resource); this is best-effort one-phase-commit. The only divergence window is the final `commitTransaction()` failing *after* the DB committed (row exists, event missing) — logged for reconciliation. DB-first ordering ensures the reverse (event without row) can never happen. For strict atomicity, use the transactional-outbox pattern.

## Run it

Needs Docker. [`docker-compose.yml`](docker-compose.yml) starts Kafka (`:9092`), Kafka UI (<http://localhost:8085>), PostgreSQL (`:5432`), Adminer (<http://localhost:8086>). The app's `%prod` config in [application.properties](src/main/resources/application.properties) points at these by default.

```shell
docker compose up -d                            # infra
./mvnw package                                  # build (needs JDK 21)
java -jar target/quarkus-app/quarkus-run.jar    # runs in prod profile
```

Smoke test (expect `201`):
```shell
curl -i -X POST localhost:8080/items/pooled  -H 'Content-Type: application/json' -d '{"name":"a","payload":"b"}'
curl -i -X POST localhost:8080/items/chained -H 'Content-Type: application/json' -d '{"name":"a","payload":"b"}'
```

## Compare throughput + verify atomicity

[`load-test/`](load-test) load-tests both endpoints with [k6](https://k6.io/), then reconciles the
rows committed to PostgreSQL against the events committed to Kafka (counted with a `read_committed`
consumer) to prove each is all-or-nothing: **db count == kafka count == HTTP 201s**.

With the stack up and the app running:
```powershell
# -UseLocalK6 uses your installed k6; omit it to run k6 via Docker.
# -InvalidRatio is the fraction of deliberately-invalid requests (force rollback); 0 = all valid.
./load-test/run-comparison.ps1 -UseLocalK6 -Vus 30 -Duration 15s -InvalidRatio 0
```

Example output:
```
Endpoint        Creates/s Reqs/s p95 ms Created Failed DbRows KafkaMsgs Consistent
/items/pooled       522.6  522.6   77.7    7862      0   7862      7862       True
/items/chained       20.7  630.9   54.5     311   9181    311       311       True
PASS: DB rows == Kafka msgs == 201s for both.
```
- **Throughput**: pooled handles ~25× the creates/s of chained — chained's `@Bulkhead(1)` rejects almost everything under concurrency (note its huge `Failed`/`Reqs/s`, tiny `Creates/s`).
- **Atomicity**: for both, `DbRows == KafkaMsgs == Created`; every failed request left no row and no event.
