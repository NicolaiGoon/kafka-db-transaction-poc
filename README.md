# kafka-db-transaction-poc

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: <https://quarkus.io/>.

## Architecture

Flow: `POST /items` → persist the item to PostgreSQL **and** publish an `item-created` event to
Kafka inside a single chained transaction → `201 Created` if both commit, `500` if anything fails.

- **[ItemResource](src/main/java/com/noethex/ItemResource.java)** – REST endpoint. Blocking
  (`@Blocking`) because the transaction uses blocking JDBC and the blocking Kafka transactional
  producer API, so it runs on a worker thread.
- **[ItemCoordinator](src/main/java/com/noethex/ItemCoordinator.java)** – opens a Kafka
  transaction around a fresh JTA/DB transaction (`QuarkusTransaction.requiringNew()`). The Kafka
  send is awaited *inside* the DB transaction so a broker failure rolls the DB back; the DB is
  committed first, then the Kafka transaction is committed. **No compensation / after-the-fact
  deletion** — any failure before the final commit aborts both sides.
- **[KafkaTransactionalProducerPool](src/main/java/com/noethex/KafkaTransactionalProducerPool.java)**
  – a pool of transactional producers, each with its own `transactional.id`. A transactional
  producer can only run one transaction at a time, so each concurrent request borrows a dedicated
  producer; this is what lets the service handle many concurrent create transactions without
  collisions. `item.kafka.tx.pool-size` == max concurrent transactions.

### The one consistency caveat

There is **no true 2PC** between PostgreSQL and Kafka (Kafka is not an XA resource). This is
**best-effort one-phase-commit**: the only divergence window is a failure of the final
`commitTransaction()` *after* the DB has committed — the row exists but the event was not
published. DB-first commit ordering guarantees the inverse (a published event for a missing row)
can never happen; such cases are logged for reconciliation, and the producer is discarded. For
strict end-to-end atomicity, use the transactional-outbox pattern (event row written in the same
DB transaction, relayed to Kafka by CDC/poller).

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at <http://localhost:8080/q/dev/>.

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./mvnw package -Dquarkus.package.jar.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using:

```shell script
./mvnw package -Dnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```shell script
./mvnw package -Dnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/kafka-db-transaction-poc-1.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult <https://quarkus.io/guides/maven-tooling>.

## Related Guides

- REST Jackson ([guide](https://quarkus.io/guides/rest#json-serialisation)): Jackson serialization support for Quarkus
  REST. This extension is not compatible with the quarkus-resteasy extension, or any of the extensions that depend on it
- Apache Kafka Client ([guide](https://quarkus.io/guides/kafka)): Connect to Apache Kafka with its native API, including
  the transactional producer used here ([transactions](https://quarkus.io/guides/kafka#kafka-transactions))
- Hibernate ORM with Panache ([guide](https://quarkus.io/guides/hibernate-orm-panache)): Blocking JPA persistence
- JDBC Driver - PostgreSQL ([guide](https://quarkus.io/guides/datasource)): Connect to the PostgreSQL database via JDBC
- Narayana JTA ([guide](https://quarkus.io/guides/transaction)): JTA transaction support (`QuarkusTransaction`)

## Provided Code

### REST

Easily start your REST Web Services

[Related guide section...](https://quarkus.io/guides/getting-started-reactive#reactive-jax-rs-resources)
