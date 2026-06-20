package com.noethex;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.faulttolerance.Bulkhead;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Alternative to {@link ItemCoordinator}: binds a Kafka transaction to a Hibernate ORM (JTA)
 * transaction using SmallRye's {@link KafkaTransactions} emitter — the pattern documented in the
 * Quarkus Kafka guide ("Chaining Kafka Transactions with Hibernate ORM transactions").
 *
 * <p>The method runs inside a JTA transaction ({@code @Transactional}); {@code withTransaction}
 * opens a Kafka transaction around the persist + send so that a failure on either side rolls back
 * both. Same fundamental best-effort-1PC caveat as the pooled path (no real 2PC with Kafka).
 *
 * <p>{@code @Bulkhead(1)}: a {@code KafkaTransactions} emitter wraps a single producer and only
 * one transaction may be in flight at a time. The bulkhead serializes access so concurrent
 * requests never collide on the producer — at the cost of throughput. That serialization is
 * precisely why the high-traffic path ({@link ItemCoordinator}) uses a pool of producers instead.
 */
@ApplicationScoped
public class ItemTxCoordinator {

    private final ItemRepository repository;
    private final KafkaTransactions<ItemCreatedEvent> emitter;

    @Inject
    public ItemTxCoordinator(ItemRepository repository,
                             @Channel("items-tx") KafkaTransactions<ItemCreatedEvent> emitter) {
        this.repository = repository;
        this.emitter = emitter;
    }

    @Transactional
    @Bulkhead(1)
    public Item createItem(CreateItemRequest request) {
        Item item = new Item(request.name(), request.payload());
        emitter.withTransaction(tx -> {
            // UUID id is assigned during persist (pre-insert generator), so it is available for
            // the event/key without an explicit flush. If a DB-assigned id were used, persistAndFlush
            // would be required here to obtain it before building the event.
            repository.persist(item);
            ItemCreatedEvent event = ItemCreatedEvent.from(item);
            tx.send(Message.of(event)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                            .withKey(item.getId().toString()) // partition affinity / ordering / dedup
                            .build()));
            Log.infov("Staged item {0} in chained Kafka/ORM transaction", item.getId());
            return Uni.createFrom().voidItem();
        }).await().indefinitely();
        return item;
    }
}
