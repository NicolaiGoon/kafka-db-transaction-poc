package com.noethex;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Coordinates the chained transaction: a Kafka transaction is opened around a brand-new
 * database transaction so that a failure in <em>either</em> resource, up to the final commit,
 * rolls back <em>both</em> — no after-the-fact compensation.
 *
 * <p>Commit ordering is DB first, Kafka second. This is best-effort one-phase-commit: a
 * genuine 2PC across PostgreSQL and Kafka is impossible (Kafka is not an XA resource). The
 * only divergence window is a failure of the final {@link KafkaProducer#commitTransaction()}
 * after the DB transaction has committed — i.e. the row exists but the event was not
 * published. We commit the DB first deliberately so this window can never produce a "phantom"
 * event for a row that does not exist; such a producer is discarded and the inconsistency is
 * logged for reconciliation.
 */
@ApplicationScoped
public class ItemCoordinator {

    private static final Logger LOG = Logger.getLogger(ItemCoordinator.class);

    private final ItemRepository repository;
    private final KafkaTransactionalProducerPool producerPool;
    private final ObjectMapper objectMapper;
    private final String topic;

    @Inject
    public ItemCoordinator(ItemRepository repository,
                           KafkaTransactionalProducerPool producerPool,
                           ObjectMapper objectMapper,
                           @ConfigProperty(name = "item.kafka.topic", defaultValue = "item-created") String topic) {
        this.repository = repository;
        this.producerPool = producerPool;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public Item createItem(CreateItemRequest request) {
        PooledProducer pooled = producerPool.borrow();
        KafkaProducer<String, byte[]> producer = pooled.producer();
        boolean discardProducer = false;
        try {
            producer.beginTransaction();

            Item item;
            try {
                // requiringNew() runs in a fresh JTA transaction and rolls it back if the
                // callable throws (e.g. a DB constraint violation or a failed Kafka send),
                // which then triggers the Kafka abort below.
                item = QuarkusTransaction.requiringNew()
                        .call(() -> persistAndStage(request, producer));
            } catch (RuntimeException dbOrSendFailure) {
                // DB transaction already rolled back; abort the Kafka side too.
                discardProducer = !abortQuietly(producer);
                throw new ItemCreationException(
                        "Create failed before commit; DB and Kafka both rolled back", dbOrSendFailure);
            }

            // DB committed when requiringNew().call(...) returned. Commit Kafka last.
            try {
                producer.commitTransaction();
            } catch (KafkaException commitFailure) {
                // The DB row is committed but the event could not be published. The producer's
                // transactional state is now in doubt, so discard it. Flag for reconciliation.
                discardProducer = true;
                LOG.errorf(commitFailure,
                        "Kafka commit failed after DB commit for item %s; event NOT published, reconciliation required",
                        item.getId());
                throw new ItemCreationException("Kafka commit failed after DB commit", commitFailure);
            }

            return item;
        } finally {
            if (discardProducer) {
                producerPool.replace(pooled);
            } else {
                producerPool.release(pooled);
            }
        }
    }

    /**
     * Runs inside the JTA transaction: persist the item and stage (and confirm) the Kafka
     * record. The send future is awaited so a broker-side failure surfaces here and rolls back
     * the DB transaction, rather than only surfacing at commit time.
     */
    Item persistAndStage(CreateItemRequest request, KafkaProducer<String, byte[]> producer) {
        Item item = new Item(request.name(), request.payload());
        repository.persist(item); // UUID id assigned during persist (pre-insert generator)

        ItemCreatedEvent event = ItemCreatedEvent.from(item);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                topic,
                item.getId().toString(), // key by id => partition affinity, ordering, dedup target
                serialize(event));
        try {
            producer.send(record).get();
        } catch (ExecutionException e) {
            throw new ItemCreationException("Failed to stage Kafka record", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ItemCreationException("Interrupted while staging Kafka record", e);
        }
        return item;
    }

    private boolean abortQuietly(KafkaProducer<String, byte[]> producer) {
        try {
            producer.abortTransaction();
            return true;
        } catch (KafkaException e) {
            LOG.error("Failed to abort Kafka transaction; producer will be discarded", e);
            return false;
        }
    }

    private byte[] serialize(ItemCreatedEvent event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new ItemCreationException("Failed to serialize item-created event", e);
        }
    }
}
