package com.noethex;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Pool of Kafka transactional producers.
 *
 * <p>A Kafka producer can only have one transaction in flight at a time, and a given
 * {@code transactional.id} may only be used by one producer instance. Sharing a single
 * transactional producer across concurrent requests therefore causes collisions
 * ("Transaction already in progress" / producer fencing). To process many concurrent
 * create requests safely, each request borrows a dedicated producer (its own
 * {@code transactional.id}) from this pool for the duration of its transaction and returns
 * it afterwards. Pool size == maximum number of concurrent DB+Kafka transactions.
 */
@ApplicationScoped
public class KafkaTransactionalProducerPool {

    private static final Logger LOG = Logger.getLogger(KafkaTransactionalProducerPool.class);

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "item.kafka.tx.pool-size", defaultValue = "16")
    int poolSize;

    @ConfigProperty(name = "item.kafka.tx.id-prefix", defaultValue = "item-tx")
    String transactionalIdPrefix;

    @ConfigProperty(name = "item.kafka.tx.borrow-timeout", defaultValue = "PT5S")
    Duration borrowTimeout;

    @ConfigProperty(name = "item.kafka.tx.transaction-timeout", defaultValue = "PT10S")
    Duration transactionTimeout;

    private final List<PooledProducer> allProducers = new ArrayList<>();
    private BlockingQueue<PooledProducer> available;

    void onStart(@Observes StartupEvent event) {
        available = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            PooledProducer pooled = new PooledProducer(transactionalIdPrefix + "-" + i, newProducer(transactionalIdPrefix + "-" + i));
            allProducers.add(pooled);
            available.add(pooled);
        }
        LOG.infof("Initialized %d transactional Kafka producers (prefix '%s')", poolSize, transactionalIdPrefix);
    }

    void onStop(@Observes ShutdownEvent event) {
        for (PooledProducer pooled : allProducers) {
            pooled.close();
        }
    }

    /** Borrow a producer, blocking up to the configured timeout if the pool is exhausted. */
    public PooledProducer borrow() {
        try {
            PooledProducer pooled = available.poll(borrowTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (pooled == null) {
                throw new IllegalStateException(
                        "No transactional Kafka producer available within " + borrowTimeout
                                + " (pool of " + poolSize + " exhausted)");
            }
            return pooled;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while borrowing a transactional producer", e);
        }
    }

    /** Return a healthy producer to the pool for reuse. */
    public void release(PooledProducer pooled) {
        available.offer(pooled);
    }

    /**
     * Discard a producer whose transactional state is unrecoverable (fatal error or a failed
     * abort/commit) and put a freshly initialized replacement back into the pool, keeping the
     * same {@code transactional.id} so producer fencing semantics are preserved.
     */
    public void replace(PooledProducer pooled) {
        LOG.warnf("Replacing transactional producer '%s' after unrecoverable error", pooled.transactionalId());
        pooled.close();
        pooled.reset(newProducer(pooled.transactionalId()));
        available.offer(pooled);
    }

    private KafkaProducer<String, byte[]> newProducer(String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Required for transactions; idempotence is implied but set explicitly for clarity.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) transactionTimeout.toMillis());
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        // Registers the transactional.id with the broker (fences any previous instance).
        producer.initTransactions();
        return producer;
    }
}
