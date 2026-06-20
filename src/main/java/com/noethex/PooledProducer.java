package com.noethex;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.jboss.logging.Logger;

/**
 * A transactional {@link KafkaProducer} together with its stable {@code transactional.id}.
 * The id is fixed for the lifetime of the pool slot; the underlying producer can be replaced
 * (keeping the id) when it reaches an unrecoverable state. Borrowed by a single thread at a
 * time, so no internal synchronization is required.
 */
public final class PooledProducer {

    private static final Logger LOG = Logger.getLogger(PooledProducer.class);

    private final String transactionalId;
    private KafkaProducer<String, byte[]> producer;

    PooledProducer(String transactionalId, KafkaProducer<String, byte[]> producer) {
        this.transactionalId = transactionalId;
        this.producer = producer;
    }

    public KafkaProducer<String, byte[]> producer() {
        return producer;
    }

    public String transactionalId() {
        return transactionalId;
    }

    void reset(KafkaProducer<String, byte[]> replacement) {
        this.producer = replacement;
    }

    void close() {
        try {
            producer.close();
        } catch (RuntimeException e) {
            LOG.warnf(e, "Error closing transactional producer '%s'", transactionalId);
        }
    }
}
