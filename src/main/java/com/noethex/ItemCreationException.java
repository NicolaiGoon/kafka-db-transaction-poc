package com.noethex;

/**
 * Raised when the create flow fails. Whenever this is thrown, neither the DB row nor the
 * Kafka event has been committed (both were rolled back / aborted), with the single
 * documented exception of a failure of the very last Kafka {@code commitTransaction()} call
 * after the DB transaction already committed — the unavoidable best-effort-1PC window.
 */
public class ItemCreationException extends RuntimeException {

    public ItemCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
