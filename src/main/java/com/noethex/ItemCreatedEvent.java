package com.noethex;

import java.time.Instant;
import java.util.UUID;

public record ItemCreatedEvent(
        UUID id,
        String name,
        String payload,
        Instant createdAt
) {
    public static ItemCreatedEvent from(Item item) {
        return new ItemCreatedEvent(
                item.getId(),
                item.getName(),
                item.getPayload(),
                item.getCreatedAt());
    }
}