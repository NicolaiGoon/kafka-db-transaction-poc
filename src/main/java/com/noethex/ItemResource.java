package com.noethex;

import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@Path("/items")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ItemResource {

    private static final Logger LOG = Logger.getLogger(ItemResource.class);

    @Inject
    ItemCoordinator coordinator;

    @Inject
    ItemTxCoordinator txCoordinator;

    /**
     * High-throughput path: chained Kafka tx + DB tx using a pool of transactional producers,
     * so many requests run concurrently without colliding.
     */
    @POST
    @Path("/pooled")
    @Blocking
    public Response createPooled(CreateItemRequest request) {
        return create(() -> coordinator.createItem(request));
    }

    /**
     * Alternative path: chained Kafka tx + Hibernate ORM tx via SmallRye {@code KafkaTransactions}
     * (the documented Quarkus pattern). Correct, but serialized to one transaction at a time by the
     * bulkhead in {@link ItemTxCoordinator}.
     */
    @POST
    @Path("/chained")
    @Blocking
    public Response createChained(CreateItemRequest request) {
        return create(() -> txCoordinator.createItem(request));
    }

    private Response create(java.util.function.Supplier<Item> action) {
        try {
            Item item = action.get();
            return Response.status(Response.Status.CREATED)
                    .entity(ItemCreatedEvent.from(item))
                    .build();
        } catch (RuntimeException failure) {
            LOG.error("Failed to create item", failure);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Could not create item"))
                    .build();
        }
    }
}
