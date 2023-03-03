package dev.lydtech.dispatch.util;

import java.util.UUID;

import dev.lydtech.dispatch.message.OrderCreated;

public class TestEventData {

    public static OrderCreated buildOrderCreatedEvent(UUID orderId, String item) {
        return OrderCreated.builder()
                .orderId(orderId)
                .item(item)
                .build();
    }
}
