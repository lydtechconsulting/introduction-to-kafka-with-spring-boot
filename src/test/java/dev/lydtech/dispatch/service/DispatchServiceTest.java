package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.service.DispatchService;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class DispatchServiceTest {

    private DispatchService service;
    @BeforeEach
    void setUp() {
        service = new DispatchService();
    }
    @Test
    void process() {
        UUID randomUUID = UUID.randomUUID();
        service.process(TestEventData.buildOrderCreatedEvent(randomUUID, "dummyItem"));
    }
}