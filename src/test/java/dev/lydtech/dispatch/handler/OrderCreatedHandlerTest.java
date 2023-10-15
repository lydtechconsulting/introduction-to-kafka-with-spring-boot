package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.service.DispatchService;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

class OrderCreatedHandlerTest {

    private OrderCreatedHandler handler;

    private DispatchService dispatchServiceMock;
    @BeforeEach
    void setUp() {
        dispatchServiceMock = mock(DispatchService.class);
        handler = new OrderCreatedHandler(dispatchServiceMock);
    }
    @Test
    void listen_success() throws Exception {
        String key = UUID.randomUUID().toString();
        UUID randomUUID = UUID.randomUUID();
        handler.listen(key, 0, TestEventData.buildOrderCreatedEvent(randomUUID, "dummyItem"));
        verify(dispatchServiceMock, times(1))
                .process(key, TestEventData.buildOrderCreatedEvent(randomUUID, "dummyItem"));
    }

    @Test
    void listen_ServiceThrowsException() throws Exception {
        String key = UUID.randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), "dummyItem");
        doThrow(new RuntimeException("Service failure")).when(dispatchServiceMock).process(key, testEvent);
        handler.listen(key, 0,testEvent);
        verify(dispatchServiceMock, times(1)).process(key, testEvent);
    }
}