package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.handler.OrderCreatedHandler;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import dev.lydtech.dispatch.service.DispatchService;

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
        UUID randomUUID = UUID.randomUUID();
        handler.listen(TestEventData.buildOrderCreatedEvent(randomUUID, "dummyItem"));
        verify(dispatchServiceMock, times(1))
                .process(TestEventData.buildOrderCreatedEvent(randomUUID, "dummyItem"));
    }

    @Test
    void listen_ServiceThrowsException() throws Exception {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), "dummyItem");
        doThrow(new RuntimeException("Service failure")).when(dispatchServiceMock).process(testEvent);
        handler.listen(testEvent);
        verify(dispatchServiceMock, times(1)).process(testEvent);
    }
}