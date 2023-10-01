package handler;

import dev.lydtech.dispatch.handler.OrderCreatedHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import dev.lydtech.dispatch.service.DispatchService;

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
    void listen() {
        handler.listen("dummyPayload");
        verify(dispatchServiceMock, times(1)).process("dummyPayload");
    }
}