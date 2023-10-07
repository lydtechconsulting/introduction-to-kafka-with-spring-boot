package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.message.DispatchPreparing;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DispatchServiceTest {

    private DispatchService service;
    private KafkaTemplate kafkaProducerMock;

    @BeforeEach
    void setUp() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        service = new DispatchService(kafkaProducerMock);
    }
    @Test
    void process() throws Exception {
        when(kafkaProducerMock.send(anyString(), ArgumentMatchers.any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        UUID randomUUID = UUID.randomUUID();
        service.process(TestEventData.buildOrderCreatedEvent(randomUUID, "dummyItem"));
        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), ArgumentMatchers.any(OrderDispatched.class));
        verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), ArgumentMatchers.any(DispatchPreparing.class));
    }

    @Test
    void process_ProducerThrowsException_orderDispatched() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), "dummyOrderCreated");
        doThrow(new RuntimeException("OrderDispatched Producer failure"))
                .when(kafkaProducerMock).send(eq("order.dispatched"), ArgumentMatchers.any(OrderDispatched.class));
        Exception exception = assertThrows(RuntimeException.class, () -> service.process(testEvent));

        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), ArgumentMatchers.any(OrderDispatched.class));
        assertThat(exception.getMessage(), equalTo("OrderDispatched Producer failure"));
    }


    @Test
    void process_ProducerThrowsException_dispatchTracking() {
        OrderCreated orderCreatedTestEvent = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), "dummyOrderCreated");
        doThrow(new RuntimeException("Dispatch tracking Producer failure"))
                .when(kafkaProducerMock).send(eq("dispatch.tracking"), ArgumentMatchers.any(DispatchPreparing.class));
        Exception exception = assertThrows(RuntimeException.class, () -> service.process(orderCreatedTestEvent));

        verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), ArgumentMatchers.any(DispatchPreparing.class));
        assertThat(exception.getMessage(), equalTo("Dispatch tracking Producer failure"));
    }
}