package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OrderCreatedHandlerTest {

    private KafkaTemplate kafkaProducerMock;
    private OrderCreatedHandler handler;

    @BeforeEach
    public void setUp() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        handler = new OrderCreatedHandler(kafkaProducerMock);
    }

    @Test
    public void testListen_Success() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), RandomStringUtils.randomAlphabetic(8));
        handler.listen(testEvent);
        verify(kafkaProducerMock, times(1)).send(eq("order_dispatched"), any(OrderDispatched.class));
    }

    @Test
    public void testListen_ProducerThrowsException() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), RandomStringUtils.randomAlphabetic(8));
        doThrow(new RuntimeException("Producer failure")).when(kafkaProducerMock).send(eq("order_dispatched"), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> {
            handler.listen(testEvent);
        });

        verify(kafkaProducerMock, times(1)).send(eq("order_dispatched"), any(OrderDispatched.class));
        assertThat(exception.getMessage(), equalTo("Producer failure"));
    }
}
