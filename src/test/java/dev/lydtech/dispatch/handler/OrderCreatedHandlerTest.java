package dev.lydtech.dispatch.handler;

import java.util.concurrent.CompletableFuture;

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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        when(kafkaProducerMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));

        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), RandomStringUtils.randomAlphabetic(8));
        handler.listen(key, testEvent);
        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
    }

    /**
     * The send to Kafka throws an exception (e.g. if Kafka is unavailable).  To ensure the message is not retried
     * continually, this is caught and an error is logged.
     */
    @Test
    public void testListen_ProducerThrowsException() {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), RandomStringUtils.randomAlphabetic(8));
        doThrow(new RuntimeException("Producer failure")).when(kafkaProducerMock).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

        handler.listen(key, testEvent);

        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
    }
}
