package dev.lydtech.dispatch.handler;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// lombok generates a constructor to set instance attributes (in this case kafkaProducer)
@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {

    private final KafkaTemplate<String, OrderDispatched> kafkaProducer;

    @KafkaListener(
            id = "orderConsumerClient",
            topics = "order_created",
            groupId = "dispatch_order_created_consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated orderCreated) {
        log.info("Received message: key: " +key+ " - orderId: " + orderCreated.getOrderId() + " - item: " + orderCreated.getItem());
        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderCreated.getOrderId())
                .build();
        try {
            kafkaProducer.send("order_dispatched", key, orderDispatched).get();
        } catch (Exception e) {
            log.error("Unable to send event", e);
            throw new RuntimeException("Unable to send event", e);
        }
    }
}
