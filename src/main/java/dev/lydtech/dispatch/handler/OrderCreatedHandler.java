package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
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
            groupId = "dispatch_order_created_consumer"
    )
    public void listen(OrderCreated orderCreated) {
        log.info("receive message: orderId: " + orderCreated.getOrderId() + " item: " + orderCreated.getItem());
        dispatchOrder(orderCreated);
    }

    private void dispatchOrder(OrderCreated orderCreated) {
        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderCreated.getOrderId())
                .build();
        kafkaProducer.send("order_dispatched",  orderDispatched);
    }
}
