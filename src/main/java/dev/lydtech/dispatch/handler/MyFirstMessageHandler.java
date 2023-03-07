package dev.lydtech.dispatch.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class MyFirstMessageHandler {

    @KafkaListener(
            id = "MyFirstConsumerClient",
            topics = "my.first.topic",
            groupId = "my.first.topic.consumer",
            properties = { ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG+"=org.apache.kafka.common.serialization.StringDeserializer" }
    )
    public void listen(String payload) {
        log.info("received message: payload: " + payload);
    }
}
