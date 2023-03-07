package dev.lydtech.dispatch.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dev.lydtech.dispatch.DispatchConfiguration;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class, OrderDispatchIntegrationTest.TestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
public class OrderDispatchIntegrationTest {

    private final static String ORDER_CREATED_TOPIC = "order_created";
    private final static String ORDER_DISPATCHED_TOPIC = "order_dispatched";

    @Autowired
    private KafkaTemplate testKafkaTemplate;

    @Autowired
    private KafkaTestListener testReceiver;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Configuration
    static class TestConfig {

        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }

        @Bean
        public KafkaTemplate<String, String> testKafkaTemplate(final ProducerFactory<String, String> testProducerFactory) {
            return new KafkaTemplate<>(testProducerFactory);
        }

        @Bean(name = "testProducerFactory")
        public ProducerFactory<String, String> testProducerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean(name = "testKafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, OrderDispatched> testKafkaListenerContainerFactory(final ConsumerFactory<String, OrderDispatched> testConsumerFactory) {
            final ConcurrentKafkaListenerContainerFactory<String, OrderDispatched> factory = new ConcurrentKafkaListenerContainerFactory();
            factory.setConsumerFactory(testConsumerFactory);
            return factory;
        }

        @Bean(name = "testConsumerFactory")
        public ConsumerFactory<String, OrderDispatched> testConsumerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderDispatched.class.getCanonicalName());

            return new DefaultKafkaConsumerFactory<>(config);
        }
    }

    @BeforeEach
    public void setUp() {
        testReceiver.counter.set(0);

        // Wait until the partitions are assigned.
        registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    /**
     * Use this receiver to consume messages from the outbound topic.
     */
    public static class KafkaTestListener {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaIntegrationTest",
                topics = ORDER_DISPATCHED_TOPIC,
                autoStartup = "true",
                containerFactory = "testKafkaListenerContainerFactory")
        void receive(@Payload final OrderDispatched payload) {
            log.debug("KafkaTestListener - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    /**
     * Send in an order_created event and ensure an outbound order_dispatched event is emitted.
     */
    @Test
    public void testOrderDispatchFlow() throws Exception {
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(1));
    }

    public SendResult sendMessage(final String topic, final Object data) throws Exception {
        return (SendResult)testKafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }
}
