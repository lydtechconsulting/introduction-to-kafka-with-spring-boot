package dev.lydtech.dispatch.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dev.lydtech.dispatch.DispatchConfiguration;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class, OrderDispatchIntegrationTest.TestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
public class OrderDispatchIntegrationTest {

    private final static String ORDER_CREATED_TOPIC = "order.created";
    private final static String ORDER_DISPATCHED_TOPIC = "order.dispatched";

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
        public KafkaTemplate<String, OrderCreated> testKafkaTemplate(final ProducerFactory<String, OrderCreated> testProducerFactory) {
            return new KafkaTemplate<>(testProducerFactory);
        }

        @Bean(name = "testProducerFactory")
        public ProducerFactory<String, OrderCreated> testProducerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
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
        testReceiver.keyedMessages = new ArrayList<>();

        // Wait until the partitions are assigned.
        registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    /**
     * Use this receiver to consume messages from the outbound topic.
     */
    public static class KafkaTestListener {
        AtomicInteger counter = new AtomicInteger(0);
        List<ImmutablePair<String, OrderDispatched>> keyedMessages = new ArrayList<>();

        @KafkaListener(groupId = "KafkaIntegrationTest",
                topics = ORDER_DISPATCHED_TOPIC,
                autoStartup = "true",
                containerFactory = "testKafkaListenerContainerFactory")
        void receive(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload final OrderDispatched payload) {
            log.debug("KafkaTestListener - Received message key: " + key + " - payload: " + payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            keyedMessages.add(ImmutablePair.of(key, payload));
            counter.incrementAndGet();
        }
    }

    /**
     * Send in an order.created event and ensure an outbound order.dispatched event is emitted.
     *
     * The key sent with the order.created event should match the key received for the order.dispatched event.
     */
    @Test
    public void testOrderDispatchFlow() throws Exception {
        UUID orderId = randomUUID();
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(orderId, "my-item");
        String key = randomUUID().toString();
        sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(1));

        assertThat(testReceiver.keyedMessages.size(), equalTo(1));
        assertThat(testReceiver.keyedMessages.get(0).getLeft(), equalTo(key));
        assertThat(testReceiver.keyedMessages.get(0).getRight().getOrderId(), equalTo(orderId));
    }

    public SendResult sendMessage(final String topic, final String key, final Object data) throws Exception {
        return (SendResult)testKafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }
}
