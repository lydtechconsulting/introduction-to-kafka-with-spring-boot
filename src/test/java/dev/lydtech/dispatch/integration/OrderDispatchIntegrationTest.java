package dev.lydtech.dispatch.integration;


import dev.lydtech.dispatch.DispatchConfiguration;
import dev.lydtech.dispatch.message.DispatchCompleted;
import dev.lydtech.dispatch.message.DispatchPreparing;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static dev.lydtech.dispatch.integration.WiremockUtils.reset;
import static dev.lydtech.dispatch.integration.WiremockUtils.stubWiremock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
@AutoConfigureWireMock(port = 0)
public class OrderDispatchIntegrationTest {

    private static final String ORDER_CREATED_TOPIC = "order.created";
    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private KafkaTestListener testListener;

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaTestListener testListener() {
            return new KafkaTestListener();
        }
    }

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = { DISPATCH_TRACKING_TOPIC, ORDER_DISPATCHED_TOPIC})
    public static class KafkaTestListener {
        AtomicInteger dispatchPreparingCounter = new AtomicInteger();

        AtomicInteger dispatchCompletedCounter = new AtomicInteger();

        AtomicInteger orderDispatchedCounter = new AtomicInteger();

        @KafkaHandler
        void receiveDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload DispatchPreparing payload) {
            log.debug("Received DispatchPreparing: " + payload + " key: " + key);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            dispatchPreparingCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveDispatchCompleted(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload DispatchCompleted payload) {
            log.debug("Received DispatchCompleted: " + payload + " key: " + key);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            dispatchCompletedCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderDispatched payload) {
            log.debug("Received OrderDispatched: " + payload + " key: " + key);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            orderDispatchedCounter.incrementAndGet();
        }

    }

    @BeforeEach
    public void setUp() {
        testListener.dispatchPreparingCounter.set(0);
        testListener.orderDispatchedCounter.set(0);
        registry.getListenerContainers().stream().forEach(
                messageListenerContainer ->
                        ContainerTestUtils.waitForAssignment(messageListenerContainer,
                                messageListenerContainer.getContainerProperties().getTopics().length *
                                        embeddedKafkaBroker.getPartitionsPerTopic())
        );

        reset();
    }
    @Test
    public void testOrderDispatchFlow_success() throws Exception {
        stubWiremock("/api/stock?item=my-item", 200, "true");

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, orderCreated);

        Awaitility.await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.dispatchPreparingCounter::get, equalTo(1));
        Awaitility.await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                        .until(testListener.dispatchCompletedCounter::get, equalTo(1));
        Awaitility.await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.orderDispatchedCounter::get, equalTo(1));
    }

    @Test
    public void testOrderDispatchFlow_NotRetryableException() throws Exception {
        stubWiremock("/api/stock?item=my-item", 400, "bad request");

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, orderCreated);

        TimeUnit.SECONDS.sleep(3);

        assertThat(testListener.dispatchPreparingCounter.get(), equalTo(0));
        assertThat(testListener.orderDispatchedCounter.get(), equalTo(0));
        assertThat(testListener.dispatchCompletedCounter.get(), equalTo(0));
    }

    @Test
    public void testOrderDispatchFlow_RetryableException() throws Exception {
        stubWiremock("/api/stock?item=my-item", 503, "Service unavailable", "failOnce", STARTED, "succeedNextTime");
        stubWiremock("/api/stock?item=my-item", 200, "true", "failOnce", "succeedNextTime", "succeedNextTime");
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, orderCreated);


        Awaitility.await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.dispatchPreparingCounter::get, equalTo(1));
        Awaitility.await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.dispatchCompletedCounter::get, equalTo(1));
        Awaitility.await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.orderDispatchedCounter::get, equalTo(1));
    }

    private void sendMessage(String topic, Object data) throws Exception {
        kafkaTemplate.send(MessageBuilder.withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
                .build()).get();
    }
 }
