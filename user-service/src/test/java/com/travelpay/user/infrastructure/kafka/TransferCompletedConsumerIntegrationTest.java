package com.travelpay.user.infrastructure.kafka;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import com.sun.net.httpserver.HttpServer;
import com.travelpay.user.domain.model.entity.User;
import com.travelpay.user.domain.repository.UserRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 송금 완료 이벤트 소비자를 실제 앱 설정(Flyway 스키마)·PostgreSQL·Kafka로 확인합니다.
 * Slack은 JDK HttpServer로 만든 가짜 Webhook이 받은 본문을 모읍니다.
 * 같은 파티션의 메시지는 순서대로 처리되므로, 확인할 메시지 뒤에 같은 키로 표시용 이벤트를 보내 그것이 끝나면 앞의 처리도 끝났다고 봅니다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.discovery.enabled=false",
        "spring.cloud.service-registry.auto-registration.enabled=false",
        "logging.structured.format.console=",
        "travelpay.user.transfer-notification.consumer-enabled=true"
})
class TransferCompletedConsumerIntegrationTest {

    private static final String REDIS_PASSWORD = UUID.randomUUID().toString();
    private static final Duration WAIT = Duration.ofSeconds(30);

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-bookworm")
            .withDatabaseName("travelpay_user_transfer_notification_test")
            .withUsername("travelpay_test")
            .withPassword(UUID.randomUUID().toString())
            .withInitScript("postgres/user-schema.sql")
            .withUrlParam("currentSchema", "user_service");

    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:8.2.11-bookworm"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--requirepass", REDIS_PASSWORD, "--appendonly", "no", "--save", "");

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");

    private static final HttpServer SLACK;
    private static final List<String> SLACK_BODIES = new CopyOnWriteArrayList<>();
    private static final AtomicInteger SLACK_STATUS = new AtomicInteger(200);

    static {
        POSTGRES.start();
        REDIS.start();
        KAFKA.start();
        try (var admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TransferCompletedConsumer.TOPIC,
                    TransferNotificationKafkaConfig.TOPIC_PARTITIONS, (short) 1))).all().get();
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
        try {
            SLACK = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException exception) {
            throw new IllegalStateException(exception);
        }
        SLACK.createContext("/services/test-hook", exchange -> {
            SLACK_BODIES.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(SLACK_STATUS.get(), -1);
            exchange.close();
        });
        SLACK.start();
    }

    @DynamicPropertySource
    static void connections(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> REDIS_PASSWORD);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("travelpay.user.transfer-notification.slack-webhook-url",
                () -> "http://127.0.0.1:" + SLACK.getAddress().getPort() + "/services/test-hook");
    }

    @AfterAll
    static void stopSlack() {
        SLACK.stop(0);
    }

    @Autowired private JdbcTemplate jdbc;
    @Autowired private UserRepository users;

    private final KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));

    @BeforeEach
    void slackAnswersOk() {
        SLACK_STATUS.set(200);
    }

    @AfterEach
    void closeProducer() {
        producer.close();
    }

    // 보낸 분·받는 분을 마스킹한 이름과 회원 ID, 각자 기준의 증감으로 표시. 회원이 없으면 이름 대신 알 수 없음
    @Test
    void aCompletedTransferIsPostedToSlackOnceWithMaskedNamesAndSignedAmounts() {
        var transferId = UUID.randomUUID();
        var senderId = users.save(User.register("홍길동", randomPhone(), Instant.now())).getId();
        var receiverId = UUID.randomUUID();
        var eventId = UUID.randomUUID();

        send(transferId.toString(), eventId.toString(), payload(transferId, senderId, receiverId, 1000, "KRW"));

        awaitNotified(eventId);
        var bodies = bodiesFor(transferId);
        assertEquals(1, bodies.size());
        var body = bodies.getFirst();
        assertTrue(body.startsWith("{\"text\":"), body);
        assertTrue(body.contains("송금 완료 · 1,000 KRW"), body);
        assertTrue(body.contains("보낸 분 홍*동 (" + senderId + ") -1,000 KRW"), body);
        assertTrue(body.contains("받는 분 알 수 없는 회원 (" + receiverId + ") +1,000 KRW"), body);
        assertFalse(body.contains("홍길동"), body);
        assertEquals(transferId, jdbc.queryForObject(
                "select transfer_id from user_service.transfer_notifications where event_id = ?", UUID.class, eventId));
    }

    @Test
    void theSameEventDeliveredTwiceIsPostedOnce() {
        var transferId = UUID.randomUUID();
        var eventId = UUID.randomUUID();
        var payload = payload(transferId, UUID.randomUUID(), UUID.randomUUID(), 3000, "KRW");

        send(transferId.toString(), eventId.toString(), payload);
        awaitNotified(eventId);
        send(transferId.toString(), eventId.toString(), payload);
        awaitMarkerAfter(transferId.toString());

        assertEquals(1, bodiesFor(transferId).size());
        assertEquals(1, jdbc.queryForObject(
                "select count(*) from user_service.transfer_notifications where event_id = ?", Integer.class, eventId));
        assertEquals(2, screenNotifications(transferId).size(), "재전달돼도 화면 알림은 회원당 한 행");
    }

    // 이벤트 하나로 보낸 사람·받는 사람 화면 알림이 한 행씩 생기고, 상대방 ID가 서로 엇갈려 들어감
    @Test
    void eachPartyGetsOneUnreadScreenNotification() {
        var transferId = UUID.randomUUID();
        var eventId = UUID.randomUUID();
        var senderId = UUID.randomUUID();
        var receiverId = UUID.randomUUID();

        send(transferId.toString(), eventId.toString(), payload(transferId, senderId, receiverId, 7000, "KRW"));
        awaitNotified(eventId);

        var rows = screenNotifications(transferId);
        assertEquals(2, rows.size());
        var byUser = new java.util.HashMap<UUID, Map<String, Object>>();
        rows.forEach(row -> byUser.put((UUID) row.get("user_id"), row));
        assertEquals("TRANSFER_SENT", byUser.get(senderId).get("type"));
        assertEquals(receiverId, byUser.get(senderId).get("counterpart_id"));
        assertEquals("TRANSFER_RECEIVED", byUser.get(receiverId).get("type"));
        assertEquals(senderId, byUser.get(receiverId).get("counterpart_id"));
        rows.forEach(row -> {
            assertEquals(eventId, row.get("event_id"));
            assertEquals(7000L, ((Number) row.get("amount")).longValue());
            assertNull(row.get("read_at"));
        });
    }

    // Slack이 계속 실패하면 1초 간격 2번 다시 보낸 뒤 포기하고 다음 메시지로 넘어감. 수신 기록은 미발송으로 남고,
    // 같은 이벤트가 다시 전달되면(payment 재발행) 그때 보냄
    @Test
    void aSlackFailureIsRetriedThenLeftUnnotifiedAndSentWhenTheEventComesAgain() {
        var transferId = UUID.randomUUID();
        var eventId = UUID.randomUUID();
        var payload = payload(transferId, UUID.randomUUID(), UUID.randomUUID(), 5000, "KRW");
        SLACK_STATUS.set(500);

        send(transferId.toString(), eventId.toString(), payload);
        await(() -> bodiesFor(transferId).size() >= 3, "재시도 포함 3번 전송");
        SLACK_STATUS.set(200);
        awaitMarkerAfter(transferId.toString());

        assertEquals(3, bodiesFor(transferId).size());
        assertNull(notifiedAt(eventId));
        assertEquals(2, screenNotifications(transferId).size(), "Slack이 실패해도 화면 알림은 남음");

        send(transferId.toString(), eventId.toString(), payload);
        awaitNotified(eventId);
        assertEquals(4, bodiesFor(transferId).size());
    }

    // 형식이 잘못된 메시지는 재시도 없이 넘기고, 뒤의 정상 메시지는 처리함(소비자가 멈추지 않음)
    @Test
    void malformedEventsAreSkippedAndTheConsumerKeepsGoing() {
        var key = UUID.randomUUID().toString();
        var withoutHeader = UUID.randomUUID();

        send(key, null, payload(withoutHeader, UUID.randomUUID(), UUID.randomUUID(), 1000, "KRW"));
        send(key, UUID.randomUUID().toString(), "{not json");
        send(key, UUID.randomUUID().toString(), payload(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 0, "KRW"));
        awaitMarkerAfter(key);

        assertEquals(0, bodiesFor(withoutHeader).size());
        assertEquals(0, jdbc.queryForObject(
                "select count(*) from user_service.transfer_notifications where transfer_id = ?", Integer.class,
                withoutHeader));
    }

    private List<Map<String, Object>> screenNotifications(UUID transferId) {
        return jdbc.queryForList("""
                select user_id, event_id, type, counterpart_id, amount, read_at
                from user_service.user_notifications where transfer_id = ?
                """, transferId);
    }

    /** 같은 키(같은 파티션)로 정상 이벤트를 보내고 그것이 알려질 때까지 기다립니다. */
    private void awaitMarkerAfter(String key) {
        var markerEvent = UUID.randomUUID();
        send(key, markerEvent.toString(), payload(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 1, "KRW"));
        awaitNotified(markerEvent);
    }

    private void awaitNotified(UUID eventId) {
        await(() -> notifiedAt(eventId) != null, "eventId=" + eventId + " 알림");
        assertNotNull(notifiedAt(eventId));
    }

    private Instant notifiedAt(UUID eventId) {
        var rows = jdbc.queryForList(
                "select notified_at from user_service.transfer_notifications where event_id = ?", eventId);
        if (rows.isEmpty() || rows.getFirst().get("notified_at") == null) {
            return null;
        }
        return ((Timestamp) rows.getFirst().get("notified_at")).toInstant();
    }

    private static List<String> bodiesFor(UUID transferId) {
        return SLACK_BODIES.stream().filter(body -> body.contains(transferId.toString())).toList();
    }

    private static void await(BooleanSupplier condition, String what) {
        var deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(what + "을(를) " + WAIT + " 안에 확인하지 못했습니다.");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new AssertionError(exception);
            }
        }
    }

    private void send(String key, String eventId, String value) {
        var record = new ProducerRecord<>(TransferCompletedConsumer.TOPIC, key, value);
        if (eventId != null) {
            record.headers().add(TransferCompletedConsumer.EVENT_ID_HEADER, eventId.getBytes(StandardCharsets.UTF_8));
        }
        try {
            producer.send(record).get();
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static String randomPhone() {
        var digits = String.format("%08d", Math.floorMod(UUID.randomUUID().getLeastSignificantBits(), 100_000_000L));
        return "010-" + digits.substring(0, 4) + "-" + digits.substring(4);
    }

    /** payment-service TransferCompletedPayload와 같은 6개 필드 */
    private static String payload(UUID transferId, UUID senderId, UUID receiverId, long amount, String currency) {
        return """
                {"transferId":"%s","senderId":"%s","receiverId":"%s","amount":%d,"currency":"%s","completedAt":"%s"}
                """.formatted(transferId, senderId, receiverId, amount, currency, Instant.parse("2026-11-20T07:50:41.938833Z"))
                .strip();
    }
}
