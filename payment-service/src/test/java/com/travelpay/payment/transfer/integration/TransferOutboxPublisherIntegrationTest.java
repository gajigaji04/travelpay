package com.travelpay.payment.transfer.integration;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.travelpay.payment.transfer.application.TransferOutboxPublisher;
import com.travelpay.payment.transfer.client.KafkaTransferEventSender;
import com.travelpay.payment.transfer.config.TransferKafkaTopicConfig;
import com.travelpay.payment.transfer.domain.TransferStatus;
import com.travelpay.payment.transfer.dto.TransferCreateRequest;
import com.travelpay.payment.transfer.dto.TransferResponse;
import com.travelpay.payment.transfer.support.TransferOutboxIntegrationSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.KafkaContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 송금 완료 이벤트 발행기({@link TransferOutboxPublisher})를 실제 PostgreSQL과 Kafka로 확인합니다.
 * 스케줄러는 끈 채(테스트 공통 설정) 발행 메소드를 직접 호출합니다. 토픽 선언(NewTopic)도 꺼져 있어 테스트가 토픽을 만듭니다.
 * 토픽은 테스트끼리 공유하므로 메시지는 이번 테스트가 넣은 eventId로만 셉니다. Docker가 없으면 건너뜁니다.
 * (구성은 wallet-service OutboxPublisherIntegrationTest와 같습니다.)
 */
class TransferOutboxPublisherIntegrationTest extends TransferOutboxIntegrationSupport {

    private static final String TOPIC = TransferKafkaTopicConfig.TRANSFER_COMPLETED;
    private static final int BATCH_SIZE = 50;

    // 로컬 compose·운영과 같은 이미지입니다(wallet-service 테스트와 같음).
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, TransferKafkaTopicConfig.TRANSFER_COMPLETED_PARTITIONS,
                    (short) 1))).all().get();
        }
    }

    @Autowired
    private TransferOutboxPublisher publisher;

    /** 계약: 토픽 payment.transfer.completed, 키 transferId, 헤더 eventId(Outbox 행 ID), 값은 Outbox에 저장한 6개 필드입니다. */
    @Test
    void publishesCompletedTransferEventWithContractAndMarksPublished() {
        UUID sender = member();
        UUID receiver = member();
        TransferResponse response = transferService.request(sender, "publish-contract",
                new TransferCreateRequest(receiver, 3_000L, "KRW", null));
        UUID transferId = response.transferId();
        UUID eventId = (UUID) outboxOf(transferId).get("id");

        assertEquals(1, publisher.publishPending(BATCH_SIZE));
        // 발행된 건은 다시 가져가지 않습니다.
        assertEquals(0, publisher.publishPending(BATCH_SIZE));

        Map<String, Object> row = outboxById(eventId);
        assertEquals("PUBLISHED", row.get("status"));
        assertNotNull(row.get("published_at"));
        assertEquals(0, row.get("retry_count"));

        List<ConsumerRecord<String, String>> records = consume(Set.of(eventId), 1);
        assertEquals(1, records.size());
        ConsumerRecord<String, String> record = records.getFirst();
        assertEquals(TOPIC, record.topic());
        assertEquals(transferId.toString(), record.key());
        assertEquals(eventId.toString(), eventIdOf(record));
        Map<String, Object> payload = parse(record.value());
        assertEquals(Set.of("transferId", "senderId", "receiverId", "amount", "currency", "completedAt"),
                payload.keySet());
        assertEquals(transferId.toString(), payload.get("transferId"));
        assertEquals(sender.toString(), payload.get("senderId"));
        assertEquals(receiver.toString(), payload.get("receiverId"));
        assertEquals(3_000L, ((Number) payload.get("amount")).longValue());
        assertEquals("KRW", payload.get("currency"));
        assertEquals(updatedAtOf(transferId), Instant.parse((String) payload.get("completedAt")));
    }

    /**
     * Kafka가 멈춰 있어도 송금은 평소처럼 COMPLETED로 끝나고(요청 경로는 Kafka를 부르지 않음) Outbox 행이 PENDING으로 남습니다.
     * 발행은 실패 횟수만 올리고 행을 유지하며, Kafka가 돌아오면 같은 eventId로 발행합니다.
     */
    @Test
    void transferCompletesWhileKafkaIsDownAndEventIsPublishedAfterRecovery() {
        UUID sender = member();
        UUID receiver = member();
        UUID transferId;
        UUID eventId;

        pauseKafka();
        try {
            // Kafka 전송 대기(max.block 3초)에 걸리지 않고 바로 끝나야 합니다.
            TransferResponse response = assertTimeout(Duration.ofMillis(2500), () -> transferService.request(
                    sender, "publish-outage", new TransferCreateRequest(receiver, AMOUNT, "KRW", null)));
            transferId = response.transferId();
            assertEquals(TransferStatus.COMPLETED, response.status());
            assertEquals("COMPLETED", statusOf(transferId));
            eventId = (UUID) outboxOf(transferId).get("id");

            assertEquals(0, publisher.publishPending(BATCH_SIZE));
            Map<String, Object> row = outboxById(eventId);
            assertEquals("PENDING", row.get("status"));
            assertNull(row.get("published_at"));
            assertEquals(1, row.get("retry_count"));
        } finally {
            unpauseKafka();
        }

        // 브로커 재접속 시간이 걸릴 수 있어 다음 주기처럼 몇 번 다시 호출합니다.
        Instant deadline = Instant.now().plusSeconds(60);
        while (publisher.publishPending(BATCH_SIZE) == 0 && Instant.now().isBefore(deadline)) {
            // 다시 호출
        }

        assertEquals("PUBLISHED", outboxById(eventId).get("status"));
        assertEquals("COMPLETED", statusOf(transferId));
        // 멈춘 동안 시간 초과로 실패 처리한 요청이 브로커 재개 후 기록될 수 있어 중복은 허용합니다(같은 eventId, 소비자가 걸러 냄).
        List<ConsumerRecord<String, String>> records = consume(Set.of(eventId), 1);
        assertTrue(records.size() >= 1);
        records.forEach(record -> {
            assertEquals(eventId.toString(), eventIdOf(record));
            assertEquals(transferId.toString(), record.key());
        });
    }

    /** 서버 2대의 발행기가 동시에 돌아도 같은 이벤트를 함께 가져가지 않습니다(SKIP LOCKED). */
    @Test
    void twoPublishersDoNotPublishSameEvent() throws Exception {
        Set<UUID> eventIds = new HashSet<>();
        Instant base = Instant.now().minusSeconds(60);
        for (int i = 0; i < 20; i++) {
            eventIds.add(insertCompletedTransferWithPendingOutbox(base.plusMillis(i)));
        }

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Integer> first = executor.submit(() -> {
                start.await();
                return publisher.publishPending(10);
            });
            Future<Integer> second = executor.submit(() -> {
                start.await();
                return publisher.publishPending(10);
            });
            start.countDown();

            assertEquals(10, first.get());
            assertEquals(10, second.get());
        } finally {
            executor.shutdownNow();
        }

        assertEquals(20, jdbcTemplate.queryForObject(
                "select count(*) from transfer_outbox where status = 'PUBLISHED'", Integer.class));
        List<ConsumerRecord<String, String>> records = consume(eventIds, eventIds.size());
        assertEquals(20, records.size());
        Set<String> received = new HashSet<>();
        records.forEach(record -> received.add(eventIdOf(record)));
        assertEquals(20, received.size());
    }

    /** 보낸 뒤 상태 변경이 실패해 롤백되면 PENDING으로 남고, 다음 호출에서 같은 eventId로 다시 발행합니다(at-least-once). */
    @Test
    void republishesWithSameEventIdWhenMarkingPublishedFails() {
        UUID eventId = insertCompletedTransferWithPendingOutbox(Instant.now().minusSeconds(60));

        jdbcTemplate.execute("""
                create or replace function fail_transfer_outbox_publish() returns trigger as $$
                begin
                    raise exception 'transfer_outbox 상태 변경 실패 (테스트)';
                end;
                $$ language plpgsql
                """);
        jdbcTemplate.execute("""
                create trigger fail_transfer_outbox_publish before update on transfer_outbox
                for each row when (new.status = 'PUBLISHED') execute function fail_transfer_outbox_publish()
                """);
        try {
            assertThrows(RuntimeException.class, () -> publisher.publishPending(BATCH_SIZE));
        } finally {
            jdbcTemplate.execute("drop trigger if exists fail_transfer_outbox_publish on transfer_outbox");
            jdbcTemplate.execute("drop function if exists fail_transfer_outbox_publish()");
        }

        Map<String, Object> rolledBack = outboxById(eventId);
        assertEquals("PENDING", rolledBack.get("status"));
        assertEquals(0, rolledBack.get("retry_count"));

        assertEquals(1, publisher.publishPending(BATCH_SIZE));
        assertEquals("PUBLISHED", outboxById(eventId).get("status"));

        List<ConsumerRecord<String, String>> records = consume(Set.of(eventId), 2);
        assertEquals(2, records.size());
        records.forEach(record -> assertEquals(eventId.toString(), eventIdOf(record)));
    }

    /** 토픽을 처음부터 읽어 eventId가 일치하는 메시지를 모읍니다. expected건을 받은 뒤에도 잠시 더 읽어 중복을 확인합니다. */
    private List<ConsumerRecord<String, String>> consume(Set<UUID> eventIds, int expected) {
        Set<String> ids = new HashSet<>();
        eventIds.forEach(id -> ids.add(id.toString()));
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            Instant deadline = Instant.now().plusSeconds(30);
            Instant settleUntil = null;
            while (Instant.now().isBefore(deadline) && (settleUntil == null || Instant.now().isBefore(settleUntil))) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (ids.contains(eventIdOf(record))) {
                        matched.add(record);
                    }
                }
                if (settleUntil == null && matched.size() >= expected) {
                    settleUntil = Instant.now().plusSeconds(2);
                }
            }
        }
        return matched;
    }

    private static String eventIdOf(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(KafkaTransferEventSender.EVENT_ID_HEADER);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void pauseKafka() {
        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private static void unpauseKafka() {
        KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    }
}
