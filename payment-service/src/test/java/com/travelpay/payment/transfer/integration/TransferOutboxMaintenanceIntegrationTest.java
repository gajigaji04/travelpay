package com.travelpay.payment.transfer.integration;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.travelpay.payment.transfer.application.TransferOutboxBacklogMonitor;
import com.travelpay.payment.transfer.application.TransferOutboxCleanupService;
import com.travelpay.payment.transfer.domain.TransferOutboxRepository;
import com.travelpay.payment.transfer.support.TransferOutboxIntegrationSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 송금 완료 이벤트 Outbox 운영 작업을 실제 PostgreSQL로 확인합니다.
 * <ul>
 *   <li>PUBLISHED 정리({@link TransferOutboxCleanupService}): 보관 기간이 지난 PUBLISHED만 지우고 PENDING은 남김</li>
 *   <li>PENDING 적체 지표({@link TransferOutboxBacklogMonitor}): 건수와 가장 오래된 건의 나이</li>
 * </ul>
 * 두 스케줄러는 테스트 공통 설정에서 꺼 두고 직접 호출합니다(지표는 테스트용 레지스트리·고정 시계로 만듦).
 */
class TransferOutboxMaintenanceIntegrationTest extends TransferOutboxIntegrationSupport {

    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);

    @Autowired
    private TransferOutboxCleanupService cleanupService;

    @Autowired
    private TransferOutboxRepository outboxRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    // 보관 기간(기본 7일)이 지난 PUBLISHED만 지움. 기간 안의 PUBLISHED와 아무리 오래된 PENDING도 남김
    @Test
    void purgesOnlyPublishedEventsOlderThanRetention() {
        UUID oldPublished = publishedEvent(NOW.minus(Duration.ofDays(8)));
        UUID recentPublished = publishedEvent(NOW.minus(Duration.ofDays(6)));
        UUID oldPending = insertCompletedTransferWithPendingOutbox(NOW.minus(Duration.ofDays(30)));

        assertEquals(1, cleanupService.purge(NOW));

        assertEquals(0, countById(oldPublished));
        assertEquals(1, countById(recentPublished));
        assertEquals(1, countById(oldPending));
    }

    // 한 트랜잭션에 batchSize건씩, 남은 행이 있으면 같은 주기 안에서 이어서 지움
    @Test
    void purgesInBatchesUntilNothingIsLeft() {
        var service = new TransferOutboxCleanupService(outboxRepository, transactionManager, Duration.ofDays(7), 2);
        for (int i = 0; i < 5; i++) {
            publishedEvent(NOW.minus(Duration.ofDays(10)).plusSeconds(i));
        }

        assertEquals(5, service.purge(NOW));
        assertEquals(0, service.purge(NOW));
        assertEquals(0, publishedCount());
    }

    // 다른 트랜잭션이 잡고 있는 행은 기다리지 않고 건너뛰고, 풀린 뒤 다음 주기에 지움
    @Test
    void skipsLockedRowsAndPurgesThemNextTime() throws Exception {
        UUID locked = publishedEvent(NOW.minus(Duration.ofDays(9)));
        UUID free = publishedEvent(NOW.minus(Duration.ofDays(9)));
        var lockHeld = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var holder = CompletableFuture.runAsync(() -> new TransactionTemplate(transactionManager)
                .executeWithoutResult(status -> {
                    jdbcTemplate.queryForList("select id from transfer_outbox where id = ? for update", locked);
                    lockHeld.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }));
        assertTrue(lockHeld.await(10, TimeUnit.SECONDS));

        try {
            assertEquals(1, cleanupService.purge(NOW));
            assertEquals(1, countById(locked));
            assertEquals(0, countById(free));
        } finally {
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
        }

        assertEquals(1, cleanupService.purge(NOW));
        assertEquals(0, countById(locked));
    }

    @Test
    void rejectsRetentionShorterThanOneDayAndInvalidBatchSize() {
        assertThrows(IllegalStateException.class, () -> new TransferOutboxCleanupService(
                outboxRepository, transactionManager, Duration.ofHours(23), 1000));
        assertThrows(IllegalStateException.class, () -> new TransferOutboxCleanupService(
                outboxRepository, transactionManager, Duration.ofDays(7), 0));
        assertThrows(IllegalStateException.class, () -> new TransferOutboxCleanupService(
                outboxRepository, transactionManager, Duration.ofDays(7), 10_001));
    }

    // PENDING 건수와 가장 오래된 PENDING의 나이(초). PUBLISHED는 세지 않음. 나이는 지표를 읽는 시각 기준
    @Test
    void backlogGaugesReportPendingCountAndOldestAge() {
        var registry = new SimpleMeterRegistry();
        var monitor = new TransferOutboxBacklogMonitor(outboxRepository, registry, Clock.fixed(NOW, ZoneOffset.UTC));

        monitor.refresh();
        assertEquals(0.0, gauge(registry, TransferOutboxBacklogMonitor.PENDING));
        assertEquals(0.0, gauge(registry, TransferOutboxBacklogMonitor.OLDEST_PENDING_AGE));

        insertCompletedTransferWithPendingOutbox(NOW.minusSeconds(600));
        insertCompletedTransferWithPendingOutbox(NOW.minusSeconds(60));
        publishedEvent(NOW.minus(Duration.ofDays(1)));
        monitor.refresh();

        assertEquals(2.0, gauge(registry, TransferOutboxBacklogMonitor.PENDING));
        assertEquals(600.0, gauge(registry, TransferOutboxBacklogMonitor.OLDEST_PENDING_AGE));

        jdbcTemplate.update("delete from transfer_outbox where status = 'PENDING'");
        monitor.refresh();
        assertEquals(0.0, gauge(registry, TransferOutboxBacklogMonitor.PENDING));
        assertEquals(0.0, gauge(registry, TransferOutboxBacklogMonitor.OLDEST_PENDING_AGE));
    }

    private static double gauge(SimpleMeterRegistry registry, String name) {
        return registry.get(name).gauge().value();
    }

    /** 완료 송금과 publishedAt에 발행된 Outbox 행을 넣고 이벤트 ID를 반환합니다. */
    private UUID publishedEvent(Instant publishedAt) {
        UUID eventId = insertCompletedTransferWithPendingOutbox(publishedAt.minusSeconds(1));
        jdbcTemplate.update("update transfer_outbox set status = 'PUBLISHED', published_at = ? where id = ?",
                publishedAt.atOffset(ZoneOffset.UTC), eventId);
        return eventId;
    }

    private int countById(UUID eventId) {
        return jdbcTemplate.queryForObject("select count(*) from transfer_outbox where id = ?", Integer.class, eventId);
    }

    private int publishedCount() {
        return jdbcTemplate.queryForObject(
                "select count(*) from transfer_outbox where status = 'PUBLISHED'", Integer.class);
    }
}
