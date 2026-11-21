package com.travelpay.user.application.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.travelpay.user.common.config.JpaAuditingConfiguration;
import com.travelpay.user.domain.model.vo.UserNotificationType;
import com.travelpay.user.domain.repository.TransferNotificationRepository;
import com.travelpay.user.domain.repository.UserNotificationRepository;
import com.travelpay.user.infrastructure.persistence.JpaRepositoryTestConfiguration;
import com.travelpay.user.infrastructure.persistence.PostgresJpaTestConfiguration;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 보관 기간 정리가 읽은 오래된 화면 알림과 오래된 수신 기록만, 잠긴 행은 건너뛰고 지우는지 실제 PostgreSQL로 확인합니다. */
@SpringJUnitConfig(classes = {
        JpaAuditingConfiguration.class, PostgresJpaTestConfiguration.class, JpaRepositoryTestConfiguration.class,
        NotificationRetentionService.class, NotificationRetentionServiceIntegrationTest.TestBeans.class
})
// 배치 크기를 작게 두어 여러 번 나눠 지우는 경로도 확인
@TestPropertySource(properties = {
        "travelpay.user.notification-retention.read-notification-retention=90d",
        "travelpay.user.notification-retention.transfer-event-retention=30d",
        "travelpay.user.notification-retention.batch-size=2"
})
class NotificationRetentionServiceIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-11-21T03:00:00Z");
    private static final Instant READ_CUTOFF = NOW.minus(Duration.ofDays(90));
    private static final Instant TRANSFER_CUTOFF = NOW.minus(Duration.ofDays(30));

    @Autowired private NotificationRetentionService service;
    @Autowired private UserNotificationRepository notifications;
    @Autowired private TransferNotificationRepository transferEvents;
    @Autowired private EntityManagerFactory entityManagerFactory;
    @Autowired private PlatformTransactionManager transactionManager;

    private EntityManager entityManager;
    private TransactionTemplate transactions;

    @BeforeEach
    void setUp() {
        entityManager = SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);
        transactions = new TransactionTemplate(transactionManager);
        transactions.executeWithoutResult(status -> {
            entityManager.createQuery("delete from UserNotification").executeUpdate();
            entityManager.createQuery("delete from TransferNotification").executeUpdate();
        });
    }

    @Test
    void onlyReadNotificationsOlderThanTheRetentionAreDeletedAcrossBatches() {
        var me = UUID.randomUUID();
        var friend = UUID.randomUUID();
        // 지울 대상 3건(배치 크기 2 → 2건, 1건으로 나눠 지움)
        recordNotification(me, READ_CUTOFF.minusSeconds(1));
        recordNotification(me, READ_CUTOFF.minus(Duration.ofDays(30)));
        recordNotification(friend, READ_CUTOFF.minus(Duration.ofDays(1)));
        // 남길 대상: 기준 시각과 같은 것, 기간 안의 것
        var boundary = recordNotification(me, READ_CUTOFF);
        var recent = recordNotification(friend, NOW.minus(Duration.ofDays(1)));
        markAllRead(me, friend);
        // 남길 대상: 오래됐어도 읽지 않은 것
        var unread = recordNotification(me, READ_CUTOFF.minus(Duration.ofDays(365)));

        var result = service.purge();

        assertEquals(new NotificationRetentionService.Result(3, 0), result);
        assertEquals(3, count("UserNotification"));
        assertTrue(notificationExists(boundary), "기준 시각과 같은 알림은 아직 기간 안");
        assertTrue(notificationExists(recent));
        assertTrue(notificationExists(unread), "읽지 않은 알림은 기간이 지나도 남김");
        assertEquals(new NotificationRetentionService.Result(0, 0), service.purge(), "다시 돌려도 더 지울 것이 없음");
    }

    @Test
    void transferEventsOlderThanTheRetentionAreDeletedWhetherNotifiedOrNot() {
        recordTransferEvent(UUID.randomUUID(), TRANSFER_CUTOFF.minusSeconds(1));
        recordTransferEvent(UUID.randomUUID(), TRANSFER_CUTOFF.minus(Duration.ofDays(10)));
        recordTransferEvent(UUID.randomUUID(), TRANSFER_CUTOFF.minus(Duration.ofDays(20)));
        var boundary = recordTransferEvent(UUID.randomUUID(), TRANSFER_CUTOFF);
        var recent = recordTransferEvent(UUID.randomUUID(), NOW.minusSeconds(60));

        var result = service.purge();

        assertEquals(new NotificationRetentionService.Result(0, 3), result);
        assertEquals(2, count("TransferNotification"));
        assertTrue(transferEventExists(boundary));
        assertTrue(transferEventExists(recent));
    }

    // Slack 전송 중(행 잠금)인 수신 기록은 기다리지 않고 건너뛰었다가, 잠금이 풀린 다음 주기에 지움
    @Test
    void rowsLockedByAnotherTransactionAreSkippedAndDeletedOnTheNextRun() throws Exception {
        var locked = recordTransferEvent(UUID.randomUUID(), TRANSFER_CUTOFF.minus(Duration.ofDays(1)));
        recordTransferEvent(UUID.randomUUID(), TRANSFER_CUTOFF.minus(Duration.ofDays(2)));
        var lockHeld = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var holder = executor.submit(() -> transactions.executeWithoutResult(status -> {
                transferEvents.findByEventIdForUpdate(locked).orElseThrow();
                lockHeld.countDown();
                await(release);
            }));
            assertTrue(lockHeld.await(10, TimeUnit.SECONDS));

            assertEquals(new NotificationRetentionService.Result(0, 1), service.purge());
            assertTrue(transferEventExists(locked));

            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
        }
        finally {
            release.countDown();
            executor.shutdownNow();
        }

        assertEquals(new NotificationRetentionService.Result(0, 1), service.purge());
        assertEquals(0, count("TransferNotification"));
    }

    private UUID recordNotification(UUID userId, Instant occurredAt) {
        var eventId = UUID.randomUUID();
        transactions.executeWithoutResult(status -> notifications.recordIfAbsent(eventId, userId,
                UserNotificationType.TRANSFER_RECEIVED, UUID.randomUUID(), UUID.randomUUID(), 1000, "KRW",
                occurredAt, NOW));
        return eventId;
    }

    private void markAllRead(UUID... userIds) {
        transactions.executeWithoutResult(status -> {
            for (var userId : userIds) {
                notifications.markAllRead(userId, NOW);
            }
        });
    }

    private UUID recordTransferEvent(UUID eventId, Instant receivedAt) {
        transactions.executeWithoutResult(status -> transferEvents.recordIfAbsent(eventId, UUID.randomUUID(), 1000,
                "KRW", receivedAt, receivedAt));
        return eventId;
    }

    private boolean notificationExists(UUID eventId) {
        return transactions.execute(status -> entityManager.createQuery(
                        "select count(n) from UserNotification n where n.eventId = :eventId", Long.class)
                .setParameter("eventId", eventId).getSingleResult()) == 1;
    }

    private boolean transferEventExists(UUID eventId) {
        return transactions.execute(status -> entityManager.createQuery(
                        "select count(n) from TransferNotification n where n.eventId = :eventId", Long.class)
                .setParameter("eventId", eventId).getSingleResult()) == 1;
    }

    private long count(String entity) {
        return transactions.execute(status -> entityManager.createQuery(
                "select count(n) from " + entity + " n", Long.class).getSingleResult());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("잠금 해제 신호를 기다리다 시간이 지났습니다.");
            }
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }
}
