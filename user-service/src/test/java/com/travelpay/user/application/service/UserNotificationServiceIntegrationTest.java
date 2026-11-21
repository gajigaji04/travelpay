package com.travelpay.user.application.service;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import com.travelpay.common.error.BusinessException;
import com.travelpay.common.error.ErrorCode;
import com.travelpay.common.error.CommonErrorCode;
import com.travelpay.user.common.config.JpaAuditingConfiguration;
import com.travelpay.user.common.error.UserErrorCode;
import com.travelpay.user.domain.model.entity.User;
import com.travelpay.user.domain.model.vo.UserNotificationType;
import com.travelpay.user.domain.repository.UserNotificationRepository;
import com.travelpay.user.infrastructure.persistence.JpaRepositoryTestConfiguration;
import com.travelpay.user.infrastructure.persistence.PostgresJpaTestConfiguration;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 화면 알림의 기록(중복 무시)·목록(최신순·마스킹 이름)·읽음 표시를 실제 PostgreSQL로 확인합니다. */
@SpringJUnitConfig(classes = {
        JpaAuditingConfiguration.class, PostgresJpaTestConfiguration.class, JpaRepositoryTestConfiguration.class,
        UserNotificationService.class, UserNotificationServiceIntegrationTest.TestBeans.class
})
class UserNotificationServiceIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-11-21T03:00:00Z");
    private static final Instant AT = Instant.parse("2026-10-31T00:00:00Z");

    @Autowired private UserNotificationService service;
    @Autowired private UserNotificationRepository notifications;
    @Autowired private EntityManagerFactory entityManagerFactory;
    @Autowired private PlatformTransactionManager transactionManager;

    private EntityManager entityManager;
    private TransactionTemplate transactions;
    private UUID me;
    private UUID friend;

    @BeforeEach
    void setUp() {
        entityManager = SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);
        transactions = new TransactionTemplate(transactionManager);
        transactions.executeWithoutResult(status -> {
            for (var entity : List.of("UserNotification", "UserConsent", "UserCredentials", "User")) {
                entityManager.createQuery("delete from " + entity).executeUpdate();
            }
        });
        me = saveMember("김송금", "010-1900-0001");
        friend = saveMember("홍길동", "010-1900-0002");
    }

    @Test
    void theSameEventIsRecordedOncePerMember() {
        var eventId = UUID.randomUUID();

        assertTrue(record(eventId, me, UserNotificationType.TRANSFER_RECEIVED, friend, NOW.minusSeconds(60)));
        assertFalse(record(eventId, me, UserNotificationType.TRANSFER_RECEIVED, friend, NOW.minusSeconds(60)));
        assertTrue(record(eventId, friend, UserNotificationType.TRANSFER_SENT, me, NOW.minusSeconds(60)));

        assertEquals(1, service.list(me, 0, 20).getTotalElements());
        assertEquals(1, service.list(friend, 0, 20).getTotalElements());
    }

    @Test
    void listIsNewestFirstWithMaskedCounterpartNamesAndOnlyMine() {
        var missing = UUID.randomUUID();
        record(UUID.randomUUID(), me, UserNotificationType.TRANSFER_RECEIVED, friend, NOW.minusSeconds(120));
        record(UUID.randomUUID(), me, UserNotificationType.TRANSFER_SENT, missing, NOW.minusSeconds(60));
        record(UUID.randomUUID(), friend, UserNotificationType.TRANSFER_SENT, me, NOW.minusSeconds(30));

        var page = service.list(me, 0, 20);

        assertEquals(2, page.getTotalElements());
        var newest = page.getContent().get(0);
        assertEquals(UserNotificationType.TRANSFER_SENT, newest.type());
        assertEquals(missing, newest.counterpartId());
        assertNull(newest.counterpartName(), "회원을 찾지 못하면 이름 없이 보낸다");
        var older = page.getContent().get(1);
        assertEquals(friend, older.counterpartId());
        assertEquals("홍*동", older.counterpartName());
        assertFalse(older.read());
        assertEquals(1, service.list(me, 1, 1).getContent().size());
        assertEquals(2, service.list(me, 0, 1000).getContent().size(), "100을 넘는 size도 오류 없이 상한으로 조회");
    }

    @Test
    void markReadOnlyTouchesMyOwnNotificationAndIsIdempotent() {
        record(UUID.randomUUID(), me, UserNotificationType.TRANSFER_RECEIVED, friend, NOW.minusSeconds(60));
        record(UUID.randomUUID(), friend, UserNotificationType.TRANSFER_SENT, me, NOW.minusSeconds(60));
        var mine = service.list(me, 0, 20).getContent().getFirst().notificationId();
        var theirs = service.list(friend, 0, 20).getContent().getFirst().notificationId();
        assertEquals(1, service.countUnread(me));

        service.markRead(me, mine);
        service.markRead(me, mine);

        assertEquals(0, service.countUnread(me));
        assertTrue(service.list(me, 0, 20).getContent().getFirst().read());
        assertError(UserErrorCode.NOTIFICATION_NOT_FOUND, () -> service.markRead(me, theirs));
        assertError(UserErrorCode.NOTIFICATION_NOT_FOUND, () -> service.markRead(me, UUID.randomUUID()));
        assertEquals(1, service.countUnread(friend));
    }

    @Test
    void markAllReadChangesOnlyMyUnreadNotifications() {
        record(UUID.randomUUID(), me, UserNotificationType.TRANSFER_RECEIVED, friend, NOW.minusSeconds(90));
        record(UUID.randomUUID(), me, UserNotificationType.TRANSFER_SENT, friend, NOW.minusSeconds(60));
        record(UUID.randomUUID(), friend, UserNotificationType.TRANSFER_SENT, me, NOW.minusSeconds(60));

        assertEquals(2, service.markAllRead(me));
        assertEquals(0, service.markAllRead(me));
        assertEquals(0, service.countUnread(me));
        assertEquals(1, service.countUnread(friend));
    }

    @Test
    void invalidPagingIsRejected() {
        assertError(CommonErrorCode.INVALID_REQUEST, () -> service.list(me, -1, 20));
        assertError(CommonErrorCode.INVALID_REQUEST, () -> service.list(me, 0, 0));
    }

    private boolean record(UUID eventId, UUID userId, UserNotificationType type, UUID counterpartId,
            Instant occurredAt) {
        return Boolean.TRUE.equals(transactions.execute(status -> notifications.recordIfAbsent(eventId, userId, type,
                UUID.randomUUID(), counterpartId, 1000, "KRW", occurredAt, NOW)));
    }

    private UUID saveMember(String realName, String phone) {
        return transactions.execute(status -> {
            var user = User.register(realName, phone, AT);
            entityManager.persist(user);
            return user.getId();
        });
    }

    private static void assertError(ErrorCode expected, Executable call) {
        assertEquals(expected, assertThrows(BusinessException.class, call).getErrorCode());
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }
}
