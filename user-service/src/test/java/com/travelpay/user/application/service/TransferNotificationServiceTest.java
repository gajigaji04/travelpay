package com.travelpay.user.application.service;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.user.application.dto.TransferCompletedEvent;
import com.travelpay.user.application.service.TransferNotificationMessage.Party;
import com.travelpay.user.application.service.TransferNotificationService.Outcome;
import com.travelpay.user.domain.model.entity.TransferNotification;
import com.travelpay.user.domain.model.entity.User;
import com.travelpay.user.domain.repository.TransferNotificationRepository;
import com.travelpay.user.domain.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 알림 여부 판단만 봅니다. 실제 DB 잠금·Kafka 재시도는 TransferCompletedConsumerIntegrationTest가 확인합니다. */
class TransferNotificationServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-20T08:00:00Z");

    private final TransferNotificationRepository repository = mock(TransferNotificationRepository.class);
    private final TransferNotificationSender sender = mock(TransferNotificationSender.class);
    private final UserRepository userRepository = mock(UserRepository.class);
    private final TransferNotificationService service = new TransferNotificationService(repository, sender,
            userRepository, new CallbackOnlyTransactionManager(), Clock.fixed(NOW, ZoneOffset.UTC));
    private final TransferCompletedEvent event = new TransferCompletedEvent(UUID.randomUUID(), UUID.randomUUID(),
            UUID.randomUUID(), UUID.randomUUID(), 1000, "KRW", Instant.parse("2026-11-20T07:50:41Z"));

    private TransferNotification notification;

    @BeforeEach
    void recordedEvent() {
        notification = TransferNotificationMessageTest.notification(
                event.transferId(), event.amount(), event.currency(), event.completedAt());
        when(repository.findByEventIdForUpdate(event.eventId())).thenReturn(Optional.of(notification));
    }

    @Test
    void recordsWithoutSendingWhenNoWebhookIsConfigured() {
        when(sender.isConfigured()).thenReturn(false);

        assertEquals(Outcome.RECORDED_ONLY, service.handle(event));

        verify(repository).recordIfAbsent(event.eventId(), event.transferId(), 1000, "KRW", event.completedAt(), NOW);
        verify(sender, never()).send(any());
        assertFalse(notification.isNotified());
    }

    @Test
    void sendsWithBothPartiesNamesAndMarksTheEventNotified() {
        var senderUser = mock(User.class);
        when(senderUser.getRealName()).thenReturn("홍길동");
        when(userRepository.findById(event.senderId())).thenReturn(Optional.of(senderUser));
        when(userRepository.findById(event.receiverId())).thenReturn(Optional.empty());
        when(sender.isConfigured()).thenReturn(true);

        assertEquals(Outcome.NOTIFIED, service.handle(event));

        verify(sender).send(TransferNotificationMessage.of(notification,
                new Party(event.senderId(), "홍길동"), new Party(event.receiverId(), null)));
        assertEquals(NOW, notification.getNotifiedAt());
    }

    @Test
    void doesNotSendAnEventThatWasAlreadyNotified() {
        notification.markNotified(NOW.minusSeconds(60));
        when(sender.isConfigured()).thenReturn(true);

        assertEquals(Outcome.ALREADY_NOTIFIED, service.handle(event));

        verify(sender, never()).send(any());
    }

    @Test
    void aFailedSendLeavesTheEventUnnotifiedAndPropagatesForRetry() {
        when(sender.isConfigured()).thenReturn(true);
        doThrow(new TransferNotificationSendException("HTTP 500")).when(sender).send(any());

        assertThrows(TransferNotificationSendException.class, () -> service.handle(event));

        assertNull(notification.getNotifiedAt());
    }

    /** 트랜잭션 경계 없이 콜백만 실행합니다. */
    private static final class CallbackOnlyTransactionManager extends AbstractPlatformTransactionManager {
        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
