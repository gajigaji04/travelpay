package com.travelpay.user.infrastructure.persistence;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.user.domain.model.entity.UserNotification;
import com.travelpay.user.domain.model.vo.UserNotificationType;
import com.travelpay.user.domain.repository.UserNotificationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

/** 회원 화면 알림의 저장·조회 계약을 Spring Data JPA에 연결합니다. */
@Repository
@RequiredArgsConstructor
public class UserNotificationRepositoryImpl implements UserNotificationRepository {

    private final UserNotificationJpaRepository userNotificationJpaRepository;

    @Override
    public boolean recordIfAbsent(UUID eventId, UUID userId, UserNotificationType type, UUID transferId,
            UUID counterpartId, long amount, String currency, Instant occurredAt, Instant createdAt) {
        return userNotificationJpaRepository.insertIfAbsent(eventId, userId, type.name(), transferId, counterpartId,
                amount, currency, occurredAt, createdAt) == 1;
    }

    @Override
    public Page<UserNotification> findByUserId(UUID userId, Pageable pageable) {
        return userNotificationJpaRepository.findByUserId(userId, pageable);
    }

    @Override
    public long countUnread(UUID userId) {
        return userNotificationJpaRepository.countUnread(userId);
    }

    @Override
    public Optional<UserNotification> findByIdAndUserId(UUID notificationId, UUID userId) {
        return userNotificationJpaRepository.findByIdAndUserId(notificationId, userId);
    }

    @Override
    public int markAllRead(UUID userId, Instant readAt) {
        return userNotificationJpaRepository.markAllRead(userId, readAt);
    }
}
