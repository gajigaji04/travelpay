package com.travelpay.user.infrastructure.persistence;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.user.domain.model.entity.TransferNotification;
import com.travelpay.user.domain.repository.TransferNotificationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

/** 송금 완료 이벤트 수신 기록의 저장·조회 계약을 Spring Data JPA에 연결합니다. */
@Repository
@RequiredArgsConstructor
public class TransferNotificationRepositoryImpl implements TransferNotificationRepository {

    private final TransferNotificationJpaRepository transferNotificationJpaRepository;

    @Override
    public boolean recordIfAbsent(UUID eventId, UUID transferId, long amount, String currency, Instant completedAt,
            Instant receivedAt) {
        return transferNotificationJpaRepository.insertIfAbsent(
                eventId, transferId, amount, currency, completedAt, receivedAt) == 1;
    }

    @Override
    public Optional<TransferNotification> findByEventIdForUpdate(UUID eventId) {
        return transferNotificationJpaRepository.findByEventIdForUpdate(eventId);
    }

    @Override
    public int deleteReceivedBefore(Instant cutoff, int limit) {
        return transferNotificationJpaRepository.deleteReceivedBefore(cutoff, limit);
    }
}
