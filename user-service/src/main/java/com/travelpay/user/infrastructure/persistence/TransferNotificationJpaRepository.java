package com.travelpay.user.infrastructure.persistence;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.user.domain.model.entity.TransferNotification;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA가 구현하는 송금 완료 이벤트 수신 기록 저장소입니다. */
public interface TransferNotificationJpaRepository extends JpaRepository<TransferNotification, UUID> {

    // 같은 이벤트가 동시에 와도 기본키 충돌을 예외 없이 넘기도록 PostgreSQL의 on conflict를 씁니다.
    // 스키마는 연결의 currentSchema(user_service)를 따르므로 테이블 이름만 씁니다.
    @Modifying
    @Query(value = """
            insert into transfer_notifications
                (event_id, transfer_id, amount, currency, completed_at, received_at, notified_at)
            values (:eventId, :transferId, :amount, :currency, :completedAt, :receivedAt, null)
            on conflict (event_id) do nothing
            """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") UUID eventId, @Param("transferId") UUID transferId,
            @Param("amount") long amount, @Param("currency") String currency,
            @Param("completedAt") Instant completedAt, @Param("receivedAt") Instant receivedAt);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select notification from TransferNotification notification where notification.eventId = :eventId")
    Optional<TransferNotification> findByEventIdForUpdate(@Param("eventId") UUID eventId);

    // JPQL에는 limit·skip locked가 없어 native로 씁니다. Slack 전송 중(findByEventIdForUpdate)인 행은 건너뜁니다.
    @Modifying
    @Query(value = """
            delete from transfer_notifications
            where event_id in (
                select event_id from transfer_notifications
                where received_at < :cutoff
                order by received_at
                limit :limit
                for update skip locked)
            """, nativeQuery = true)
    int deleteReceivedBefore(@Param("cutoff") Instant cutoff, @Param("limit") int limit);
}
