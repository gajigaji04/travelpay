package com.travelpay.user.infrastructure.persistence;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.user.domain.model.entity.UserNotification;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA가 구현하는 회원 화면 알림 저장소입니다. */
public interface UserNotificationJpaRepository extends JpaRepository<UserNotification, UUID> {

    // 같은 이벤트가 다시 와도 (event_id, user_id) 유일 조건 충돌을 예외 없이 넘기도록 on conflict를 씁니다.
    // 스키마는 연결의 currentSchema(user_service)를 따르므로 테이블 이름만 씁니다.
    @Modifying
    @Query(value = """
            insert into user_notifications
                (id, user_id, event_id, type, transfer_id, counterpart_id, amount, currency,
                 occurred_at, created_at, read_at)
            values (gen_random_uuid(), :userId, :eventId, :type, :transferId, :counterpartId, :amount, :currency,
                    :occurredAt, :createdAt, null)
            on conflict (event_id, user_id) do nothing
            """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") UUID eventId, @Param("userId") UUID userId, @Param("type") String type,
            @Param("transferId") UUID transferId, @Param("counterpartId") UUID counterpartId,
            @Param("amount") long amount, @Param("currency") String currency,
            @Param("occurredAt") Instant occurredAt, @Param("createdAt") Instant createdAt);

    @Query(value = """
            select notification from UserNotification notification
            where notification.userId = :userId
            order by notification.occurredAt desc, notification.id desc
            """,
            countQuery = "select count(notification) from UserNotification notification where notification.userId = :userId")
    Page<UserNotification> findByUserId(@Param("userId") UUID userId, Pageable pageable);

    @Query("""
            select count(notification) from UserNotification notification
            where notification.userId = :userId and notification.readAt is null
            """)
    long countUnread(@Param("userId") UUID userId);

    Optional<UserNotification> findByIdAndUserId(UUID id, UUID userId);

    @Modifying
    @Query("""
            update UserNotification notification set notification.readAt = :readAt
            where notification.userId = :userId and notification.readAt is null
            """)
    int markAllRead(@Param("userId") UUID userId, @Param("readAt") Instant readAt);
}
