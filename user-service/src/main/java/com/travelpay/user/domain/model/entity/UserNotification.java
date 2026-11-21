package com.travelpay.user.domain.model.entity;

import java.time.Instant;
import java.util.UUID;

import com.travelpay.user.domain.model.vo.UserNotificationType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 회원 화면에 보여 줄 송금 완료 알림입니다(V5 user_notifications).
 * 행은 저장소의 insert ... on conflict do nothing이 만들고, 여기서는 읽음 표시만 바꿉니다.
 * 상대방 이름은 저장하지 않습니다.
 */
@Entity
@Table(name = "user_notifications",
        uniqueConstraints = @UniqueConstraint(name = "uk_user_notifications_event_user",
                columnNames = {"event_id", "user_id"}))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UserNotification {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "user_id", nullable = false, updatable = false)
    private UUID userId;

    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, updatable = false, length = 30)
    private UserNotificationType type;

    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "counterpart_id", nullable = false, updatable = false)
    private UUID counterpartId;

    @Column(name = "amount", nullable = false, updatable = false)
    private long amount;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "read_at")
    private Instant readAt;

    public boolean isRead() {
        return readAt != null;
    }

    /** 읽음으로 표시합니다. 이미 읽은 알림은 처음 읽은 시각을 그대로 둡니다. */
    public void markRead(Instant readAt) {
        if (readAt == null) {
            throw new IllegalArgumentException("읽은 시각은 비어 있을 수 없습니다.");
        }
        if (this.readAt == null) {
            this.readAt = readAt;
        }
    }
}
