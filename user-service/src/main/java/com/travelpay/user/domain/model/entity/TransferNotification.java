package com.travelpay.user.domain.model.entity;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * payment-service에서 받은 송금 완료 이벤트와 Slack 알림 여부입니다.
 * 이벤트 ID가 기본키라 같은 이벤트를 다시 받아도 한 행만 남고, notifiedAt이 있으면 이미 알린 이벤트입니다.
 * 행은 수신 기록 저장소(insert ... on conflict do nothing)가 만들고, 여기서는 알림 완료 표시만 바꿉니다.
 */
@Entity
@Table(name = "transfer_notifications")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TransferNotification {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "amount", nullable = false, updatable = false)
    private long amount;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(name = "completed_at", nullable = false, updatable = false)
    private Instant completedAt;

    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;

    @Column(name = "notified_at")
    private Instant notifiedAt;

    public boolean isNotified() {
        return notifiedAt != null;
    }

    /** Slack 전송이 성공한 뒤 호출합니다. 이미 알린 이벤트를 다시 표시하지 않습니다. */
    public void markNotified(Instant notifiedAt) {
        if (notifiedAt == null) {
            throw new IllegalArgumentException("알림 시각은 비어 있을 수 없습니다.");
        }
        if (this.notifiedAt != null) {
            throw new IllegalStateException("이미 알린 송금 완료 이벤트입니다. eventId=" + eventId);
        }
        this.notifiedAt = notifiedAt;
    }
}
