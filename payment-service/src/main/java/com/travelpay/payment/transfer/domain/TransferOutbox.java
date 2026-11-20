package com.travelpay.payment.transfer.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * 송금 완료 이벤트 Outbox입니다. 송금이 COMPLETED로 바뀌는 트랜잭션에서 함께 저장하고, 발행기가 Kafka로 보냅니다.
 * id가 이벤트 ID(Kafka 헤더 eventId)이며 재발행 때도 같은 값을 씁니다. 송금 하나에 한 건만 있습니다(UNIQUE(transfer_id)).
 */
@Entity
@Table(name = "transfer_outbox")
public class TransferOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    /** 이벤트 본문 JSON 문자열입니다(JSONB 컬럼). 저장한 그대로 발행합니다. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20, nullable = false)
    private TransferOutboxStatus status;

    /** 발행에 실패한 횟수입니다. 발행기가 적게 실패한 건부터 보냅니다. */
    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected TransferOutbox() {
    }

    /** 발행 대기(PENDING) 상태로 만듭니다. */
    public static TransferOutbox pending(UUID transferId, String payload, Instant createdAt) {
        TransferOutbox outbox = new TransferOutbox();
        outbox.transferId = Objects.requireNonNull(transferId, "transferId");
        outbox.payload = Objects.requireNonNull(payload, "payload");
        outbox.status = TransferOutboxStatus.PENDING;
        outbox.createdAt = Objects.requireNonNull(createdAt, "createdAt");
        return outbox;
    }

    public void markPublished(Instant publishedAt) {
        this.status = TransferOutboxStatus.PUBLISHED;
        this.publishedAt = Objects.requireNonNull(publishedAt, "publishedAt");
    }

    public void increaseRetryCount() {
        this.retryCount++;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public String getPayload() {
        return payload;
    }

    public TransferOutboxStatus getStatus() {
        return status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
