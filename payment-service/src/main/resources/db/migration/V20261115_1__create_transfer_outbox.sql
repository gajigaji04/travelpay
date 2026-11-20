-- 송금 완료 이벤트 Outbox입니다. 송금이 COMPLETED로 바뀌는 트랜잭션에서 함께 저장하고, 발행기가 Kafka(payment.transfer.completed)로 보냅니다.
-- id는 이벤트 ID(Kafka 헤더 eventId)이며, 발행에 실패해도 행을 남겨 같은 ID로 다시 보냅니다.
-- 송금 하나에 완료 이벤트는 하나입니다(uk_transfer_outbox_transfer_id). 이 테이블을 만들기 전에 완료된 송금은 넣지 않습니다(backfill 없음).
CREATE TABLE transfer_outbox (
    id           UUID        NOT NULL,
    transfer_id  UUID        NOT NULL,
    payload      JSONB       NOT NULL,
    status       VARCHAR(20) NOT NULL,
    retry_count  INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT pk_transfer_outbox PRIMARY KEY (id),
    CONSTRAINT uk_transfer_outbox_transfer_id UNIQUE (transfer_id),
    CONSTRAINT fk_transfer_outbox_transfer FOREIGN KEY (transfer_id) REFERENCES transfer (id),
    CONSTRAINT ck_transfer_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED')),
    CONSTRAINT ck_transfer_outbox_published_at CHECK ((status = 'PUBLISHED') = (published_at IS NOT NULL)),
    CONSTRAINT ck_transfer_outbox_retry_count CHECK (retry_count >= 0)
);

-- 발행기: 미발행 건만 조회합니다.
CREATE INDEX ix_transfer_outbox_pending ON transfer_outbox (created_at, id) WHERE status = 'PENDING';
