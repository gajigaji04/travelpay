-- user-service: src/main/resources/db/migration/V4__create_transfer_notifications.sql
-- 스키마: user_service
-- payment-service의 송금 완료 이벤트(payment.transfer.completed)를 받은 기록입니다. 엔티티 TransferNotification과 같은 구조여야 합니다.
-- Kafka 전달은 at-least-once라 같은 이벤트가 다시 올 수 있으므로, 이벤트 ID(헤더 eventId)를 기본키로 두고 중복을 거릅니다.
-- notified_at이 비어 있으면 Slack에 아직 보내지 않은 이벤트입니다(Webhook 미설정이거나 전송 실패).
-- 이름·전화번호 같은 개인정보와 보낸 사람·받는 사람 ID는 저장하지 않습니다.

CREATE TABLE transfer_notifications (
    event_id      UUID         NOT NULL,
    transfer_id   UUID         NOT NULL,
    amount        BIGINT       NOT NULL,
    currency      VARCHAR(3)   NOT NULL,
    completed_at  TIMESTAMPTZ  NOT NULL,
    received_at   TIMESTAMPTZ  NOT NULL,
    notified_at   TIMESTAMPTZ,

    CONSTRAINT transfer_notifications_pkey PRIMARY KEY (event_id)
);
