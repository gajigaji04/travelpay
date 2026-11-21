-- user-service: src/main/resources/db/migration/V5__create_user_notifications.sql
-- 스키마: user_service
-- 회원 화면에 보여 줄 송금 완료 알림입니다. 엔티티 UserNotification과 같은 구조여야 합니다.
-- 송금 완료 이벤트 하나로 보낸 사람(TRANSFER_SENT)·받는 사람(TRANSFER_RECEIVED) 알림을 한 행씩 만듭니다.
-- Kafka 재전달에도 회원당 한 행만 남도록 (event_id, user_id)를 유일하게 둡니다.
-- 상대방 이름은 저장하지 않고 조회할 때 users에서 읽어 마스킹합니다. read_at이 비어 있으면 읽지 않은 알림입니다.

CREATE TABLE user_notifications (
    id              UUID         NOT NULL,
    user_id         UUID         NOT NULL,
    event_id        UUID         NOT NULL,
    type            VARCHAR(30)  NOT NULL,
    transfer_id     UUID         NOT NULL,
    counterpart_id  UUID         NOT NULL,
    amount          BIGINT       NOT NULL,
    currency        VARCHAR(3)   NOT NULL,
    occurred_at     TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    read_at         TIMESTAMPTZ,

    CONSTRAINT user_notifications_pkey PRIMARY KEY (id),
    CONSTRAINT uk_user_notifications_event_user UNIQUE (event_id, user_id),
    -- 이름은 Hibernate가 enum 열에 만드는 검사와 같게 둠(FlywayMigrationTest가 엔티티와 비교)
    CONSTRAINT user_notifications_type_check CHECK (type IN ('TRANSFER_SENT', 'TRANSFER_RECEIVED'))
);

-- 회원별 최신순 목록
CREATE INDEX ix_user_notifications_user_occurred
    ON user_notifications (user_id, occurred_at DESC, id DESC);

-- 읽지 않은 개수(프런트 주기 조회)
CREATE INDEX ix_user_notifications_user_unread
    ON user_notifications (user_id)
    WHERE read_at IS NULL;
