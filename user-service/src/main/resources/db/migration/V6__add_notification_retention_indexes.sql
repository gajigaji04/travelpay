-- user-service: src/main/resources/db/migration/V6__add_notification_retention_indexes.sql
-- 스키마: user_service
-- 보관 기간 정리(NotificationRetentionService)가 오래된 행을 찾을 때 테이블 전체를 읽지 않도록 인덱스를 둡니다.
-- 화면 알림은 읽은 것만 지우므로 읽은 행만 담는 부분 인덱스로 둡니다.

CREATE INDEX ix_user_notifications_read_occurred
    ON user_notifications (occurred_at)
    WHERE read_at IS NOT NULL;

CREATE INDEX ix_transfer_notifications_received
    ON transfer_notifications (received_at);
