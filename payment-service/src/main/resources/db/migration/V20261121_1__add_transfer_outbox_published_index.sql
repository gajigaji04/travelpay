-- 발행한 지 보관 기간(기본 7일)이 지난 송금 완료 이벤트를 지울 때 발행 시각 순으로 찾습니다(TransferOutboxCleanupService).
-- PUBLISHED 행만 담는 부분 인덱스라 미발행(PENDING) 조회용 ix_transfer_outbox_pending과 겹치지 않습니다.
CREATE INDEX ix_transfer_outbox_published ON transfer_outbox (published_at) WHERE status = 'PUBLISHED';
