package com.travelpay.user.domain.repository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.user.domain.model.entity.TransferNotification;

/** 송금 완료 이벤트 수신 기록의 저장·조회 계약입니다. */
public interface TransferNotificationRepository {

    /**
     * 처음 받은 이벤트면 알리지 않은 상태로 기록하고, 이미 있는 이벤트 ID면 아무것도 바꾸지 않습니다.
     * 같은 이벤트가 동시에 와도 한 행만 남습니다.
     *
     * @return 이번에 새로 기록했으면 true
     */
    boolean recordIfAbsent(UUID eventId, UUID transferId, long amount, String currency, Instant completedAt,
            Instant receivedAt);

    /** 알림을 보낼 이벤트를 잠금 조회합니다. 쓰기 트랜잭션 안에서 호출하며 종료 시 잠금을 해제합니다. */
    Optional<TransferNotification> findByEventIdForUpdate(UUID eventId);
}
