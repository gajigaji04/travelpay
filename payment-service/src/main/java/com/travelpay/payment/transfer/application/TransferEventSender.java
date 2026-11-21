package com.travelpay.payment.transfer.application;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** 송금 완료 이벤트를 메시지 브로커로 보냅니다. 응답은 future로 돌려주고 기다리지 않습니다. */
public interface TransferEventSender {

    /**
     * @param transferId 메시지 키(같은 송금의 이벤트는 같은 파티션)
     * @param eventId    이벤트 ID(Outbox 행 ID). 재발행 때도 같으며 소비자가 중복 확인에 씁니다
     * @param payload    이벤트 본문 JSON
     */
    CompletableFuture<Void> sendCompleted(UUID transferId, UUID eventId, String payload);
}
