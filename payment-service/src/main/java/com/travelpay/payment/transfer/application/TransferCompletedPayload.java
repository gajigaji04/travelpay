package com.travelpay.payment.transfer.application;

import java.time.Instant;
import java.util.UUID;

import com.travelpay.payment.transfer.domain.Transfer;

/**
 * 송금 완료 이벤트(payment.transfer.completed) 본문입니다. 소비자와의 계약이므로 필드를 바꿀 때는 소비자 담당과 먼저 협의합니다.
 * amount는 통화별 최소 단위 정수, completedAt은 송금이 COMPLETED로 바뀐 시각(UTC ISO-8601)입니다.
 */
public record TransferCompletedPayload(
        UUID transferId,
        UUID senderId,
        UUID receiverId,
        long amount,
        String currency,
        Instant completedAt
) {

    public static TransferCompletedPayload of(Transfer transfer, Instant completedAt) {
        return new TransferCompletedPayload(transfer.getId(), transfer.getSenderId(), transfer.getReceiverId(),
                transfer.getAmount(), transfer.getCurrency(), completedAt);
    }
}
