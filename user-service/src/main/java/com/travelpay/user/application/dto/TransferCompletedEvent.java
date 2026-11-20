package com.travelpay.user.application.dto;

import java.time.Instant;
import java.util.Currency;
import java.util.UUID;

/**
 * payment-service가 발행한 송금 완료 이벤트 중 알림에 쓰는 값입니다.
 * 보낸 사람·받는 사람 ID는 Slack 문구에 이름과 함께 넣지만 DB에는 저장하지 않습니다.
 *
 * @param eventId     Kafka 헤더 eventId(payment Outbox 행 ID). 재발행 때도 같은 값이라 중복 판단에 씁니다
 * @param amount      통화별 최소 단위 정수(KRW는 원, USD는 센트)
 * @param completedAt 송금이 COMPLETED로 바뀐 시각(UTC)
 */
public record TransferCompletedEvent(UUID eventId, UUID transferId, UUID senderId, UUID receiverId, long amount,
        String currency, Instant completedAt) {

    public TransferCompletedEvent {
        if (eventId == null || transferId == null || senderId == null || receiverId == null || completedAt == null) {
            throw new IllegalArgumentException(
                    "송금 완료 이벤트에 eventId·transferId·senderId·receiverId·completedAt이 있어야 합니다.");
        }
        if (amount <= 0) {
            throw new IllegalArgumentException("송금 완료 이벤트의 금액은 0보다 커야 합니다. amount=" + amount);
        }
        if (currency == null || !currency.matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("송금 완료 이벤트의 통화는 ISO 4217 세 글자여야 합니다. currency=" + currency);
        }
        try {
            Currency.getInstance(currency);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("알 수 없는 통화입니다. currency=" + currency, exception);
        }
    }
}
