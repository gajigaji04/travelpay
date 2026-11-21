package com.travelpay.user.presentation.dto;

import java.time.Instant;
import java.util.UUID;

import com.travelpay.user.application.dto.UserNotificationResult;
import com.travelpay.user.domain.model.vo.UserNotificationType;
import io.swagger.v3.oas.annotations.media.Schema;

/** 회원 화면 알림 한 건입니다. 문구는 프런트가 type·금액·상대방 이름으로 만듭니다. */
public record UserNotificationResponse(
        @Schema(description = "알림 ID")
        UUID notificationId,
        @Schema(description = "TRANSFER_SENT 내가 보낸 송금 완료, TRANSFER_RECEIVED 나에게 온 송금 완료")
        UserNotificationType type,
        @Schema(description = "송금 ID(송금 상세 조회에 사용)")
        UUID transferId,
        @Schema(description = "상대방 회원 ID(SENT면 받는 분, RECEIVED면 보낸 분)")
        UUID counterpartId,
        @Schema(description = "상대방 마스킹 이름(예: 홍*동). 회원을 찾지 못하면 null", nullable = true, example = "홍*동")
        String counterpartName,
        @Schema(description = "송금 금액(통화별 최소 단위)", example = "10000")
        long amount,
        @Schema(description = "통화 코드", example = "KRW")
        String currency,
        @Schema(description = "송금 완료 시각(UTC, ISO-8601)")
        Instant occurredAt,
        @Schema(description = "읽음 여부")
        boolean read
) {

    public static UserNotificationResponse from(UserNotificationResult result) {
        return new UserNotificationResponse(result.notificationId(), result.type(), result.transferId(),
                result.counterpartId(), result.counterpartName(), result.amount(), result.currency(),
                result.occurredAt(), result.read());
    }
}
