package com.travelpay.user.application.dto;

import java.time.Instant;
import java.util.UUID;

import com.travelpay.user.domain.model.vo.UserNotificationType;

/**
 * 회원 화면 알림 한 건입니다. counterpartName은 마스킹한 이름이며, 상대 회원을 찾지 못하면 null입니다.
 */
public record UserNotificationResult(UUID notificationId, UserNotificationType type, UUID transferId,
        UUID counterpartId, String counterpartName, long amount, String currency, Instant occurredAt, boolean read) {
}
