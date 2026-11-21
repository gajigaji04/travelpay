package com.travelpay.user.common.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 화면 알림과 송금 완료 수신 기록의 보관 기간입니다. 정리 주기와 켜기 여부는 스케줄러가 같은 접두사로 따로 읽습니다.
 *
 * @param readNotificationRetention 읽은 화면 알림을 발생 시각(occurred_at)부터 남겨 둘 기간(1일 이상).
 *                                  읽지 않은 알림은 기간이 지나도 지우지 않습니다
 * @param transferEventRetention    송금 완료 수신 기록(Slack 중복 거르기)을 받은 시각(received_at)부터 남겨 둘 기간.
 *                                  지운 뒤 같은 이벤트가 다시 오면 중복을 거르지 못하므로 토픽 보관 기간(Kafka 기본 7일)보다
 *                                  길어야 하며, 7일 미만은 거부합니다
 * @param batchSize                 한 트랜잭션에서 지울 최대 행 수(1~10,000)
 */
@ConfigurationProperties("travelpay.user.notification-retention")
public record NotificationRetentionProperties(Duration readNotificationRetention, Duration transferEventRetention,
        int batchSize) {

    public static final int MAX_BATCH_SIZE = 10_000;
    private static final Duration MIN_READ_NOTIFICATION_RETENTION = Duration.ofDays(1);
    private static final Duration MIN_TRANSFER_EVENT_RETENTION = Duration.ofDays(7);

    public NotificationRetentionProperties {
        if (readNotificationRetention == null
                || readNotificationRetention.compareTo(MIN_READ_NOTIFICATION_RETENTION) < 0) {
            throw new IllegalArgumentException(
                    "travelpay.user.notification-retention.read-notification-retention은 1일 이상이어야 합니다(예: 90d).");
        }
        if (transferEventRetention == null || transferEventRetention.compareTo(MIN_TRANSFER_EVENT_RETENTION) < 0) {
            throw new IllegalArgumentException("travelpay.user.notification-retention.transfer-event-retention은 "
                    + "토픽 보관 기간보다 긴 7일 이상이어야 합니다(예: 30d).");
        }
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("travelpay.user.notification-retention.batch-size는 1 이상 "
                    + MAX_BATCH_SIZE + " 이하여야 합니다. value=" + batchSize);
        }
    }
}
