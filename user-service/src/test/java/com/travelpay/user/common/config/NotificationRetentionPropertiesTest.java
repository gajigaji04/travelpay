package com.travelpay.user.common.config;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 알림 보관 기간 설정이 너무 짧거나 배치 크기가 범위를 벗어나면 서버 시작 단계에서 거부합니다. */
class NotificationRetentionPropertiesTest {

    private static final Duration READ = Duration.ofDays(90);
    private static final Duration TRANSFER = Duration.ofDays(30);

    @Test
    void theBoundaryValuesAreAccepted() {
        assertDoesNotThrow(() -> new NotificationRetentionProperties(Duration.ofDays(1), Duration.ofDays(7), 1));
        assertDoesNotThrow(() -> new NotificationRetentionProperties(READ, TRANSFER,
                NotificationRetentionProperties.MAX_BATCH_SIZE));
    }

    @Test
    void readNotificationRetentionMustBeAtLeastOneDay() {
        assertThrows(IllegalArgumentException.class, () -> new NotificationRetentionProperties(null, TRANSFER, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new NotificationRetentionProperties(Duration.ofHours(23), TRANSFER, 10));
    }

    // 수신 기록을 토픽 보관 기간보다 먼저 지우면 재전달된 이벤트를 다시 Slack으로 보낼 수 있음
    @Test
    void transferEventRetentionMustOutliveTheTopicRetention() {
        assertThrows(IllegalArgumentException.class, () -> new NotificationRetentionProperties(READ, null, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new NotificationRetentionProperties(READ, Duration.ofDays(7).minusSeconds(1), 10));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, NotificationRetentionProperties.MAX_BATCH_SIZE + 1})
    void batchSizeOutsideTheRangeIsRejected(int batchSize) {
        assertThrows(IllegalArgumentException.class,
                () -> new NotificationRetentionProperties(READ, TRANSFER, batchSize));
    }
}
