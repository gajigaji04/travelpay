package com.travelpay.user.presentation.scheduler;

import com.travelpay.user.application.service.NotificationRetentionService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 정리 스케줄러는 설정으로 끌 수 있고, 한 주기가 실패해도 예외를 밖으로 던지지 않습니다. */
class NotificationRetentionSchedulerTest {

    // 서비스 클래스의 @EnableConfigurationProperties도 처리되므로 보관 기간 설정을 함께 줌
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withPropertyValues("travelpay.user.notification-retention.read-notification-retention=90d",
                    "travelpay.user.notification-retention.transfer-event-retention=30d",
                    "travelpay.user.notification-retention.batch-size=1000")
            .withBean(NotificationRetentionService.class, () -> mock(NotificationRetentionService.class))
            .withUserConfiguration(NotificationRetentionScheduler.class);

    @Test
    void theSchedulerRunsUnlessDisabled() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(NotificationRetentionScheduler.class));
        contextRunner.withPropertyValues("travelpay.user.notification-retention.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(NotificationRetentionScheduler.class));
    }

    @Test
    void aFailedRunIsLoggedSoTheNextRunStillHappens() {
        var service = mock(NotificationRetentionService.class);
        when(service.purge()).thenThrow(new IllegalStateException("DB 연결 실패"));
        var scheduler = new NotificationRetentionScheduler(service);

        assertDoesNotThrow(scheduler::purge);
        assertDoesNotThrow(scheduler::purge);
        verify(service, times(2)).purge();
    }
}
