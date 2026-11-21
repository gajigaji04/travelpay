package com.travelpay.user.presentation.scheduler;

import com.travelpay.user.application.service.NotificationRetentionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 보관 기간이 지난 화면 알림·송금 완료 수신 기록을 주기적으로 지웁니다.
 * 서버가 여러 대면 모두 돌지만, 잠긴 행은 건너뛰고 지우므로(SKIP LOCKED) 겹쳐도 같은 결과입니다.
 * travelpay.user.notification-retention.enabled=false이면 돌지 않습니다(테스트·수동 확인용).
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "travelpay.user.notification-retention", name = "enabled", havingValue = "true",
        matchIfMissing = true)
@RequiredArgsConstructor
public class NotificationRetentionScheduler {

    private final NotificationRetentionService retention;

    /** 한 주기가 실패해도 다음 주기는 돌도록 예외를 로그로만 남깁니다. */
    @Scheduled(initialDelayString = "${travelpay.user.notification-retention.initial-delay:5m}",
            fixedDelayString = "${travelpay.user.notification-retention.fixed-delay:1h}")
    public void purge() {
        try {
            retention.purge();
        } catch (RuntimeException exception) {
            log.error("알림 보관 기간 정리 주기 실패: exception={}", exception.getClass().getName(), exception);
        }
    }
}
