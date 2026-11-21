package com.travelpay.payment.transfer.application;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 발행한 지 보관 기간이 지난 송금 완료 이벤트(Outbox PUBLISHED 행)를 주기적으로 지웁니다({@link TransferOutboxCleanupService}).
 * 한 시간에 한 번, 한 주기에 길어야 수 초라 스케줄러 풀(spring.task.scheduling.pool.size)을 늘리지 않습니다.
 * 서버가 여러 대면 모두 돌지만 잠긴 행은 건너뛰고 지우므로(SKIP LOCKED) 겹쳐도 같은 결과입니다.
 * transfer.outbox.cleanup.enabled=false이면 돌지 않습니다(테스트·수동 확인용).
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "transfer.outbox.cleanup", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class TransferOutboxCleanupScheduler {

    private static final Logger log = LoggerFactory.getLogger(TransferOutboxCleanupScheduler.class);

    private final TransferOutboxCleanupService cleanupService;

    public TransferOutboxCleanupScheduler(TransferOutboxCleanupService cleanupService) {
        this.cleanupService = cleanupService;
    }

    /** 한 주기가 실패해도 다음 주기는 돌도록 예외를 로그로만 남깁니다. */
    @Scheduled(initialDelayString = "${transfer.outbox.cleanup.initial-delay:5m}",
            fixedDelayString = "${transfer.outbox.cleanup.fixed-delay:1h}")
    public void purge() {
        try {
            cleanupService.purge(Instant.now());
        } catch (RuntimeException exception) {
            log.error("송금 완료 이벤트 정리 주기 실패 exception={}", exception.getClass().getName(), exception);
        }
    }
}
