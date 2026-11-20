package com.travelpay.payment.transfer.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 미발행 송금 완료 이벤트(Outbox)를 주기적으로 발행합니다({@link TransferOutboxPublisher}).
 * Kafka가 멈추면 한 주기가 몇 초씩 걸리므로 송금 복구·결제 정산 복구와 다른 스케줄러 스레드에서 돌도록 풀 크기를 3으로 둡니다
 * (spring.task.scheduling.pool.size).
 * 서버가 여러 대면 모두 돌지만 SKIP LOCKED로 서로 다른 건을 발행합니다.
 * transfer.outbox.publisher.enabled=false이면 돌지 않고 Outbox 행은 PENDING으로 쌓입니다.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "transfer.outbox.publisher", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class TransferOutboxPublishScheduler {

    private static final Logger log = LoggerFactory.getLogger(TransferOutboxPublishScheduler.class);

    private static final String BATCH_SIZE_KEY = "transfer.outbox.publisher.batch-size";

    private final TransferOutboxPublisher publisher;
    private final int batchSize;

    /** 설정이 잘못되면 IllegalStateException으로 기동을 멈춥니다. */
    public TransferOutboxPublishScheduler(TransferOutboxPublisher publisher,
            @Value("${" + BATCH_SIZE_KEY + ":200}") int batchSize) {
        if (batchSize < 1) {
            throw new IllegalStateException(BATCH_SIZE_KEY + "는 1 이상이어야 합니다: " + batchSize);
        }
        this.publisher = publisher;
        this.batchSize = batchSize;
    }

    /** 한 주기가 실패해도 다음 주기는 돌도록 예외를 로그로만 남깁니다. */
    @Scheduled(fixedDelayString = "${transfer.outbox.publisher.fixed-delay:1s}")
    public void publish() {
        try {
            publisher.publishPending(batchSize);
        } catch (RuntimeException exception) {
            // 롤백되어 모두 PENDING으로 남고 다음 주기에 다시 보냅니다.
            log.warn("송금 완료 이벤트 발행 트랜잭션 실패 exception={}", exception.getClass().getName());
        }
    }
}
