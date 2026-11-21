package com.travelpay.payment.transfer.application;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import com.travelpay.payment.transfer.domain.TransferOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 발행한 지 보관 기간이 지난 송금 완료 이벤트(Outbox PUBLISHED 행)를 나눠서 지웁니다. 미발행(PENDING) 행은 지우지 않습니다.
 * user-service 알림 보관 기간 정리(NotificationRetentionService)와 같은 방식입니다.
 * <ul>
 *   <li>한 번에 batchSize행씩 각자 트랜잭션으로 지워 잠금을 짧게 유지합니다</li>
 *   <li>다른 트랜잭션이 잡고 있는 행은 건너뛰고(SKIP LOCKED) 다음 주기에 지웁니다. 서버가 여러 대여도 서로 기다리지 않습니다</li>
 *   <li>한 주기에 최대 {@value #MAX_BATCHES_PER_RUN}번까지만 지워, 처음 켤 때 쌓인 행이 많아도 스케줄러 스레드를 오래 잡지 않습니다.
 *   남은 행은 다음 주기에 이어서 지웁니다</li>
 * </ul>
 * 지운 뒤에는 그 이벤트를 다시 발행할 수 없으므로, 보관 기간은 소비자 장애를 알아채고 다시 보낼 여유보다 길어야 합니다(1일 미만 거부).
 */
@Service
public class TransferOutboxCleanupService {

    private static final Logger log = LoggerFactory.getLogger(TransferOutboxCleanupService.class);

    static final int MAX_BATCHES_PER_RUN = 100;
    static final int MAX_BATCH_SIZE = 10_000;
    static final Duration MIN_RETENTION = Duration.ofDays(1);
    private static final String RETENTION_KEY = "transfer.outbox.cleanup.retention";
    private static final String BATCH_SIZE_KEY = "transfer.outbox.cleanup.batch-size";
    // 인덱스로 찾은 최대 1만 행을 지우는 트랜잭션의 실행 상한. 잠긴 행은 건너뛰므로 행 잠금 대기는 없음
    private static final int TRANSACTION_TIMEOUT_SECONDS = 10;

    private final TransferOutboxRepository outboxRepository;
    private final TransactionTemplate transactions;
    private final Duration retention;
    private final int batchSize;

    /** 설정이 잘못되면 IllegalStateException으로 기동을 멈춥니다. */
    public TransferOutboxCleanupService(TransferOutboxRepository outboxRepository,
            PlatformTransactionManager transactionManager,
            @Value("${" + RETENTION_KEY + ":7d}") Duration retention,
            @Value("${" + BATCH_SIZE_KEY + ":1000}") int batchSize) {
        if (retention == null || retention.compareTo(MIN_RETENTION) < 0) {
            throw new IllegalStateException(RETENTION_KEY + "는 1일 이상이어야 합니다(예: 7d): " + retention);
        }
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalStateException(
                    BATCH_SIZE_KEY + "는 1 이상 " + MAX_BATCH_SIZE + " 이하여야 합니다: " + batchSize);
        }
        this.outboxRepository = outboxRepository;
        this.transactions = new TransactionTemplate(transactionManager);
        this.transactions.setTimeout(TRANSACTION_TIMEOUT_SECONDS);
        this.retention = retention;
        this.batchSize = batchSize;
    }

    /** now 기준으로 보관 기간이 지난 PUBLISHED 행을 지우고 지운 행 수를 반환합니다. */
    public int purge(Instant now) {
        Instant cutoff = now.minus(retention);
        int total = 0;
        for (int batch = 0; batch < MAX_BATCHES_PER_RUN; batch++) {
            int deleted = Objects.requireNonNull(
                    transactions.execute(status -> outboxRepository.deletePublishedBefore(cutoff, batchSize)));
            total += deleted;
            if (deleted < batchSize) {
                break;
            }
        }
        if (total > 0) {
            log.info("보관 기간이 지난 송금 완료 이벤트 정리 deleted={} publishedBefore={}", total, cutoff);
        }
        return total;
    }
}
