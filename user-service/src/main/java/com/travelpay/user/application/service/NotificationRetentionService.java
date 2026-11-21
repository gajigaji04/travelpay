package com.travelpay.user.application.service;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

import com.travelpay.user.common.config.NotificationRetentionProperties;
import com.travelpay.user.domain.repository.TransferNotificationRepository;
import com.travelpay.user.domain.repository.UserNotificationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 보관 기간이 지난 화면 알림(읽은 것만)과 송금 완료 수신 기록을 나눠서 지웁니다.
 * <ul>
 *   <li>한 번에 batchSize행씩 각자 트랜잭션으로 지워 잠금을 짧게 유지합니다</li>
 *   <li>다른 트랜잭션이 잡고 있는 행(읽음 표시·Slack 전송 중)은 건너뛰고(SKIP LOCKED) 다음 주기에 지웁니다.
 *   서버가 여러 대여도 같은 행을 두고 기다리지 않습니다</li>
 *   <li>한 주기에 테이블마다 최대 {@value #MAX_BATCHES_PER_RUN}번까지만 지워, 처음 켤 때 쌓인 행이 많아도
 *   한 주기가 길게 이어지지 않게 합니다. 남은 행은 다음 주기에 이어서 지웁니다</li>
 * </ul>
 */
@Slf4j
@Service
@EnableConfigurationProperties(NotificationRetentionProperties.class)
public class NotificationRetentionService {

    static final int MAX_BATCHES_PER_RUN = 100;
    // 인덱스로 찾은 최대 1만 행을 지우는 트랜잭션의 실행 상한. 잠긴 행은 건너뛰므로 행 잠금 대기는 없음
    private static final int TRANSACTION_TIMEOUT_SECONDS = 10;

    private final UserNotificationRepository userNotifications;
    private final TransferNotificationRepository transferNotifications;
    private final NotificationRetentionProperties properties;
    private final TransactionTemplate transactions;
    private final Clock clock;

    public NotificationRetentionService(UserNotificationRepository userNotifications,
            TransferNotificationRepository transferNotifications, NotificationRetentionProperties properties,
            PlatformTransactionManager transactionManager, Clock clock) {
        this.userNotifications = userNotifications;
        this.transferNotifications = transferNotifications;
        this.properties = properties;
        this.transactions = new TransactionTemplate(transactionManager);
        this.transactions.setTimeout(TRANSACTION_TIMEOUT_SECONDS);
        this.clock = clock;
    }

    /** 한 주기를 실행하고 지운 행 수를 돌려줍니다. 기준 시각은 주기 시작 때 한 번 정합니다. */
    public Result purge() {
        var now = clock.instant();
        int readNotifications = deleteInBatches(now.minus(properties.readNotificationRetention()),
                userNotifications::deleteReadOccurredBefore);
        int transferEvents = deleteInBatches(now.minus(properties.transferEventRetention()),
                transferNotifications::deleteReceivedBefore);
        var result = new Result(readNotifications, transferEvents);
        if (result.total() > 0) {
            log.info("보관 기간이 지난 알림 정리 readNotifications={} transferEvents={}", readNotifications, transferEvents);
        }
        return result;
    }

    private int deleteInBatches(Instant cutoff, BatchDelete deleteBatch) {
        int batchSize = properties.batchSize();
        int total = 0;
        for (int batch = 0; batch < MAX_BATCHES_PER_RUN; batch++) {
            int deleted = Objects.requireNonNull(transactions.execute(status -> deleteBatch.delete(cutoff, batchSize)));
            total += deleted;
            if (deleted < batchSize) {
                break;
            }
        }
        return total;
    }

    @FunctionalInterface
    private interface BatchDelete {
        int delete(Instant cutoff, int limit);
    }

    /**
     * @param readNotifications 지운 읽은 화면 알림 수
     * @param transferEvents    지운 송금 완료 수신 기록 수
     */
    public record Result(int readNotifications, int transferEvents) {

        public int total() {
            return readNotifications + transferEvents;
        }
    }
}
