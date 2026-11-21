package com.travelpay.payment.transfer.application;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.travelpay.payment.transfer.domain.TransferOutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 미발행 송금 완료 이벤트(Outbox PENDING)가 쌓이는지 지표로 내보냅니다.
 * <ul>
 *   <li>{@value #PENDING}: PENDING 건수</li>
 *   <li>{@value #OLDEST_PENDING_AGE}(초): 가장 오래된 PENDING 건이 생긴 뒤 지난 시간. 발행기가 1초마다 돌므로 정상이면 몇 초 안이고,
 *   Kafka 장애·발행기 정지(transfer.outbox.publisher.enabled=false 포함) 동안 계속 커집니다</li>
 * </ul>
 * DB 조회는 {@value #REFRESH_INTERVAL_MS}ms마다 한 번만 하고, 나이는 가장 오래된 생성 시각을 기억해 두었다가 지표를 읽을 때 계산합니다.
 * 발행기가 꺼져 있어도 적체를 볼 수 있도록 발행기와 따로 켭니다. transfer.outbox.monitor.enabled=false이면 등록하지 않습니다(테스트용).
 * 조회는 부분 인덱스(PENDING)만 읽어 짧게 끝나므로 스케줄러 풀(spring.task.scheduling.pool.size)을 늘리지 않습니다.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "transfer.outbox.monitor", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class TransferOutboxBacklogMonitor {

    private static final Logger log = LoggerFactory.getLogger(TransferOutboxBacklogMonitor.class);

    public static final String PENDING = "transfer.outbox.pending";
    public static final String OLDEST_PENDING_AGE = "transfer.outbox.pending.oldest.age";
    static final long REFRESH_INTERVAL_MS = 30_000;

    private final TransferOutboxRepository outboxRepository;
    private final Clock clock;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicReference<Instant> oldestPendingCreatedAt = new AtomicReference<>();

    @Autowired
    public TransferOutboxBacklogMonitor(TransferOutboxRepository outboxRepository, MeterRegistry meterRegistry) {
        this(outboxRepository, meterRegistry, Clock.systemUTC());
    }

    /** 나이 계산 시계를 바꿀 때(테스트) 씁니다. */
    public TransferOutboxBacklogMonitor(TransferOutboxRepository outboxRepository, MeterRegistry meterRegistry,
            Clock clock) {
        this.outboxRepository = outboxRepository;
        this.clock = clock;
        Gauge.builder(PENDING, pending, AtomicLong::get)
                .description("발행을 기다리는 송금 완료 이벤트(Outbox PENDING) 수")
                .register(meterRegistry);
        Gauge.builder(OLDEST_PENDING_AGE, this, TransferOutboxBacklogMonitor::oldestPendingAgeSeconds)
                .description("가장 오래된 미발행 송금 완료 이벤트가 생긴 뒤 지난 시간(없으면 0)")
                .baseUnit("seconds")
                .register(meterRegistry);
    }

    /** 조회가 실패하면 이전 값을 그대로 두고 다음 주기에 다시 조회합니다. */
    @Scheduled(initialDelay = 10_000, fixedDelay = REFRESH_INTERVAL_MS)
    public void refresh() {
        try {
            long count = outboxRepository.countPending();
            Instant oldest = count == 0 ? null : outboxRepository.findOldestPendingCreatedAt();
            pending.set(count);
            oldestPendingCreatedAt.set(oldest);
        } catch (RuntimeException exception) {
            log.warn("미발행 송금 완료 이벤트 수를 조회하지 못했습니다. exception={}", exception.getClass().getName());
        }
    }

    double oldestPendingAgeSeconds() {
        Instant oldest = oldestPendingCreatedAt.get();
        if (oldest == null) {
            return 0;
        }
        return Math.max(0, Duration.between(oldest, clock.instant()).toMillis() / 1000.0);
    }
}
