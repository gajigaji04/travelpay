package com.travelpay.payment.transfer.application;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.travelpay.payment.transfer.domain.TransferOutbox;
import com.travelpay.payment.transfer.domain.TransferOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 미발행 송금 완료 이벤트(Outbox)를 잠근 채 Kafka로 보내고, 응답에 따라 같은 트랜잭션에서 상태를 바꿉니다.
 * 지갑 충전 Outbox 발행기(wallet-service OutboxPublisher)와 같은 구조·시간 값입니다.
 * <ul>
 *     <li>발행에 실패한 건은 PENDING으로 남기고 실패 횟수만 올려 다음 주기에 다시 보냅니다. 횟수 한도는 없습니다
 *         (알림 이벤트라 늦게라도 보내며, 자금·송금 상태에는 영향이 없음).</li>
 *     <li>상태 변경이 실패해 롤백되면 이미 보낸 건도 PENDING으로 남아 같은 eventId로 다시 보냅니다(at-least-once).
 *         소비자가 eventId로 중복을 걸러야 합니다.</li>
 *     <li>잠금(SKIP LOCKED)은 트랜잭션이 끝날 때까지 유지되어 서버가 여러 대여도 같은 건을 함께 보내지 않습니다.</li>
 * </ul>
 * 송금 요청·복구·확정 경로는 Outbox 행만 저장하고 Kafka를 부르지 않으므로, Kafka가 멈춰도 송금 처리는 그대로 됩니다.
 */
@Service
public class TransferOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(TransferOutboxPublisher.class);

    static final int TX_TIMEOUT_SECONDS = 10;
    // Kafka가 꺼져 있으면 send() 하나가 max.block(3초)까지 멈추므로, 이 시간이 지나면 남은 건은 다음 주기로 미룹니다.
    private static final Duration SEND_BUDGET = Duration.ofSeconds(3);
    // delivery.timeout(5초) 응답을 기다리되, 트랜잭션(10초) 안에 상태 변경까지 끝나도록 시작 후 8초까지만 기다립니다.
    private static final Duration RESPONSE_DEADLINE = Duration.ofSeconds(8);

    private final TransferOutboxRepository outboxRepository;
    private final TransferEventSender eventSender;

    public TransferOutboxPublisher(TransferOutboxRepository outboxRepository, TransferEventSender eventSender) {
        this.outboxRepository = outboxRepository;
        this.eventSender = eventSender;
    }

    /** 미발행 건을 최대 batchSize건 보내고, 발행에 성공한 건수를 반환합니다. */
    @Transactional(timeout = TX_TIMEOUT_SECONDS)
    public int publishPending(int batchSize) {
        Instant startedAt = Instant.now();
        List<TransferOutbox> events = outboxRepository.findPendingForUpdate(batchSize);
        if (events.isEmpty()) {
            return 0;
        }

        Instant sendDeadline = startedAt.plus(SEND_BUDGET);
        List<CompletableFuture<Void>> futures = new ArrayList<>(events.size());
        for (TransferOutbox event : events) {
            futures.add(send(event, sendDeadline));
        }

        Instant responseDeadline = startedAt.plus(RESPONSE_DEADLINE);
        int published = 0;
        for (int i = 0; i < events.size(); i++) {
            TransferOutbox event = events.get(i);
            if (await(event, futures.get(i), responseDeadline)) {
                event.markPublished(Instant.now().truncatedTo(ChronoUnit.MICROS));
                published++;
            } else {
                event.increaseRetryCount();
            }
        }
        return published;
    }

    private CompletableFuture<Void> send(TransferOutbox event, Instant sendDeadline) {
        if (Instant.now().isAfter(sendDeadline)) {
            return CompletableFuture.failedFuture(new TimeoutException("보내기 시간을 넘겨 다음 주기로 미룹니다."));
        }
        try {
            return eventSender.sendCompleted(event.getTransferId(), event.getId(), event.getPayload());
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }

    private boolean await(TransferOutbox event, CompletableFuture<Void> future, Instant deadline) {
        try {
            long remainingMillis = Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
            future.get(remainingMillis, TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            logFailure(event, exception);
        } catch (ExecutionException exception) {
            logFailure(event, NestedExceptionUtils.getMostSpecificCause(exception));
        } catch (TimeoutException exception) {
            logFailure(event, exception);
        }
        return false;
    }

    // Kafka 장애 중에는 주기마다 건별로 남으므로 스택 없이 한 줄만 남깁니다.
    private void logFailure(TransferOutbox event, Throwable cause) {
        log.warn("송금 완료 이벤트 발행 실패 eventId={} transferId={} retryCount={} exception={}",
                event.getId(), event.getTransferId(), event.getRetryCount(), cause.getClass().getName());
    }
}
