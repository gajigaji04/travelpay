package com.travelpay.user.application.service;

import java.time.Clock;
import java.util.UUID;

import com.travelpay.user.application.dto.TransferCompletedEvent;
import com.travelpay.user.application.service.TransferNotificationMessage.Party;
import com.travelpay.user.domain.model.entity.User;
import com.travelpay.user.domain.repository.TransferNotificationRepository;
import com.travelpay.user.domain.repository.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 송금 완료 이벤트를 기록하고 Slack에 한 번 알립니다.
 * <ol>
 *   <li>수신 기록 트랜잭션: 처음 받은 eventId면 알리지 않은 상태로 저장(이미 있으면 그대로)</li>
 *   <li>알림 트랜잭션: 그 행을 잠그고, 이미 알렸으면 끝. 아니면 보낸 분·받는 분 이름을 조회해 보내고
 *   성공했을 때만 알림 시각을 남김</li>
 * </ol>
 * 행 잠금 안에서 보내므로 같은 이벤트가 동시에 와도 한 번만 보냅니다. 전송이 실패하면 알림 트랜잭션만 롤백되어
 * 수신 기록은 알리지 않은 상태로 남고, 예외는 소비자의 재시도로 넘어갑니다.
 * 전송 성공 뒤 커밋이 실패하면 재전달 때 한 번 더 보낼 수 있습니다(at-least-once).
 */
@Slf4j
@Service
public class TransferNotificationService {

    private final TransferNotificationRepository repository;
    private final TransferNotificationSender sender;
    private final UserRepository userRepository;
    private final TransactionTemplate transactions;
    private final Clock clock;

    public TransferNotificationService(TransferNotificationRepository repository, TransferNotificationSender sender,
            UserRepository userRepository, PlatformTransactionManager transactionManager, Clock clock) {
        this.repository = repository;
        this.sender = sender;
        this.userRepository = userRepository;
        this.transactions = new TransactionTemplate(transactionManager);
        this.clock = clock;
    }

    public Outcome handle(TransferCompletedEvent event) {
        transactions.executeWithoutResult(status -> repository.recordIfAbsent(event.eventId(), event.transferId(),
                event.amount(), event.currency(), event.completedAt(), clock.instant()));

        return transactions.execute(status -> {
            var notification = repository.findByEventIdForUpdate(event.eventId())
                    .orElseThrow(() -> new IllegalStateException("기록한 송금 완료 이벤트가 없습니다. eventId=" + event.eventId()));
            if (notification.isNotified()) {
                log.info("이미 알린 송금 완료 이벤트라 건너뜀 eventId={} transferId={}", event.eventId(), event.transferId());
                return Outcome.ALREADY_NOTIFIED;
            }
            if (!sender.isConfigured()) {
                log.info("Slack Webhook이 설정되지 않아 수신 기록만 남김 eventId={} transferId={}",
                        event.eventId(), event.transferId());
                return Outcome.RECORDED_ONLY;
            }
            sender.send(TransferNotificationMessage.of(notification,
                    party(event.senderId()), party(event.receiverId())));
            notification.markNotified(clock.instant());
            log.info("송금 완료 알림 보냄 eventId={} transferId={}", event.eventId(), event.transferId());
            return Outcome.NOTIFIED;
        });
    }

    /** 회원이 없어도 알림은 보내도록 이름만 비워 둡니다. */
    private Party party(UUID userId) {
        return new Party(userId, userRepository.findById(userId).map(User::getRealName).orElse(null));
    }

    public enum Outcome {
        /** 이번에 Slack으로 보냄 */
        NOTIFIED,
        /** 같은 이벤트를 이미 보냄(중복 전달) */
        ALREADY_NOTIFIED,
        /** Webhook 미설정이라 기록만 함 */
        RECORDED_ONLY
    }
}
