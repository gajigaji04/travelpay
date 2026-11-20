package com.travelpay.payment.transfer.application;

import java.time.Instant;

import com.travelpay.payment.transfer.domain.Transfer;
import com.travelpay.payment.transfer.domain.TransferOutbox;
import com.travelpay.payment.transfer.domain.TransferOutboxRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * 송금 완료 이벤트를 Outbox(transfer_outbox)에 기록합니다. Kafka로 보내는 것은 발행기({@link TransferOutboxPublisher})가 합니다.
 * 반드시 송금을 COMPLETED로 바꾼 트랜잭션 안에서, 조건부 UPDATE가 실제로 1행을 바꿨을 때만 호출합니다.
 * 그래야 상태 변경과 이벤트 기록이 함께 커밋되거나 함께 롤백되고, 송금 하나에 이벤트가 한 건만 남습니다.
 * 트랜잭션 밖에서 부르면 예외입니다(MANDATORY).
 */
@Component
public class TransferCompletedOutboxWriter {

    private final TransferOutboxRepository outboxRepository;
    private final JsonMapper jsonMapper;

    public TransferCompletedOutboxWriter(TransferOutboxRepository outboxRepository, JsonMapper jsonMapper) {
        this.outboxRepository = outboxRepository;
        this.jsonMapper = jsonMapper;
    }

    /**
     * @param transfer    완료된 송금. 이벤트에는 송금 생성 뒤 바뀌지 않는 값(당사자·금액·통화)만 넣으므로 상태 변경 전에 읽은 엔티티여도 됩니다.
     * @param completedAt COMPLETED로 바꾼 조건부 UPDATE에 기록한 시각
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void record(Transfer transfer, Instant completedAt) {
        String payload = jsonMapper.writeValueAsString(TransferCompletedPayload.of(transfer, completedAt));
        outboxRepository.save(TransferOutbox.pending(transfer.getId(), payload, completedAt));
    }
}
