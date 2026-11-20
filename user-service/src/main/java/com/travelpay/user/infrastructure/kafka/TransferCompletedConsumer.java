package com.travelpay.user.infrastructure.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.travelpay.user.application.dto.TransferCompletedEvent;
import com.travelpay.user.application.service.TransferNotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * payment.transfer.completed 수신 → eventId로 중복을 거르고 Slack에 알립니다({@link TransferNotificationService}).
 * 이벤트 계약(토픽·헤더·값)은 docs/guide/payment-transfer.md의 「송금 완료 이벤트 (Kafka)」를 따릅니다.
 */
@Component
public class TransferCompletedConsumer {

    static final String GROUP_ID = "user-transfer-notification";
    public static final String TOPIC = "payment.transfer.completed";
    /** payment-service KafkaTransferEventSender.EVENT_ID_HEADER와 같은 값 */
    static final String EVENT_ID_HEADER = "eventId";

    private final TransferNotificationService service;
    private final JsonMapper jsonMapper;

    public TransferCompletedConsumer(TransferNotificationService service, JsonMapper jsonMapper) {
        this.service = service;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(id = GROUP_ID, groupId = GROUP_ID, topics = TOPIC,
            containerFactory = TransferNotificationKafkaConfig.CONTAINER_FACTORY)
    public void onTransferCompleted(ConsumerRecord<String, String> record) {
        service.handle(event(record));
    }

    private TransferCompletedEvent event(ConsumerRecord<String, String> record) {
        UUID eventId = eventId(record);
        Payload payload;
        try {
            payload = record.value() == null ? null : jsonMapper.readValue(record.value(), Payload.class);
        } catch (JacksonException exception) {
            throw new InvalidTransferEventException("값 JSON 형식 오류", exception);
        }
        if (payload == null) {
            throw new InvalidTransferEventException("값 없음", null);
        }
        if (payload.amount() == null) {
            throw new InvalidTransferEventException("amount 없음", null);
        }
        try {
            return new TransferCompletedEvent(eventId, payload.transferId(), payload.senderId(), payload.receiverId(),
                    payload.amount(), payload.currency(), payload.completedAt());
        } catch (IllegalArgumentException exception) {
            throw new InvalidTransferEventException(exception.getMessage(), exception);
        }
    }

    private static UUID eventId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(EVENT_ID_HEADER);
        if (header == null || header.value() == null) {
            throw new InvalidTransferEventException("eventId 헤더 없음", null);
        }
        try {
            return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException exception) {
            throw new InvalidTransferEventException("eventId 헤더가 UUID가 아님", exception);
        }
    }

    /** 값 JSON에서 알림에 쓰는 필드만 읽습니다 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record Payload(UUID transferId, UUID senderId, UUID receiverId, Long amount, String currency,
            Instant completedAt) {
    }
}
