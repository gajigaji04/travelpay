package com.travelpay.payment.transfer.client;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.travelpay.payment.transfer.application.TransferEventSender;
import com.travelpay.payment.transfer.config.TransferKafkaTopicConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 송금 완료 이벤트를 Kafka 토픽 payment.transfer.completed로 보냅니다.
 * 키는 transferId, 헤더 eventId는 Outbox 행 ID, 값은 Outbox에 저장한 JSON 그대로입니다.
 */
@Component
public class KafkaTransferEventSender implements TransferEventSender {

    public static final String EVENT_ID_HEADER = "eventId";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaTransferEventSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public CompletableFuture<Void> sendCompleted(UUID transferId, UUID eventId, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                TransferKafkaTopicConfig.TRANSFER_COMPLETED, transferId.toString(), payload);
        record.headers().add(EVENT_ID_HEADER, eventId.toString().getBytes(StandardCharsets.UTF_8));
        return kafkaTemplate.send(record).thenApply(result -> null);
    }
}
