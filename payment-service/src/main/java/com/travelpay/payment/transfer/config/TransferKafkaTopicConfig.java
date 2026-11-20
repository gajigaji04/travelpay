package com.travelpay.payment.transfer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 송금이 발행하는 토픽입니다. 브로커 자동 생성이 꺼져 있어 여기서 선언합니다(지갑 토픽과 같은 방식).
 * 발행기를 끄면(transfer.outbox.publisher.enabled=false) 토픽도 선언하지 않아 기동 때 브로커에 붙지 않습니다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "transfer.outbox.publisher", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class TransferKafkaTopicConfig {

    public static final String TRANSFER_COMPLETED = "payment.transfer.completed";
    // 파티션 수는 팀 리뷰에서 정합니다. 우선 지갑 충전 토픽과 같은 값입니다.
    public static final int TRANSFER_COMPLETED_PARTITIONS = 3;

    @Bean
    public NewTopic transferCompletedTopic() {
        return TopicBuilder.name(TRANSFER_COMPLETED)
                .partitions(TRANSFER_COMPLETED_PARTITIONS)
                .replicas(1)    // 로컬 브로커 1대
                .build();
    }
}
