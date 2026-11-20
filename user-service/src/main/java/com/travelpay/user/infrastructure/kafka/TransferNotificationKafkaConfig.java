package com.travelpay.user.infrastructure.kafka;

import com.travelpay.user.application.service.TransferNotificationSendException;
import com.travelpay.user.common.config.TransferNotificationProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 송금 완료 알림 소비자 컨테이너입니다(wallet-service ChargeSagaKafkaConfig와 같은 구조). 레코드 단위로 커밋합니다.
 * Slack·DB 오류는 1초 간격으로 2번 다시 처리하고, 그래도 실패하면 ERROR를 남기고 다음 메시지로 넘어갑니다.
 * 그 이벤트의 수신 기록은 알리지 않은 상태(notified_at null)로 남습니다.
 * 형식이 잘못된 이벤트(InvalidTransferEventException)는 다시 처리해도 같으므로 바로 넘깁니다.
 * travelpay.user.transfer-notification.consumer-enabled=false면 컨테이너를 시작하지 않습니다(이벤트는 토픽에 남음).
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(TransferNotificationProperties.class)
public class TransferNotificationKafkaConfig {

    public static final String CONTAINER_FACTORY = "transferNotificationListenerContainerFactory";

    static final long RETRY_INTERVAL_MS = 1000L;
    static final long RETRY_ATTEMPTS = 2L;
    /** payment-service TransferKafkaTopicConfig의 파티션 수 */
    static final int TOPIC_PARTITIONS = 3;

    @Bean(CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> transferNotificationListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory, TransferNotificationProperties properties) {
        if (properties.concurrency() > TOPIC_PARTITIONS) {
            log.warn("송금 완료 알림 소비자 수가 토픽 파티션 수보다 많음 concurrency={} partitions={}",
                    properties.concurrency(), TOPIC_PARTITIONS);
        }
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.concurrency());
        factory.setAutoStartup(properties.consumerEnabled());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    private static DefaultErrorHandler errorHandler() {
        // 예외 메시지 대신 클래스 이름과 위치만 남깁니다(메시지 내용은 이 서비스의 예외만 신뢰).
        var handler = new DefaultErrorHandler(
                (record, exception) -> log.error("송금 완료 알림 처리 포기 topic={} partition={} offset={} transferId={} "
                                + "exception={} message={}", record.topic(), record.partition(), record.offset(),
                        record.key(), NestedExceptionUtils.getMostSpecificCause(exception).getClass().getName(),
                        safeMessage(exception)),
                new FixedBackOff(RETRY_INTERVAL_MS, RETRY_ATTEMPTS));
        handler.addNotRetryableExceptions(InvalidTransferEventException.class);
        return handler;
    }

    private static String safeMessage(Exception exception) {
        for (Throwable current = exception; current != null; current = current.getCause()) {
            if (current instanceof InvalidTransferEventException
                    || current instanceof TransferNotificationSendException) {
                return current.getMessage();
            }
        }
        return "";
    }
}
