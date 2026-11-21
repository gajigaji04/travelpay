package com.travelpay.user.infrastructure.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.travelpay.user.application.service.TransferNotificationSendException;
import com.travelpay.user.common.config.TransferNotificationProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 송금 완료 알림 소비자 컨테이너입니다(wallet-service ChargeSagaKafkaConfig와 같은 구조). 레코드 단위로 커밋합니다.
 * <ul>
 *   <li>Slack·DB 오류는 1초 간격으로 2번 다시 처리하고, 그래도 실패하면 DLT({@value #DLT_TOPIC})로 보내고 다음 메시지로 넘어갑니다.
 *   그 이벤트의 수신 기록은 알리지 않은 상태(notified_at null)로 남습니다.</li>
 *   <li>형식이 잘못된 이벤트(InvalidTransferEventException)는 다시 처리해도 같으므로 재시도 없이 바로 DLT로 보냅니다.</li>
 *   <li>DLT 메시지는 원본의 키·값·헤더(eventId 포함)를 그대로 담고 같은 파티션 번호로 갑니다. 원본 위치(토픽·파티션·오프셋)·
 *   예외 클래스 헤더는 Spring Kafka가 붙이고, 예외 메시지·스택은 내부 정보가 담길 수 있어 빼며 대신
 *   {@value #FAILURE_KIND_HEADER}(invalid·exhausted)와 {@value #FAILURE_REASON_HEADER}(이 서비스 예외의 메시지만)를 붙입니다.</li>
 *   <li>DLT로 보내는 데 실패하면(Kafka 장애) 그 메시지를 처음부터 다시 처리합니다. 넘어가지 않으므로 잃지 않습니다.</li>
 *   <li>DLT는 자동으로 다시 처리하지 않습니다. 보낸 건수를 {@value #DEAD_LETTER_METRIC} 지표로 세고,
 *   확인·재투입 절차는 docs/guide/user-service.md 「송금 완료 알림 DLT」를 따릅니다.</li>
 * </ul>
 * travelpay.user.transfer-notification.consumer-enabled=false면 컨테이너를 시작하지 않고 DLT 토픽도 선언하지 않습니다(이벤트는 토픽에 남음).
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(TransferNotificationProperties.class)
public class TransferNotificationKafkaConfig {

    public static final String CONTAINER_FACTORY = "transferNotificationListenerContainerFactory";
    public static final String DLT_TOPIC = TransferCompletedConsumer.TOPIC + ".DLT";
    public static final String FAILURE_KIND_HEADER = "failureKind";
    public static final String FAILURE_REASON_HEADER = "failureReason";
    public static final String DEAD_LETTER_METRIC = "transfer.notification.dead.letter";

    static final long RETRY_INTERVAL_MS = 1000L;
    static final long RETRY_ATTEMPTS = 2L;
    /** payment-service TransferKafkaTopicConfig의 파티션 수. DLT도 같은 파티션 번호로 보내므로 같은 수로 만듭니다 */
    static final int TOPIC_PARTITIONS = 3;
    /** 운영자가 확인·재투입할 여유를 두도록 원 토픽(Kafka 기본 7일)보다 길게 둡니다 */
    static final Duration DLT_RETENTION = Duration.ofDays(30);

    @Bean(CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> transferNotificationListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory, TransferNotificationProperties properties,
            KafkaOperations<?, ?> kafkaOperations, MeterRegistry meterRegistry) {
        if (properties.concurrency() > TOPIC_PARTITIONS) {
            log.warn("송금 완료 알림 소비자 수가 토픽 파티션 수보다 많음 concurrency={} partitions={}",
                    properties.concurrency(), TOPIC_PARTITIONS);
        }
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.concurrency());
        factory.setAutoStartup(properties.consumerEnabled());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(errorHandler(deadLetterRecoverer(kafkaOperations, meterRegistry)));
        return factory;
    }

    /** 브로커 자동 생성이 꺼져 있어 소비자 쪽에서 DLT를 선언합니다(원 토픽은 발행하는 payment-service가 선언). */
    @Bean
    @ConditionalOnProperty(prefix = "travelpay.user.transfer-notification", name = "consumer-enabled",
            havingValue = "true")
    public NewTopic transferCompletedDeadLetterTopic() {
        return TopicBuilder.name(DLT_TOPIC)
                .partitions(TOPIC_PARTITIONS)
                .replicas(1)    // 로컬 브로커 1대. 운영 값은 원 토픽과 함께 인프라와 정함
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(DLT_RETENTION.toMillis()))
                .build();
    }

    static DefaultErrorHandler errorHandler(ConsumerAwareRecordRecoverer recoverer) {
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, RETRY_ATTEMPTS));
        handler.addNotRetryableExceptions(InvalidTransferEventException.class);
        return handler;
    }

    static ConsumerAwareRecordRecoverer deadLetterRecoverer(KafkaOperations<?, ?> kafkaOperations,
            MeterRegistry meterRegistry) {
        // Spring Kafka 4의 기본 대상은 "<토픽>-dlt"라서, 선언한 토픽과 원본과 같은 파티션 번호를 직접 정합니다.
        var publisher = new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, exception) -> new TopicPartition(DLT_TOPIC, record.partition()));
        publisher.excludeHeader(HeadersToAdd.EX_MSG, HeadersToAdd.EX_STACKTRACE);
        publisher.setHeadersFunction((record, exception) -> new RecordHeaders(new RecordHeader[] {
                new RecordHeader(FAILURE_KIND_HEADER, kind(exception).getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(FAILURE_REASON_HEADER, safeMessage(exception).getBytes(StandardCharsets.UTF_8))}));
        var invalid = deadLetterCounter(meterRegistry, FailureKind.INVALID);
        var exhausted = deadLetterCounter(meterRegistry, FailureKind.EXHAUSTED);
        return (record, consumer, exception) -> {
            // 예외 메시지 대신 클래스 이름과 위치만 남깁니다(메시지 내용은 이 서비스의 예외만 신뢰).
            log.error("송금 완료 알림 처리 포기, DLT로 보냄 topic={} partition={} offset={} transferId={} kind={} "
                            + "exception={} message={}", record.topic(), record.partition(), record.offset(),
                    record.key(), kind(exception),
                    NestedExceptionUtils.getMostSpecificCause(exception).getClass().getName(), safeMessage(exception));
            publisher.accept(record, consumer, exception);
            (isInvalid(exception) ? invalid : exhausted).increment();
        };
    }

    private static Counter deadLetterCounter(MeterRegistry meterRegistry, FailureKind kind) {
        return Counter.builder(DEAD_LETTER_METRIC)
                .description("처리를 포기하고 DLT로 보낸 송금 완료 이벤트 수")
                .tag("kind", kind.value)
                .register(meterRegistry);
    }

    private static String kind(Exception exception) {
        return (isInvalid(exception) ? FailureKind.INVALID : FailureKind.EXHAUSTED).value;
    }

    private static boolean isInvalid(Throwable exception) {
        for (Throwable current = exception; current != null; current = current.getCause()) {
            if (current instanceof InvalidTransferEventException) {
                return true;
            }
        }
        return false;
    }

    private static String safeMessage(Exception exception) {
        for (Throwable current = exception; current != null; current = current.getCause()) {
            if (current instanceof InvalidTransferEventException
                    || current instanceof TransferNotificationSendException) {
                return current.getMessage() == null ? "" : current.getMessage();
            }
        }
        return "";
    }

    enum FailureKind {
        /** 형식 오류(재시도 없음) */
        INVALID("invalid"),
        /** Slack·DB 오류 재시도 소진 */
        EXHAUSTED("exhausted");

        final String value;

        FailureKind(String value) {
            this.value = value;
        }
    }
}
