package com.travelpay.user.infrastructure.slack;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;

import com.travelpay.user.application.service.TransferNotificationSendException;
import com.travelpay.user.application.service.TransferNotificationSender;
import com.travelpay.user.common.config.TransferNotificationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Slack Incoming Webhook으로 {@code {"text": "..."}}를 POST합니다.
 * 연결 2초·응답 3초에서 포기합니다. 알림 트랜잭션이 송금 완료 행을 잠근 채 기다리므로 길게 두지 않습니다.
 * Webhook 주소가 비밀값이라 실패해도 주소가 담길 수 있는 원인 예외 메시지는 남기지 않습니다.
 */
@Component
@EnableConfigurationProperties(TransferNotificationProperties.class)
public class SlackTransferNotificationSender implements TransferNotificationSender {

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    static final Duration READ_TIMEOUT = Duration.ofSeconds(3);

    private final TransferNotificationProperties properties;
    private final JsonMapper jsonMapper;
    private final RestClient restClient;

    public SlackTransferNotificationSender(TransferNotificationProperties properties, JsonMapper jsonMapper) {
        this.properties = properties;
        this.jsonMapper = jsonMapper;
        var requestFactory = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
        requestFactory.setReadTimeout(READ_TIMEOUT);
        this.restClient = RestClient.builder().requestFactory(requestFactory).build();
    }

    @Override
    public boolean isConfigured() {
        return properties.slackConfigured();
    }

    @Override
    public void send(String text) {
        if (!isConfigured()) {
            throw new IllegalStateException("Slack Webhook이 설정되지 않았습니다.");
        }
        var body = jsonMapper.writeValueAsString(Map.of("text", text));
        try {
            restClient.post()
                    .uri(URI.create(properties.slackWebhookUrl()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientResponseException exception) {
            throw new TransferNotificationSendException("HTTP " + exception.getStatusCode().value());
        } catch (RestClientException exception) {
            throw new TransferNotificationSendException(exception.getClass().getSimpleName());
        }
    }
}
