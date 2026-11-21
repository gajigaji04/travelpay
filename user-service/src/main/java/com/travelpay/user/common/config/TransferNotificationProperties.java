package com.travelpay.user.common.config;

import java.net.URI;
import java.net.URISyntaxException;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 송금 완료 이벤트 소비자와 Slack 알림 설정입니다.
 *
 * @param consumerEnabled   payment.transfer.completed 소비자를 시작할지. 끄면 이벤트는 토픽에 남고 켤 때 이어서 읽습니다
 * @param concurrency       소비자 스레드 수(1 이상, 토픽 파티션 3개를 넘으면 남는 스레드는 쉼)
 * @param slackWebhookUrl   Slack Incoming Webhook 주소. 비밀값이라 환경 변수(SLACK_WEBHOOK_URL)로만 받고 로그에 남기지 않습니다.
 *                          비어 있으면 수신 기록만 하고 보내지 않습니다
 */
@ConfigurationProperties("travelpay.user.transfer-notification")
public record TransferNotificationProperties(boolean consumerEnabled, int concurrency, String slackWebhookUrl) {

    public TransferNotificationProperties {
        if (concurrency < 1) {
            throw new IllegalArgumentException(
                    "travelpay.user.transfer-notification.concurrency는 1 이상이어야 합니다. value=" + concurrency);
        }
        slackWebhookUrl = slackWebhookUrl == null ? "" : slackWebhookUrl.strip();
        if (!slackWebhookUrl.isEmpty() && !isHttpUrl(slackWebhookUrl)) {
            // 주소 자체가 비밀값이라 오류 메시지에 넣지 않습니다.
            throw new IllegalArgumentException(
                    "travelpay.user.transfer-notification.slack-webhook-url은 http(s) 주소여야 합니다.");
        }
    }

    public boolean slackConfigured() {
        return !slackWebhookUrl.isEmpty();
    }

    /** 로그·예외 메시지에 주소가 남지 않도록 출력에서 가립니다. */
    @Override
    public String toString() {
        return "TransferNotificationProperties[consumerEnabled=" + consumerEnabled + ", concurrency=" + concurrency
                + ", slackWebhookUrl=" + (slackConfigured() ? "****" : "") + "]";
    }

    private static boolean isHttpUrl(String value) {
        try {
            var uri = new URI(value);
            return ("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                    && uri.getHost() != null;
        } catch (URISyntaxException exception) {
            return false;
        }
    }
}
