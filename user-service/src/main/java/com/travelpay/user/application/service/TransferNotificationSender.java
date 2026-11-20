package com.travelpay.user.application.service;

/** 송금 완료 알림 문구를 외부 채널(Slack)로 보냅니다. */
public interface TransferNotificationSender {

    /** 보낼 곳이 설정되어 있는지. false면 수신 기록만 하고 보내지 않습니다. */
    boolean isConfigured();

    /**
     * 문구를 보냅니다. 실패하면 {@link TransferNotificationSendException}을 던지며,
     * 예외 메시지에는 보낼 곳의 주소를 넣지 않습니다.
     */
    void send(String text);
}
