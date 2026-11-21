package com.travelpay.user.application.service;

/**
 * 송금 완료 알림 전송 실패입니다. Webhook 주소가 비밀값이라 원인 예외(주소가 메시지에 들어 있을 수 있음)를 붙이지 않고
 * 원인의 종류(HTTP 상태나 예외 클래스 이름)만 메시지에 남깁니다. 일시적일 수 있어 소비자가 재시도합니다.
 */
public class TransferNotificationSendException extends RuntimeException {

    public TransferNotificationSendException(String reason) {
        super("송금 완료 알림을 보내지 못했습니다. reason=" + reason, null, false, false);
    }
}
