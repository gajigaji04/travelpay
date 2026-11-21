package com.travelpay.user.infrastructure.kafka;

/** 헤더 eventId나 값 JSON을 읽을 수 없는 송금 완료 이벤트입니다. 다시 처리해도 같으므로 재시도하지 않습니다. */
public class InvalidTransferEventException extends RuntimeException {

    public InvalidTransferEventException(String reason, Throwable cause) {
        super("송금 완료 이벤트를 읽을 수 없습니다. reason=" + reason, cause);
    }
}
