package com.travelpay.payment.transfer.domain;

/** 송금 완료 이벤트(Outbox)의 발행 상태입니다. */
public enum TransferOutboxStatus {
    /** 아직 발행하지 않았거나 발행에 실패해 다시 보낼 건입니다. */
    PENDING,
    /** Kafka가 받았다고 응답한 건입니다. */
    PUBLISHED
}
