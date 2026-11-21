package com.travelpay.user.domain.model.vo;

/** 회원 화면 알림의 종류입니다. */
public enum UserNotificationType {
    /** 내가 보낸 송금이 완료됨 */
    TRANSFER_SENT,
    /** 나에게 온 송금이 완료됨 */
    TRANSFER_RECEIVED
}
