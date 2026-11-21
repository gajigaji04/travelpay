package com.travelpay.user.presentation.dto;

import java.util.UUID;

import com.travelpay.user.application.dto.UserDisplayNameResult;

/** 내부 일괄 이름 조회 응답 항목입니다. 마스킹한 이름만 담습니다(예: 홍*동). */
public record UserDisplayNameResponse(UUID userId, String maskedName) {

    public static UserDisplayNameResponse from(UserDisplayNameResult result) {
        return new UserDisplayNameResponse(result.userId(), result.maskedName());
    }
}
