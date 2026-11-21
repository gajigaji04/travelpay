package com.travelpay.user.application.dto;

import java.util.UUID;

/** 다른 서비스 화면에 보여 줄 회원 이름입니다. 마스킹한 값만 담습니다. */
public record UserDisplayNameResult(UUID userId, String maskedName) {
}
