package com.travelpay.payment.infra.client.user;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.travelpay.common.response.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.UUID;

@FeignClient(name = "user-service", contextId = "paymentUserApi", url = "${travelpay.client.user.url:}",
        path = "/internal/v1/users")
public interface UserFeignApi {

    @GetMapping("/{userId}/real-name")
    ApiResponse<UserResponse> getActiveUser(@PathVariable("userId") UUID userId);

    /** 송금 내역 상대방 이름 일괄 조회(1~100개). user-service가 마스킹한 이름만 반환하며, 없는 회원은 응답에서 빠집니다. */
    @GetMapping("/display-names")
    ApiResponse<List<DisplayNameResponse>> getDisplayNames(@RequestParam("ids") List<UUID> ids);

    @JsonIgnoreProperties(ignoreUnknown = true)
    record DisplayNameResponse(UUID userId, String maskedName) {
    }

    /**
     * realName은 마스킹 전 가입 성명입니다. 결제(가맹점 소유 회원 확인)는 userId만, 송금(수취인 확인)은 realName도 씁니다.
     * 로그 등에 성명이 남지 않도록 문자열 표현에는 값을 넣지 않습니다.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record UserResponse(UUID userId, String realName) {

        @Override
        public String toString() {
            return "UserResponse[REDACTED]";
        }
    }
}
