package com.travelpay.user.presentation.controller;

import java.util.Map;
import java.util.UUID;

import com.travelpay.common.auth.AuthRole;
import com.travelpay.common.auth.AuthenticatedUser;
import com.travelpay.common.auth.web.CurrentUser;
import com.travelpay.common.error.BusinessException;
import com.travelpay.common.error.CommonErrorCode;
import com.travelpay.common.response.ApiResponse;
import com.travelpay.common.response.PageResponse;
import com.travelpay.user.application.service.UserNotificationService;
import com.travelpay.user.presentation.dto.UserNotificationResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 로그인한 회원 본인의 화면 알림 API입니다. 지금은 송금 완료 알림만 있습니다.
 * 프런트는 읽지 않은 개수를 주기적으로 조회하고(권장 30초, 탭이 보이지 않을 때는 멈춤), 목록은 사용자가 열 때 조회합니다.
 * 회원(USER)만 쓰며 MASTER는 403입니다(송금 API와 같은 방식). 읽음 표시(PATCH)는 다른 상태 변경 API와 같이 CSRF 토큰이 필요합니다.
 */
@Tag(name = "알림")
@RestController
@RequiredArgsConstructor
public class UserNotificationController {

    private final UserNotificationService notificationService;

    @Operation(summary = "내 알림 목록", description = "송금 완료 시각 최신순입니다. page는 0부터, size는 기본 20·최대 100입니다.")
    @GetMapping("/api/v1/users/me/notifications")
    public ApiResponse<PageResponse<UserNotificationResponse>> list(@CurrentUser AuthenticatedUser user,
            @Parameter(description = "페이지 번호(0부터)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "페이지 크기(기본 20, 100을 넘으면 100으로 조회)") @RequestParam(defaultValue = "20") int size) {
        return ApiResponse.success(PageResponse.from(
                notificationService.list(requireMember(user), page, size), UserNotificationResponse::from));
    }

    @Operation(summary = "읽지 않은 알림 개수", description = "프런트가 주기적으로 조회하는 API입니다.")
    @GetMapping("/api/v1/users/me/notifications/unread-count")
    public ApiResponse<Map<String, Long>> unreadCount(@CurrentUser AuthenticatedUser user) {
        return ApiResponse.success(Map.of("count", notificationService.countUnread(requireMember(user))));
    }

    @Operation(summary = "알림 읽음 표시", description = "본인 알림만 바꿉니다. 이미 읽은 알림도 200입니다. 없거나 다른 회원의 알림은 404입니다.")
    @PatchMapping("/api/v1/users/me/notifications/{notificationId}/read")
    public ApiResponse<Void> markRead(@CurrentUser AuthenticatedUser user, @PathVariable UUID notificationId) {
        notificationService.markRead(requireMember(user), notificationId);
        return ApiResponse.success(null);
    }

    @Operation(summary = "알림 모두 읽음", description = "읽지 않은 본인 알림을 모두 읽음으로 바꾸고 바꾼 개수를 반환합니다.")
    @PatchMapping("/api/v1/users/me/notifications/read-all")
    public ApiResponse<Map<String, Integer>> markAllRead(@CurrentUser AuthenticatedUser user) {
        return ApiResponse.success(Map.of("updated", notificationService.markAllRead(requireMember(user))));
    }

    private static UUID requireMember(AuthenticatedUser user) {
        if (user.role() == AuthRole.MASTER) {
            throw new BusinessException(CommonErrorCode.FORBIDDEN);
        }
        return user.userId();
    }
}
