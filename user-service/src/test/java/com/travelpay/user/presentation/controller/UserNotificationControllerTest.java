package com.travelpay.user.presentation.controller;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.common.auth.AuthRole;
import com.travelpay.common.auth.AuthenticatedUser;
import com.travelpay.common.auth.CurrentUserProvider;
import com.travelpay.common.auth.web.CurrentUserArgumentResolver;
import com.travelpay.common.error.BusinessException;
import com.travelpay.common.error.GlobalExceptionHandler;
import com.travelpay.user.application.dto.UserNotificationResult;
import com.travelpay.user.application.service.UserNotificationService;
import com.travelpay.user.common.error.UserErrorCode;
import com.travelpay.user.domain.model.vo.UserNotificationType;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 응답 형식과 MASTER 거절만 봅니다. 로그인·CSRF는 /api/v1/users/** 의 기존 보안 규칙을 그대로 따릅니다. */
class UserNotificationControllerTest {

    private static final AuthenticatedUser MEMBER = new AuthenticatedUser(
            UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), AuthRole.USER);
    private static final AuthenticatedUser MASTER = new AuthenticatedUser(UUID.randomUUID(), AuthRole.MASTER);

    private final UserNotificationService service = mock(UserNotificationService.class);

    @Test
    void listsNotificationsWithCommonPageResponse() throws Exception {
        var notificationId = UUID.randomUUID();
        var transferId = UUID.randomUUID();
        var counterpartId = UUID.randomUUID();
        when(service.list(MEMBER.userId(), 0, 20)).thenReturn(new PageImpl<>(List.of(new UserNotificationResult(
                notificationId, UserNotificationType.TRANSFER_RECEIVED, transferId, counterpartId, "홍*동", 1000,
                "KRW", Instant.parse("2026-11-20T07:50:41Z"), false)), PageRequest.of(0, 20), 1));

        mvc(MEMBER).perform(get("/api/v1/users/me/notifications"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.items[0].notificationId").value(notificationId.toString()))
                .andExpect(jsonPath("$.data.items[0].type").value("TRANSFER_RECEIVED"))
                .andExpect(jsonPath("$.data.items[0].transferId").value(transferId.toString()))
                .andExpect(jsonPath("$.data.items[0].counterpartId").value(counterpartId.toString()))
                .andExpect(jsonPath("$.data.items[0].counterpartName").value("홍*동"))
                .andExpect(jsonPath("$.data.items[0].amount").value(1000))
                .andExpect(jsonPath("$.data.items[0].currency").value("KRW"))
                .andExpect(jsonPath("$.data.items[0].occurredAt").value("2026-11-20T07:50:41Z"))
                .andExpect(jsonPath("$.data.items[0].read").value(false))
                .andExpect(jsonPath("$.data.totalElements").value(1));
    }

    @Test
    void returnsUnreadCountAndReadAllResult() throws Exception {
        when(service.countUnread(MEMBER.userId())).thenReturn(3L);
        when(service.markAllRead(MEMBER.userId())).thenReturn(3);

        mvc(MEMBER).perform(get("/api/v1/users/me/notifications/unread-count"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.count").value(3));
        mvc(MEMBER).perform(patch("/api/v1/users/me/notifications/read-all"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.updated").value(3));
    }

    @Test
    void marksOneNotificationReadAndReports404ForOthers() throws Exception {
        var mine = UUID.randomUUID();
        var other = UUID.randomUUID();
        doThrow(new BusinessException(UserErrorCode.NOTIFICATION_NOT_FOUND)).when(service).markRead(MEMBER.userId(), other);

        mvc(MEMBER).perform(patch("/api/v1/users/me/notifications/{id}/read", mine))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
        verify(service).markRead(MEMBER.userId(), mine);
        mvc(MEMBER).perform(patch("/api/v1/users/me/notifications/{id}/read", other))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error.code").value("USER_NOTIFICATION_NOT_FOUND"));
    }

    @Test
    void masterCannotUseMemberNotifications() throws Exception {
        mvc(MASTER).perform(get("/api/v1/users/me/notifications/unread-count"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("COMMON_FORBIDDEN"));
        verifyNoInteractions(service);
    }

    private MockMvc mvc(AuthenticatedUser user) {
        CurrentUserProvider provider = () -> Optional.ofNullable(user);
        return MockMvcBuilders.standaloneSetup(new UserNotificationController(service))
                .setCustomArgumentResolvers(new CurrentUserArgumentResolver(provider))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }
}
