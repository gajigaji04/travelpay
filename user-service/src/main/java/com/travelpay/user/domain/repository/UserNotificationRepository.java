package com.travelpay.user.domain.repository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.travelpay.user.domain.model.entity.UserNotification;
import com.travelpay.user.domain.model.vo.UserNotificationType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/** 회원 화면 알림의 저장·조회 계약입니다. */
public interface UserNotificationRepository {

    /**
     * 같은 이벤트·회원의 알림이 없을 때만 읽지 않은 상태로 만듭니다. 이미 있으면 아무것도 바꾸지 않습니다.
     *
     * @return 이번에 새로 만들었으면 true
     */
    boolean recordIfAbsent(UUID eventId, UUID userId, UserNotificationType type, UUID transferId,
            UUID counterpartId, long amount, String currency, Instant occurredAt, Instant createdAt);

    /** 회원의 알림을 최신순(발생 시각 내림차순, 같으면 ID 내림차순)으로 조회합니다. */
    Page<UserNotification> findByUserId(UUID userId, Pageable pageable);

    long countUnread(UUID userId);

    /** 회원 본인의 알림만 찾습니다. 다른 회원의 알림 ID면 빈 결과입니다. */
    Optional<UserNotification> findByIdAndUserId(UUID notificationId, UUID userId);

    /** 회원의 읽지 않은 알림을 모두 읽음으로 바꾸고 바꾼 개수를 반환합니다. */
    int markAllRead(UUID userId, Instant readAt);

    /**
     * 발생 시각이 cutoff보다 이른 읽은 알림을 오래된 것부터 최대 limit개 지웁니다. 읽지 않은 알림은 지우지 않습니다.
     * 다른 트랜잭션이 잠근 행은 기다리지 않고 건너뜁니다. 쓰기 트랜잭션 안에서 호출합니다.
     *
     * @return 지운 행 수
     */
    int deleteReadOccurredBefore(Instant cutoff, int limit);
}
