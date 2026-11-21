package com.travelpay.user.application.service;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.travelpay.common.error.BusinessException;
import com.travelpay.common.error.CommonErrorCode;
import com.travelpay.user.application.dto.UserNotificationResult;
import com.travelpay.user.common.error.UserErrorCode;
import com.travelpay.user.domain.model.entity.User;
import com.travelpay.user.domain.model.entity.UserNotification;
import com.travelpay.user.domain.repository.UserNotificationRepository;
import com.travelpay.user.domain.repository.UserRepository;
import com.travelpay.user.presentation.dto.PersonalInfoMasking;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 로그인한 회원 본인의 화면 알림을 조회하고 읽음으로 표시합니다.
 * 알림 행은 송금 완료 이벤트 소비자({@link TransferNotificationService})가 만듭니다.
 * 프런트는 읽지 않은 개수를 주기적으로 조회(폴링)하므로 그 조회는 부분 인덱스 하나로 끝나게 둡니다.
 */
@Service
public class UserNotificationService {

    public static final int MAX_PAGE_SIZE = 100;

    private final UserNotificationRepository notifications;
    private final UserRepository users;
    private final TransactionTemplate readTransactions;
    private final TransactionTemplate writeTransactions;
    private final Clock clock;

    public UserNotificationService(UserNotificationRepository notifications, UserRepository users,
            PlatformTransactionManager transactionManager, Clock clock) {
        this.notifications = notifications;
        this.users = users;
        this.readTransactions = ReadTransactions.create(transactionManager);
        this.writeTransactions = new TransactionTemplate(transactionManager);
        this.writeTransactions.setTimeout(ReadTransactions.TIMEOUT_SECONDS);
        this.clock = clock;
    }

    /** page는 0부터, size는 1 이상이며 100을 넘으면 100으로 조회합니다. 상대방 이름은 한 페이지당 한 번에 조회합니다. */
    public Page<UserNotificationResult> list(UUID userId, int page, int size) {
        if (page < 0 || size < 1) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        }
        var pageable = PageRequest.of(page, Math.min(size, MAX_PAGE_SIZE));
        return run(readTransactions, status -> {
            Page<UserNotification> found = notifications.findByUserId(userId, pageable);
            var counterpartIds = found.stream().map(UserNotification::getCounterpartId).collect(Collectors.toSet());
            Map<UUID, String> names = counterpartIds.isEmpty() ? Map.of() : users.findAllById(counterpartIds).stream()
                    .collect(Collectors.toMap(User::getId, user -> PersonalInfoMasking.realName(user.getRealName()),
                            (first, second) -> first));
            return found.map(notification -> toResult(notification, names));
        });
    }

    public long countUnread(UUID userId) {
        return run(readTransactions, status -> notifications.countUnread(userId));
    }

    /** 본인 알림만 읽음으로 바꿉니다. 없거나 다른 회원의 알림이면 같은 404이며, 이미 읽은 알림은 그대로 성공합니다. */
    public void markRead(UUID userId, UUID notificationId) {
        run(writeTransactions, status -> {
            notifications.findByIdAndUserId(notificationId, userId)
                    .orElseThrow(() -> new BusinessException(UserErrorCode.NOTIFICATION_NOT_FOUND))
                    .markRead(clock.instant());
            return Boolean.TRUE;
        });
    }

    public int markAllRead(UUID userId) {
        return run(writeTransactions, status -> notifications.markAllRead(userId, clock.instant()));
    }

    private static UserNotificationResult toResult(UserNotification notification, Map<UUID, String> names) {
        return new UserNotificationResult(notification.getId(), notification.getType(), notification.getTransferId(),
                notification.getCounterpartId(), names.get(notification.getCounterpartId()), notification.getAmount(),
                notification.getCurrency(), notification.getOccurredAt(), notification.isRead());
    }

    /** 트랜잭션 시작·종료 실패까지 503으로 바꾸도록 트랜잭션을 이 메서드 안에서 실행합니다. */
    private static <T> T run(TransactionTemplate template, TransactionCallback<T> callback) {
        try {
            return Objects.requireNonNull(template.execute(callback));
        }
        catch (DataAccessException | TransactionException exception) {
            throw new BusinessException(CommonErrorCode.SERVICE_UNAVAILABLE, exception);
        }
    }
}
