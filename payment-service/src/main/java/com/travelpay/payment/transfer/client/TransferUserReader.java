package com.travelpay.payment.transfer.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.travelpay.common.error.BusinessException;
import com.travelpay.common.error.CommonErrorCode;
import com.travelpay.common.feign.FeignErrors;
import com.travelpay.common.response.ApiResponse;
import com.travelpay.payment.infra.client.user.UserFeignApi;
import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 수취인이 송금 받을 수 있는 회원(ACTIVE)인지 user-service 내부 API(I02, {@link UserFeignApi})로 확인합니다.
 * 404 + USER_RECIPIENT_NOT_FOUND만 수취인 없음으로 보고, 그 밖의 실패는 common 규칙({@link FeignErrors#toCommonException})을 따릅니다.
 */
@Slf4j
@Component
public class TransferUserReader {

    private static final String RECIPIENT_NOT_FOUND = "USER_RECIPIENT_NOT_FOUND";

    private final UserFeignApi userFeignApi;

    public TransferUserReader(UserFeignApi userFeignApi) {
        this.userFeignApi = userFeignApi;
    }

    /**
     * ACTIVE 회원이면 회원 정보를, 없는 회원이거나 정지·종료 회원이면 empty를 반환합니다(user-service가 둘을 구분하지 않음).
     * 경로 오류·배포 문제로 생기는 404는 수취인 없음이 아니므로 404와 USER_RECIPIENT_NOT_FOUND가 모두 맞을 때만 empty로 봅니다.
     * 다른 회원의 이름이 노출되지 않도록 응답의 회원 ID가 요청한 ID와 같은지 확인합니다.
     */
    public Optional<UserRecipientResponse> findRecipient(UUID userId) {
        ApiResponse<UserFeignApi.UserResponse> response;
        try {
            response = userFeignApi.getActiveUser(userId);
        } catch (FeignException exception) {
            if (exception.status() == 404
                    && FeignErrors.hasErrorCode(exception, RECIPIENT_NOT_FOUND)) {
                return Optional.empty();
            }
            throw FeignErrors.toCommonException(exception);
        }

        if (response == null || !response.success() || response.data() == null
                || !userId.equals(response.data().userId())
                || response.data().realName() == null || response.data().realName().isBlank()) {
            throw new BusinessException(CommonErrorCode.BAD_GATEWAY);
        }
        return Optional.of(new UserRecipientResponse(response.data().userId(), response.data().realName()));
    }

    /**
     * 송금 내역에 보여 줄 상대방 마스킹 이름을 한 번에 조회합니다(user-service 일괄 이름 조회).
     * 이름은 보조 정보라 조회가 실패해도 내역 조회를 막지 않도록 예외 대신 빈 결과를 반환하고 WARN만 남깁니다.
     * 요청하지 않은 회원 ID가 응답에 섞이면 그 항목은 버립니다.
     */
    public Map<UUID, String> findDisplayNames(Collection<UUID> userIds) {
        if (userIds.isEmpty()) {
            return Map.of();
        }
        Set<UUID> requested = Set.copyOf(userIds);
        try {
            var response = userFeignApi.getDisplayNames(List.copyOf(requested));
            if (response == null || !response.success() || response.data() == null) {
                log.warn("상대방 이름 응답을 해석하지 못해 이름 없이 응답함 count={}", requested.size());
                return Map.of();
            }
            Map<UUID, String> names = new HashMap<>();
            for (var item : response.data()) {
                if (item != null && requested.contains(item.userId())
                        && item.maskedName() != null && !item.maskedName().isBlank()) {
                    names.put(item.userId(), item.maskedName());
                }
            }
            return names;
        } catch (RuntimeException exception) {
            log.warn("상대방 이름 조회 실패로 이름 없이 응답함 count={} reason={}",
                    requested.size(), exception.getClass().getSimpleName());
            return Map.of();
        }
    }
}
