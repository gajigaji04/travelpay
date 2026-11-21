package com.travelpay.user.application.service;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.travelpay.common.error.BusinessException;
import com.travelpay.common.error.CommonErrorCode;
import com.travelpay.user.application.dto.UserDisplayNameResult;
import com.travelpay.user.domain.repository.UserRepository;
import com.travelpay.user.presentation.dto.PersonalInfoMasking;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 송금 내역처럼 요청자 본인 기록에 나오는 상대 회원들의 이름을 마스킹해 한 번에 돌려줍니다.
 * <ul>
 *   <li>실명은 이 서비스 밖으로 내보내지 않고 마스킹한 값만 반환합니다({@link PersonalInfoMasking#realName}).</li>
 *   <li>지난 기록의 상대이므로 회원 상태와 관계없이 이름을 돌려주고, 없는 회원은 결과에서 뺍니다.</li>
 *   <li>수취인 실명 조회와 달리 호출 횟수 제한을 두지 않습니다. 호출하는 서비스(payment)가 요청자 본인 송금의 상대 ID만 넘기고,
 *   실명이 아닌 마스킹 이름만 나가기 때문입니다.</li>
 * </ul>
 */
@Service
public class UserDisplayNameService {

    public static final int MAX_IDS = 100;

    private final UserRepository userRepository;
    private final TransactionTemplate readTransactions;

    public UserDisplayNameService(UserRepository userRepository, PlatformTransactionManager transactionManager) {
        this.userRepository = userRepository;
        this.readTransactions = ReadTransactions.create(transactionManager);
    }

    public List<UserDisplayNameResult> getDisplayNames(UUID requesterId, List<UUID> userIds) {
        if (requesterId == null) {
            throw new BusinessException(CommonErrorCode.UNAUTHORIZED);
        }
        if (userIds == null || userIds.isEmpty() || userIds.contains(null)) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        }
        var distinctIds = new LinkedHashSet<>(userIds);
        if (distinctIds.size() > MAX_IDS) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        }
        try {
            return Objects.requireNonNull(readTransactions.execute(status -> userRepository.findAllById(distinctIds)
                    .stream()
                    .map(user -> new UserDisplayNameResult(user.getId(), PersonalInfoMasking.realName(user.getRealName())))
                    .toList()));
        }
        catch (DataAccessException | TransactionException exception) {
            throw new BusinessException(CommonErrorCode.SERVICE_UNAVAILABLE, exception);
        }
    }
}
