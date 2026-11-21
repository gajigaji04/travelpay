package com.travelpay.payment.transfer.application;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.travelpay.common.response.PageResponse;
import com.travelpay.payment.transfer.client.TransferUserReader;
import com.travelpay.payment.transfer.dto.TransferResponse;
import org.springframework.stereotype.Component;

/**
 * 회원용 송금 응답에 상대방 마스킹 이름을 채웁니다.
 * <ul>
 *   <li>DB 조회 트랜잭션이 끝난 뒤(컨트롤러에서) 부르므로, user-service를 기다리는 동안 DB 연결을 잡지 않습니다.</li>
 *   <li>한 페이지의 상대방을 모아 한 번만 조회합니다(최대 100명, 페이지 크기 상한과 같음).</li>
 *   <li>조회가 실패하면 이름 없이(null) 그대로 응답합니다({@link TransferUserReader#findDisplayNames}).</li>
 * </ul>
 */
@Component
public class TransferCounterpartNames {

    private final TransferUserReader userReader;

    public TransferCounterpartNames(TransferUserReader userReader) {
        this.userReader = userReader;
    }

    public TransferResponse fill(TransferResponse response) {
        return fill(List.of(response)).getFirst();
    }

    public PageResponse<TransferResponse> fill(PageResponse<TransferResponse> page) {
        return new PageResponse<>(fill(page.items()), page.page(), page.size(), page.totalElements(),
                page.totalPages());
    }

    private List<TransferResponse> fill(List<TransferResponse> responses) {
        if (responses.isEmpty()) {
            return responses;
        }
        var counterpartIds = new LinkedHashSet<UUID>();
        responses.forEach(response -> counterpartIds.add(response.counterpartId()));
        Map<UUID, String> names = userReader.findDisplayNames(counterpartIds);
        return responses.stream()
                .map(response -> response.withCounterpartName(names.get(response.counterpartId())))
                .toList();
    }
}
