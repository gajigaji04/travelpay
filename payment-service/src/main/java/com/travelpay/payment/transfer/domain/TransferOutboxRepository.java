package com.travelpay.payment.transfer.domain;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface TransferOutboxRepository extends JpaRepository<TransferOutbox, UUID> {

    /**
     * 발행 대상 선점: 미발행(PENDING) 건을 최대 limit건 잠가 조회합니다. 호출한 트랜잭션이 끝날 때까지 잠금이 유지됩니다.
     * 다른 서버가 잠근 행은 기다리지 않고 건너뛰므로(SKIP LOCKED) 서버마다 서로 다른 건을 발행합니다.
     * 계속 실패하는 건이 새 건을 막지 않도록 실패 횟수가 적은 건부터, 그다음 오래된 순으로 가져옵니다(지갑 Outbox와 같은 순서).
     */
    @Query(nativeQuery = true, value = """
            select * from transfer_outbox
            where status = 'PENDING'
            order by retry_count, created_at, id
            limit :limit
            for update skip locked
            """)
    List<TransferOutbox> findPendingForUpdate(int limit);
}
