package com.travelpay.payment.transfer.support;

import java.net.SocketTimeoutException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.travelpay.common.response.ApiResponse;
import com.travelpay.payment.infra.client.ledger.LedgerFeignApi;
import com.travelpay.payment.infra.client.user.UserFeignApi;
import com.travelpay.payment.infra.client.wallet.WalletFeignApi;
import com.travelpay.payment.support.PostgresContainerSupport;
import com.travelpay.payment.transfer.application.TransferService;
import com.travelpay.payment.transfer.client.TransferLedgerClientAdapter;
import feign.Request;
import feign.RetryableException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.json.JsonMapper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

/**
 * 송금 완료 이벤트(Outbox) 통합 테스트 공통 구성입니다. 실제 PostgreSQL과 실제 서비스를 쓰고 Feign 인터페이스만 Mock입니다.
 * 상태 변경과 Outbox 기록이 함께 커밋·롤백되는지 봐야 해서 테스트 트랜잭션으로 감싸지 않고, 만든 데이터는 테스트마다 지웁니다.
 * 스케줄러(복구·정산·Outbox 발행)는 꺼 두고 필요한 메소드를 직접 호출합니다(발행기는 테스트 공통 설정에서 끔).
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "transfer.recovery.enabled=false",
        "payment.settlement-recovery.enabled=false"
})
public abstract class TransferOutboxIntegrationSupport extends PostgresContainerSupport {

    protected static final long AMOUNT = 1_000L;

    @Autowired
    protected TransferService transferService;

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected JsonMapper jsonMapper;

    @Autowired
    private TransferLedgerClientAdapter ledgerClientAdapter;

    @MockitoBean
    protected LedgerFeignApi ledgerFeignApi;

    @MockitoBean
    protected WalletFeignApi walletFeignApi;

    @MockitoBean
    protected UserFeignApi userFeignApi;

    protected final Map<UUID, UUID> walletOf = new HashMap<>();
    private final Set<UUID> users = new HashSet<>();

    @BeforeEach
    void setUpOutboxSupport() {
        // 서킷은 컨텍스트에 하나라 앞 테스트의 원장 실패가 이어지지 않게 닫습니다.
        ((CircuitBreaker) ReflectionTestUtils.getField(ledgerClientAdapter, "circuitBreaker")).reset();
        // 발행기는 테이블 전체의 PENDING을 가져가므로 다른 테스트가 남긴 행이 없게 합니다.
        jdbcTemplate.update("delete from transfer_outbox");
        givenLedgerPosts();
    }

    @AfterEach
    void cleanUpOutboxSupport() {
        for (UUID user : users) {
            jdbcTemplate.update("""
                    delete from transfer_outbox
                    where transfer_id in (select id from transfer where sender_id = ? or receiver_id = ?)""", user, user);
            jdbcTemplate.update("""
                    delete from transfer_status_history
                    where transfer_id in (select id from transfer where sender_id = ? or receiver_id = ?)""", user, user);
            jdbcTemplate.update("delete from transfer where sender_id = ? or receiver_id = ?", user, user);
        }
    }

    /** ACTIVE 회원·ACTIVE 지갑을 만듭니다. */
    protected UUID member() {
        UUID user = UUID.randomUUID();
        users.add(user);
        walletOf.put(user, UUID.randomUUID());
        doReturn(ApiResponse.success(new WalletFeignApi.WalletResponse(walletOf.get(user), "ACTIVE")))
                .when(walletFeignApi).getWallet(user);
        doReturn(ApiResponse.success(new UserFeignApi.UserResponse(user, "홍길동")))
                .when(userFeignApi).getActiveUser(user);
        return user;
    }

    /** 원장 거래 등록이 POSTED로 응답하게 합니다(기본값). */
    protected void givenLedgerPosts() {
        doAnswer(invocation -> ApiResponse.success(new LedgerFeignApi.TransactionResponse(UUID.randomUUID(), "POSTED")))
                .when(ledgerFeignApi).register(any());
    }

    /** 원장 거래 등록이 한 번 응답 시간 초과(결과 모름)가 된 뒤 POSTED로 응답하게 합니다. */
    protected void givenLedgerTimesOutOnce() {
        AtomicBoolean timedOut = new AtomicBoolean();
        doAnswer(invocation -> {
            if (timedOut.compareAndSet(false, true)) {
                throw new RetryableException(-1, "Read timed out", Request.HttpMethod.POST,
                        new SocketTimeoutException("Read timed out"), (Long) null,
                        Request.create(Request.HttpMethod.POST,
                                "http://ledger-service/internal/v1/ledger/transactions", Map.of(), null, UTF_8, null));
            }
            return ApiResponse.success(new LedgerFeignApi.TransactionResponse(UUID.randomUUID(), "POSTED"));
        }).when(ledgerFeignApi).register(any());
    }

    /** 원장 조회가 이 송금과 같은 항목의 POSTED 거래를 돌려주게 하고 그 거래 ID를 반환합니다. */
    protected UUID givenLedgerRecordPosted(UUID transferId) {
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "select source_wallet_id, target_wallet_id from transfer where id = ?", transferId);
        UUID ledgerTransactionId = UUID.randomUUID();
        List<LedgerFeignApi.EntryResult> entries = List.of(
                new LedgerFeignApi.EntryResult("USER", (UUID) row.get("source_wallet_id"), "WALLET", "KRW", "DEBIT",
                        AMOUNT, "TRANSFER_OUT"),
                new LedgerFeignApi.EntryResult("USER", (UUID) row.get("target_wallet_id"), "WALLET", "KRW", "CREDIT",
                        AMOUNT, "TRANSFER_IN"));
        doReturn(ApiResponse.success(
                new LedgerFeignApi.TransactionResultResponse(ledgerTransactionId, "POSTED", null, entries)))
                .when(ledgerFeignApi).getTransaction("TRANSFER", transferId);
        return ledgerTransactionId;
    }

    /** 원장 조회가 잔액 부족으로 거절된 거래를 돌려주게 하고 그 거래 ID를 반환합니다. */
    protected UUID givenLedgerRecordRejected(UUID transferId) {
        UUID ledgerTransactionId = UUID.randomUUID();
        doReturn(ApiResponse.success(new LedgerFeignApi.TransactionResultResponse(
                ledgerTransactionId, "REJECTED", "INSUFFICIENT_BALANCE", List.of())))
                .when(ledgerFeignApi).getTransaction("TRANSFER", transferId);
        return ledgerTransactionId;
    }

    /** 지정한 상태의 송금(KRW 1,000)을 커밋해 둡니다. 생성·수정 시각은 at입니다. */
    protected UUID insertTransfer(String status, String reviewReason, int attemptCount, Instant at) {
        UUID sender = member();
        UUID receiver = member();
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                        insert into transfer (id, sender_id, receiver_id, source_wallet_id, target_wallet_id, amount,
                            currency, status, failure_reason, attempt_count, review_reason, memo, idempotency_key,
                            created_at, updated_at)
                        values (?, ?, ?, ?, ?, ?, 'KRW', ?, null, ?, ?, null, ?, ?, ?)""",
                id, sender, receiver, walletOf.get(sender), walletOf.get(receiver), AMOUNT, status, attemptCount,
                reviewReason, "outbox-" + id, at.atOffset(ZoneOffset.UTC), at.atOffset(ZoneOffset.UTC));
        return id;
    }

    /** 결과 모름 5회로 MANUAL_REVIEW가 된 지 오래된(자동 대사 대기 시간이 지난) 송금을 커밋해 둡니다. */
    protected UUID manualReviewTransfer() {
        return insertTransfer("MANUAL_REVIEW", "RETRY_EXHAUSTED", 5, Instant.now().minusSeconds(600));
    }

    /** 완료된 송금과 미발행 Outbox 행을 SQL로 직접 넣고 이벤트 ID(Outbox 행 ID)를 반환합니다. */
    protected UUID insertCompletedTransferWithPendingOutbox(Instant createdAt) {
        UUID transferId = insertTransfer("COMPLETED", null, 0, createdAt);
        UUID eventId = UUID.randomUUID();
        jdbcTemplate.update("""
                        insert into transfer_outbox (id, transfer_id, payload, status, retry_count, created_at)
                        values (?, ?, ?::jsonb, 'PENDING', 0, ?)""",
                eventId, transferId, "{\"transferId\":\"" + transferId + "\"}", createdAt.atOffset(ZoneOffset.UTC));
        return eventId;
    }

    protected String statusOf(UUID transferId) {
        return jdbcTemplate.queryForObject("select status from transfer where id = ?", String.class, transferId);
    }

    protected Instant updatedAtOf(UUID transferId) {
        return jdbcTemplate.queryForObject("select updated_at from transfer where id = ?", Timestamp.class, transferId)
                .toInstant();
    }

    protected int outboxCount(UUID transferId) {
        return jdbcTemplate.queryForObject(
                "select count(*) from transfer_outbox where transfer_id = ?", Integer.class, transferId);
    }

    /** 송금의 Outbox 행입니다(없거나 여러 건이면 예외). payload는 JSON 문자열로 읽습니다. */
    protected Map<String, Object> outboxOf(UUID transferId) {
        return jdbcTemplate.queryForMap("""
                select id, transfer_id, payload::text as payload, status, retry_count, created_at, published_at
                from transfer_outbox where transfer_id = ?""", transferId);
    }

    protected Map<String, Object> outboxById(UUID eventId) {
        return jdbcTemplate.queryForMap("""
                select id, transfer_id, payload::text as payload, status, retry_count, created_at, published_at
                from transfer_outbox where id = ?""", eventId);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> parse(String payload) {
        return jsonMapper.readValue(payload, Map.class);
    }

    /** 송금 처리 중 재시도 대기 시간이 지난 것처럼 수정 시각을 과거로 돌립니다. */
    protected void makeRecoveryDue(UUID transferId) {
        jdbcTemplate.update("update transfer set updated_at = updated_at - interval '10 minutes' where id = ?",
                transferId);
    }
}
