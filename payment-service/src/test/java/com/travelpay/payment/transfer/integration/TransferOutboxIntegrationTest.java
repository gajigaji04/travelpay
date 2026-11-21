package com.travelpay.payment.transfer.integration;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.travelpay.common.auth.AuthRole;
import com.travelpay.common.auth.AuthenticatedUser;
import com.travelpay.common.error.BusinessException;
import com.travelpay.common.response.ApiResponse;
import com.travelpay.payment.infra.client.ledger.LedgerFeignApi;
import com.travelpay.payment.transfer.application.TransferAdminService;
import com.travelpay.payment.transfer.application.TransferCompletedOutboxWriter;
import com.travelpay.payment.transfer.application.TransferReviewReconciler;
import com.travelpay.payment.transfer.client.TransferLedgerReader;
import com.travelpay.payment.transfer.domain.TransferErrorCode;
import com.travelpay.payment.transfer.domain.TransferRepository;
import com.travelpay.payment.transfer.domain.TransferStatus;
import com.travelpay.payment.transfer.domain.TransferStatusHistoryRepository;
import com.travelpay.payment.transfer.dto.AdminTransferResolveRequest;
import com.travelpay.payment.transfer.dto.TransferCreateRequest;
import com.travelpay.payment.transfer.dto.TransferResponse;
import com.travelpay.payment.transfer.support.TransferOutboxIntegrationSupport;
import feign.FeignException;
import feign.Request;
import feign.Response;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * 송금이 COMPLETED로 확정될 때 완료 이벤트가 Outbox(transfer_outbox)에 기록되는지를 실제 PostgreSQL(커밋되는 트랜잭션)로 확인합니다.
 * Kafka는 쓰지 않습니다(발행은 {@link TransferOutboxPublisherIntegrationTest}).
 * <ul>
 *     <li>완료 경로 4개(요청 처리·복구 재전송·운영자 확정·자동 대사)마다 Outbox 1건</li>
 *     <li>COMPLETED가 아닌 확정(FAILED·결과 모름)은 기록 없음</li>
 *     <li>재요청·동시 복구에도 송금당 1건, DB UNIQUE(transfer_id)</li>
 *     <li>Outbox 저장이 실패하면 상태 변경(·이력)도 롤백되고, 원인이 사라지면 다시 확정되며 1건 기록</li>
 * </ul>
 */
class TransferOutboxIntegrationTest extends TransferOutboxIntegrationSupport {

    @Autowired
    private TransferAdminService transferAdminService;

    @Autowired
    private TransferRepository transferRepository;

    @Autowired
    private TransferStatusHistoryRepository historyRepository;

    @Autowired
    private TransferLedgerReader ledgerReader;

    @Autowired
    private TransferCompletedOutboxWriter completedOutboxWriter;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private final AuthenticatedUser admin = new AuthenticatedUser(UUID.randomUUID(), AuthRole.MASTER);

    // ---- 완료 경로별 1건 ----

    @Test
    void requestCompletionRecordsOneOutboxRowWithContractPayload() {
        UUID sender = member();
        UUID receiver = member();

        TransferResponse response = transferService.request(sender, "outbox-request",
                new TransferCreateRequest(receiver, 3_000L, "JPY", "메모는 이벤트에 넣지 않음"));

        UUID transferId = response.transferId();
        assertEquals(TransferStatus.COMPLETED, response.status());
        Map<String, Object> outbox = outboxOf(transferId);
        assertEquals("PENDING", outbox.get("status"));
        assertEquals(0, outbox.get("retry_count"));
        assertNull(outbox.get("published_at"));
        Instant completedAt = updatedAtOf(transferId);
        assertEquals(completedAt, ((Timestamp) outbox.get("created_at")).toInstant());

        Map<String, Object> payload = parse((String) outbox.get("payload"));
        assertEquals(Set.of("transferId", "senderId", "receiverId", "amount", "currency", "completedAt"),
                payload.keySet());
        assertEquals(transferId.toString(), payload.get("transferId"));
        assertEquals(sender.toString(), payload.get("senderId"));
        assertEquals(receiver.toString(), payload.get("receiverId"));
        assertEquals(3_000L, ((Number) payload.get("amount")).longValue());
        assertEquals("JPY", payload.get("currency"));
        // 시각은 UTC ISO-8601 문자열이며 송금이 COMPLETED로 바뀐 시각(updated_at)과 같습니다.
        assertEquals(completedAt, Instant.parse((String) payload.get("completedAt")));
    }

    @Test
    void recoveryCompletionRecordsOneOutboxRow() {
        UUID sender = member();
        UUID receiver = member();
        givenLedgerTimesOutOnce();
        UUID transferId = transferService.request(sender, "outbox-recovery",
                new TransferCreateRequest(receiver, AMOUNT, "KRW", null)).transferId();
        assertEquals("PROCESSING", statusOf(transferId));
        assertEquals(0, outboxCount(transferId));
        makeRecoveryDue(transferId);

        assertEquals(1, transferService.recoverDue(Instant.now(), 10));

        assertEquals("COMPLETED", statusOf(transferId));
        assertEquals(1, outboxCount(transferId));
        assertEquals(updatedAtOf(transferId),
                Instant.parse((String) parse((String) outboxOf(transferId).get("payload")).get("completedAt")));
    }

    @Test
    void operatorResolutionToCompletedRecordsOneOutboxRow() {
        UUID transferId = manualReviewTransfer();
        UUID ledgerTransactionId = givenLedgerRecordPosted(transferId);

        transferAdminService.resolve(admin, transferId,
                new AdminTransferResolveRequest(TransferStatus.COMPLETED, "원장 POSTED 확인", ledgerTransactionId));
        // 같은 요청의 재시도는 기존 결과를 돌려줄 뿐 이벤트를 다시 기록하지 않습니다.
        transferAdminService.resolve(admin, transferId,
                new AdminTransferResolveRequest(TransferStatus.COMPLETED, "원장 POSTED 확인", ledgerTransactionId));

        assertEquals("COMPLETED", statusOf(transferId));
        assertEquals(1, outboxCount(transferId));
        Map<String, Object> payload = parse((String) outboxOf(transferId).get("payload"));
        assertEquals(transferId.toString(), payload.get("transferId"));
        assertEquals(AMOUNT, ((Number) payload.get("amount")).longValue());
        assertEquals(updatedAtOf(transferId), Instant.parse((String) payload.get("completedAt")));
    }

    @Test
    void autoReconciliationToCompletedRecordsOneOutboxRow() {
        UUID transferId = manualReviewTransfer();
        givenLedgerRecordPosted(transferId);

        newReconciler().reconcile(Instant.now(), 100);
        newReconciler().reconcile(Instant.now(), 100);

        assertEquals("COMPLETED", statusOf(transferId));
        assertEquals(1, outboxCount(transferId));
        assertEquals(updatedAtOf(transferId),
                Instant.parse((String) parse((String) outboxOf(transferId).get("payload")).get("completedAt")));
    }

    // ---- COMPLETED가 아니면 기록 없음 ----

    @Test
    void failedOrUnknownResultsRecordNothing() {
        UUID sender = member();
        UUID receiver = member();
        doThrow(ledgerRejectedInsufficientBalance()).when(ledgerFeignApi).register(any());

        BusinessException exception = assertThrows(BusinessException.class, () -> transferService.request(
                sender, "outbox-failed", new TransferCreateRequest(receiver, AMOUNT, "KRW", null)));

        assertEquals(TransferErrorCode.INSUFFICIENT_BALANCE, exception.getErrorCode());
        assertEquals(0, outboxCountOfSender(sender));

        UUID rejected = manualReviewTransfer();
        UUID rejectedLedgerId = givenLedgerRecordRejected(rejected);
        transferAdminService.resolve(admin, rejected,
                new AdminTransferResolveRequest(TransferStatus.FAILED, "원장 REJECTED 확인", rejectedLedgerId));
        assertEquals("FAILED", statusOf(rejected));
        assertEquals(0, outboxCount(rejected));

        UUID autoRejected = manualReviewTransfer();
        givenLedgerRecordRejected(autoRejected);
        newReconciler().reconcile(Instant.now(), 100);
        assertEquals("FAILED", statusOf(autoRejected));
        assertEquals(0, outboxCount(autoRejected));
    }

    // ---- 중복 방지 ----

    /** 응답 유실로 PROCESSING인 송금에 복구 4개와 같은 키 재요청 4건이 동시에 몰려도 완료 이벤트는 1건입니다. */
    @Test
    void concurrentRecoveriesAndRetriesRecordOneOutboxRow() throws Exception {
        UUID sender = member();
        UUID receiver = member();
        TransferCreateRequest request = new TransferCreateRequest(receiver, AMOUNT, "KRW", null);
        givenLedgerTimesOutOnce();
        UUID transferId = transferService.request(sender, "outbox-concurrent", request).transferId();
        makeRecoveryDue(transferId);

        List<Object> results = runConcurrently(8, i -> i < 4
                ? transferService.recoverDue(Instant.now(), 10)
                : transferService.request(sender, "outbox-concurrent", request));

        results.forEach(result -> assertThat(result).isNotInstanceOf(RuntimeException.class));
        assertEquals("COMPLETED", statusOf(transferId));
        assertEquals(1, outboxCount(transferId));
    }

    /** 같은 운영자의 같은 확정 요청 8건이 동시에 와도 완료 이벤트는 1건입니다. */
    @Test
    void concurrentOperatorResolutionsRecordOneOutboxRow() throws Exception {
        UUID transferId = manualReviewTransfer();
        UUID ledgerTransactionId = givenLedgerRecordPosted(transferId);
        AdminTransferResolveRequest request =
                new AdminTransferResolveRequest(TransferStatus.COMPLETED, "원장 POSTED 확인", ledgerTransactionId);

        List<Object> results = runConcurrently(8, i -> transferAdminService.resolve(admin, transferId, request));

        results.forEach(result -> assertThat(result).isNotInstanceOf(RuntimeException.class));
        assertEquals(1, outboxCount(transferId));
        assertEquals(1, historyCount(transferId));
    }

    /** DB 제약: 같은 송금의 Outbox 행은 하나만 들어갑니다. */
    @Test
    void outboxRowIsUniquePerTransfer() {
        UUID sender = member();
        UUID receiver = member();
        UUID transferId = transferService.request(sender, "outbox-unique",
                new TransferCreateRequest(receiver, AMOUNT, "KRW", null)).transferId();

        DataIntegrityViolationException exception = assertThrows(DataIntegrityViolationException.class,
                () -> jdbcTemplate.update("""
                        insert into transfer_outbox (id, transfer_id, payload, status, retry_count, created_at)
                        values (?, ?, '{}'::jsonb, 'PENDING', 0, now())""", UUID.randomUUID(), transferId));

        assertThat(exception.getMessage()).contains("uk_transfer_outbox_transfer_id");
        assertEquals(1, outboxCount(transferId));
    }

    // ---- 롤백 ----

    /**
     * 요청 처리 중 Outbox 저장이 실패하면 COMPLETED 변경도 롤백되어 PROCESSING으로 남고(결과 모름 1회, 응답 202),
     * 원인이 사라진 뒤 복구가 같은 reference로 다시 보내 확정하면서 이벤트를 1건 기록합니다.
     */
    @Test
    void outboxFailureRollsBackCompletionAndRecoveryRecordsItLater() {
        UUID sender = member();
        UUID receiver = member();
        UUID transferId;
        failOutboxInserts();
        try {
            TransferResponse response = transferService.request(sender, "outbox-rollback",
                    new TransferCreateRequest(receiver, AMOUNT, "KRW", null));
            transferId = response.transferId();
            assertEquals(TransferStatus.PROCESSING, response.status());
        } finally {
            allowOutboxInserts();
        }
        assertEquals("PROCESSING", statusOf(transferId));
        assertEquals(1, jdbcTemplate.queryForObject(
                "select attempt_count from transfer where id = ?", Integer.class, transferId));
        assertEquals(0, outboxCount(transferId));
        makeRecoveryDue(transferId);

        assertEquals(1, transferService.recoverDue(Instant.now(), 10));

        assertEquals("COMPLETED", statusOf(transferId));
        assertEquals(1, outboxCount(transferId));
        // 재전송은 같은 reference라 원장이 한 번만 반영합니다(원장 멱등, 여기서는 호출 횟수만 확인).
        verify(ledgerFeignApi, times(2)).register(any());
    }

    /** 운영자 확정 중 Outbox 저장이 실패하면 상태 변경과 이력이 함께 롤백되고, 다시 확정하면 모두 1건씩 남습니다. */
    @Test
    void outboxFailureRollsBackOperatorResolutionAndHistory() {
        UUID transferId = manualReviewTransfer();
        UUID ledgerTransactionId = givenLedgerRecordPosted(transferId);
        AdminTransferResolveRequest request =
                new AdminTransferResolveRequest(TransferStatus.COMPLETED, "원장 POSTED 확인", ledgerTransactionId);
        failOutboxInserts();
        try {
            assertThrows(RuntimeException.class, () -> transferAdminService.resolve(admin, transferId, request));
        } finally {
            allowOutboxInserts();
        }
        assertEquals("MANUAL_REVIEW", statusOf(transferId));
        assertEquals(0, historyCount(transferId));
        assertEquals(0, outboxCount(transferId));

        transferAdminService.resolve(admin, transferId, request);

        assertEquals("COMPLETED", statusOf(transferId));
        assertEquals(1, historyCount(transferId));
        assertEquals(1, outboxCount(transferId));
    }

    /** 자동 대사 중 Outbox 저장이 실패하면 상태 변경과 SYSTEM 이력이 함께 롤백되고, 다음 주기에 확정됩니다. */
    @Test
    void outboxFailureRollsBackAutoReconciliationAndHistory() {
        UUID transferId = manualReviewTransfer();
        givenLedgerRecordPosted(transferId);
        failOutboxInserts();
        try {
            assertThrows(RuntimeException.class, () -> newReconciler().reconcile(Instant.now(), 100));
        } finally {
            allowOutboxInserts();
        }
        assertEquals("MANUAL_REVIEW", statusOf(transferId));
        assertEquals(0, historyCount(transferId));
        assertEquals(0, outboxCount(transferId));

        newReconciler().reconcile(Instant.now(), 100);

        assertEquals("COMPLETED", statusOf(transferId));
        assertEquals(1, historyCount(transferId));
        assertEquals(1, outboxCount(transferId));
    }

    // ---- 준비·조회 ----

    /** 커서 상태를 테스트마다 새로 시작하도록 대사 객체를 직접 만듭니다. */
    private TransferReviewReconciler newReconciler() {
        return new TransferReviewReconciler(transferRepository, historyRepository, ledgerReader,
                completedOutboxWriter, transactionManager, new SimpleMeterRegistry());
    }

    /** transfer_outbox INSERT가 실패하도록 트리거를 겁니다. 반드시 {@link #allowOutboxInserts()}로 풉니다. */
    private void failOutboxInserts() {
        jdbcTemplate.execute("""
                create or replace function fail_transfer_outbox_insert() returns trigger as $$
                begin
                    raise exception 'transfer_outbox 저장 실패 (테스트)';
                end;
                $$ language plpgsql
                """);
        jdbcTemplate.execute("""
                create trigger fail_transfer_outbox_insert before insert on transfer_outbox
                for each row execute function fail_transfer_outbox_insert()
                """);
    }

    private void allowOutboxInserts() {
        jdbcTemplate.execute("drop trigger if exists fail_transfer_outbox_insert on transfer_outbox");
        jdbcTemplate.execute("drop function if exists fail_transfer_outbox_insert()");
    }

    private int outboxCountOfSender(UUID sender) {
        return jdbcTemplate.queryForObject("""
                select count(*) from transfer_outbox
                where transfer_id in (select id from transfer where sender_id = ?)""", Integer.class, sender);
    }

    private int historyCount(UUID transferId) {
        return jdbcTemplate.queryForObject(
                "select count(*) from transfer_status_history where transfer_id = ?", Integer.class, transferId);
    }

    private static FeignException ledgerRejectedInsufficientBalance() {
        Response response = Response.builder()
                .status(422)
                .reason("error")
                .request(Request.create(Request.HttpMethod.POST,
                        "http://ledger-service/internal/v1/ledger/transactions", Map.of(), null, UTF_8, null))
                .headers(Map.of())
                .body("""
                        {"success":false,"data":null,"error":{"code":"LEDGER_REJECTED_INSUFFICIENT_BALANCE","message":"error","details":[]}}""",
                        UTF_8)
                .build();
        return FeignException.errorStatus("LedgerFeignApi#register(RegisterRequest)", response);
    }

    /** 작업 n개를 동시에 시작하고, 각 결과(반환값 또는 던진 예외)를 순서대로 돌려줍니다. */
    private static List<Object> runConcurrently(int count, IndexedTask task) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(count);
        CountDownLatch ready = new CountDownLatch(count);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Object>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int index = i;
                futures.add(executor.submit((Callable<Object>) () -> {
                    ready.countDown();
                    start.await();
                    try {
                        return task.run(index);
                    } catch (RuntimeException exception) {
                        return exception;
                    }
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS), "동시 실행 준비 시간 초과");
            start.countDown();
            List<Object> results = new ArrayList<>();
            for (Future<Object> future : futures) {
                results.add(future.get(60, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            executor.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface IndexedTask {
        Object run(int index);
    }
}
