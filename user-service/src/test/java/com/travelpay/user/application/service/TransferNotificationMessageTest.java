package com.travelpay.user.application.service;

import java.time.Instant;
import java.util.UUID;

import com.travelpay.user.application.service.TransferNotificationMessage.Party;
import com.travelpay.user.domain.model.entity.TransferNotification;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransferNotificationMessageTest {

    private static final UUID SENDER = UUID.fromString("1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed");
    private static final UUID RECEIVER = UUID.fromString("6ecd8c99-4036-403d-bf84-cf8400f67836");

    @Test
    void showsTheTransferThenEachPartyWithMaskedNameIdAndSignedAmount() {
        var transferId = UUID.fromString("ac625422-43d6-4e54-978e-17b7d7d81822");
        var notification = notification(transferId, 1000, "KRW", Instant.parse("2026-11-20T07:50:41.938833Z"));

        assertEquals("""
                송금 완료 · 1,000 KRW · 2026-11-20 16:50:41 KST · transfer ac625422-43d6-4e54-978e-17b7d7d81822
                보낸 분 홍*동 (1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed) -1,000 KRW
                받는 분 김* (6ecd8c99-4036-403d-bf84-cf8400f67836) +1,000 KRW""",
                TransferNotificationMessage.of(notification, new Party(SENDER, "홍길동"), new Party(RECEIVER, "김수")));
    }

    // 회원을 찾지 못해도 알림은 보내고, 이름 자리에 알 수 없음을 표시
    @Test
    void namesAMissingMemberAsUnknown() {
        var notification = notification(UUID.randomUUID(), 1050, "USD", Instant.parse("2026-11-20T07:50:41Z"));

        var message = TransferNotificationMessage.of(notification, new Party(SENDER, null), new Party(RECEIVER, "이몽룡"));

        assertTrue(message.contains("\n보낸 분 알 수 없는 회원 (" + SENDER + ") -10.50 USD"), message);
        assertTrue(message.contains("\n받는 분 이*룡 (" + RECEIVER + ") +10.50 USD"), message);
    }

    @Test
    void doesNotPrintTheRealNameWhenThePartyIsLogged() {
        assertFalse(new Party(SENDER, "홍길동").toString().contains("홍길동"));
    }

    // amount는 통화별 최소 단위 정수라 소수 자릿수가 있는 통화는 나눠서 보여 줌
    @ParameterizedTest
    @CsvSource({
            "1000, KRW, '1,000'",
            "1234567, KRW, '1,234,567'",
            "1050, USD, 10.50",
            "123456789, USD, '1,234,567.89'",
            "500, JPY, 500"
    })
    void formatsMinorUnitsWithTheCurrencyFractionDigits(long minorUnits, String currency, String expected) {
        assertEquals(expected, TransferNotificationMessage.amount(minorUnits, currency));
    }

    static TransferNotification notification(UUID transferId, long amount, String currency, Instant completedAt) {
        var notification = instantiate();
        ReflectionTestUtils.setField(notification, "eventId", UUID.randomUUID());
        ReflectionTestUtils.setField(notification, "transferId", transferId);
        ReflectionTestUtils.setField(notification, "amount", amount);
        ReflectionTestUtils.setField(notification, "currency", currency);
        ReflectionTestUtils.setField(notification, "completedAt", completedAt);
        ReflectionTestUtils.setField(notification, "receivedAt", completedAt);
        return notification;
    }

    private static TransferNotification instantiate() {
        try {
            var constructor = TransferNotification.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
