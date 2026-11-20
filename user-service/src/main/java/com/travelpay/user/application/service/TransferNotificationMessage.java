package com.travelpay.user.application.service;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Currency;
import java.util.Locale;
import java.util.UUID;

import com.travelpay.user.domain.model.entity.TransferNotification;
import com.travelpay.user.presentation.dto.PersonalInfoMasking;

/**
 * Slack에 올리는 송금 완료 문구입니다. 첫 줄은 금액·통화·완료 시각(KST)·송금 ID이고,
 * 다음 두 줄은 보낸 분·받는 분의 마스킹한 이름과 회원 ID, 각자 기준의 증감입니다.
 * 이름은 마스킹해서만 보내고(관리자 회원 검색과 같은 규칙), 전화번호는 넣지 않습니다.
 * 예:
 * <pre>
 * 송금 완료 · 1,000 KRW · 2026-11-20 16:50:41 KST · transfer ac625422-43d6-4e54-978e-17b7d7d81822
 * 보낸 분 홍*동 (1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed) -1,000 KRW
 * 받는 분 김*수 (6ecd8c99-4036-403d-bf84-cf8400f67836) +1,000 KRW
 * </pre>
 */
public final class TransferNotificationMessage {

    /** 회원이 없을 때(탈퇴 정리 등) 이름 대신 씁니다. */
    static final String UNKNOWN_MEMBER = "알 수 없는 회원";

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'KST'")
            .withZone(KST);

    private TransferNotificationMessage() {
    }

    /** realName이 null이면 회원을 찾지 못한 것으로 보고 {@link #UNKNOWN_MEMBER}로 씁니다. */
    public record Party(UUID userId, String realName) {

        @Override
        public String toString() {
            return "Party[userId=" + userId + ", realName=REDACTED]";
        }
    }

    public static String of(TransferNotification notification, Party sender, Party receiver) {
        String amount = amount(notification.getAmount(), notification.getCurrency()) + " " + notification.getCurrency();
        return "송금 완료 · " + amount
                + " · " + TIME.format(notification.getCompletedAt())
                + " · transfer " + notification.getTransferId()
                + "\n보낸 분 " + party(sender) + " -" + amount
                + "\n받는 분 " + party(receiver) + " +" + amount;
    }

    private static String party(Party party) {
        String name = party.realName() == null ? UNKNOWN_MEMBER : PersonalInfoMasking.realName(party.realName());
        return name + " (" + party.userId() + ")";
    }

    /** 최소 단위 정수를 통화의 소수 자릿수에 맞춰 천 단위 구분과 함께 씁니다(KRW 1000 → 1,000, USD 1050 → 10.50). */
    static String amount(long minorUnits, String currency) {
        int digits = Math.max(Currency.getInstance(currency).getDefaultFractionDigits(), 0);
        var format = new DecimalFormat(digits == 0 ? "#,##0" : "#,##0." + "0".repeat(digits),
                DecimalFormatSymbols.getInstance(Locale.ROOT));
        return format.format(BigDecimal.valueOf(minorUnits, digits));
    }
}
