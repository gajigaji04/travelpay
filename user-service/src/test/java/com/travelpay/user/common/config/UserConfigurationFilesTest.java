package com.travelpay.user.common.config;

import java.io.IOException;
import java.time.Duration;

import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.web.server.autoconfigure.ServerProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mock.env.MockEnvironment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** 로컬·배포 설정 파일의 보안 관련 기본값과 실행 환경의 조정 가능 여부를 확인합니다. */
class UserConfigurationFilesTest {

    @Test
    void localAndProdBindTheSameRequestHeaderBudget() throws IOException {
        var local = serverProperties("application.yml");
        var prod = serverProperties("application-prod.yml");

        assertEquals(local.getMaxHttpRequestHeaderSize(), prod.getMaxHttpRequestHeaderSize());
    }

    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void passwordChangeUsesTheBoundedDatabaseTransactionTimeout(String filename) throws IOException {
        var properties = Binder.get(environment(filename))
                .bind("travelpay.user.password-change", PasswordChangeProperties.class).get();

        assertEquals(Duration.ofSeconds(5), properties.transactionTimeout());
    }

    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void passwordChangeTransactionTimeoutCanBeOverriddenByTheEnvironment(String filename) throws IOException {
        var environment = environment(filename)
                .withProperty("USER_PASSWORD_CHANGE_TRANSACTION_TIMEOUT", "3s");
        var properties = Binder.get(environment)
                .bind("travelpay.user.password-change", PasswordChangeProperties.class).get();

        assertEquals(Duration.ofSeconds(3), properties.transactionTimeout());
    }

    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void adminApisUseTheBoundedDatabaseTransactionTimeout(String filename) throws IOException {
        var properties = Binder.get(environment(filename))
                .bind("travelpay.user.admin", AdminProperties.class).get();

        assertEquals(Duration.ofSeconds(5), properties.transactionTimeout());
    }

    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void adminTransactionTimeoutCanBeOverriddenByTheEnvironment(String filename) throws IOException {
        var environment = environment(filename).withProperty("USER_ADMIN_TRANSACTION_TIMEOUT", "3s");
        var properties = Binder.get(environment).bind("travelpay.user.admin", AdminProperties.class).get();

        assertEquals(Duration.ofSeconds(3), properties.transactionTimeout());
    }

    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void databaseConnectionWaitIsBoundedBeforeTheTransactionStarts(String filename) throws IOException {
        var hikari = Binder.get(environment(filename)).bindOrCreate("spring.datasource.hikari", HikariConfig.class);

        assertEquals(2000, hikari.getConnectionTimeout());
        assertEquals(1000, hikari.getValidationTimeout());
    }

    // DB 서버가 멈추면 SQL 취소가 처리되지 않으므로 앱 연결 풀의 소켓 읽기에 상한을 둠. 정상 DB 작업 제한(5초)보다 길어야 함
    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void theAppPoolStopsWaitingForAFrozenDatabaseAfterTheSocketReadLimit(String filename) throws IOException {
        var hikari = Binder.get(environment(filename)).bindOrCreate("spring.datasource.hikari", HikariConfig.class);

        assertEquals("7", hikari.getDataSourceProperties().getProperty("socketTimeout"));
        assertEquals("1", hikari.getDataSourceProperties().getProperty("cancelSignalTimeout"));
    }

    // 읽기 트랜잭션의 마지막 질의가 제한 없이 실행되는 경우에도 잠금 대기는 DB가 읽기 실행 상한과 같은 2초에 끝냄
    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void theAppPoolGivesUpLockWaitsAtTheReadLimit(String filename) throws IOException {
        var hikari = Binder.get(environment(filename)).bindOrCreate("spring.datasource.hikari", HikariConfig.class);

        assertEquals("-c lock_timeout=2s", hikari.getDataSourceProperties().getProperty("options"));
    }

    // 시작 검사가 읽는 키가 설정 파일의 소켓 읽기 상한 키와 달라지면 검사가 조용히 건너뛰므로 실제 파일로 확인
    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void theConfiguredDatabaseLimitsPassTheStartupCheckAgainstTheSocketReadLimit(String filename) throws IOException {
        var environment = environment(filename).withProperty("USER_DB_URL",
                "jdbc:postgresql://localhost/test?currentSchema=user_service");
        var binder = Binder.get(environment);

        assertEquals(7, environment.getProperty(DatabaseTimeoutCheck.SOCKET_TIMEOUT, Integer.class));
        assertDoesNotThrow(() -> new DatabaseTimeoutCheck(environment,
                binder.bind("travelpay.user.password-change", PasswordChangeProperties.class).get(),
                binder.bind("travelpay.user.admin", AdminProperties.class).get()));
    }

    // 계정을 지정해야 Flyway가 앱 연결 풀이 아닌 별도 연결을 써서 마이그레이션이 소켓 읽기 상한에 끊기지 않음
    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void flywayUsesTheSameAccountOnItsOwnConnection(String filename) throws IOException {
        var environment = environment(filename)
                .withProperty("USER_DB_USERNAME", "user_app").withProperty("USER_DB_PASSWORD", "secret-for-test");

        assertEquals("user_app", environment.getProperty("spring.flyway.user"));
        assertEquals("secret-for-test", environment.getProperty("spring.flyway.password"));
        assertNull(environment.getProperty("spring.flyway.url"));
    }

    // 운영 Kafka 설정을 정하기 전이라 운영에서는 송금 완료 알림 소비자를 시작하지 않음(payment 발행기도 운영에서 꺼짐)
    @Test
    void theTransferNotificationConsumerRunsLocallyButNotInProd() throws IOException {
        assertEquals(true, transferNotification(environment("application.yml")).consumerEnabled());
        assertEquals(false, transferNotification(environment("application-prod.yml")).consumerEnabled());
    }

    // Webhook 주소는 비밀값이라 설정 파일에 두지 않고 환경 변수로만 받으며, 없으면 보내지 않음
    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void theSlackWebhookComesOnlyFromTheEnvironment(String filename) throws IOException {
        assertEquals(false, transferNotification(environment(filename)).slackConfigured());

        var configured = transferNotification(environment(filename)
                .withProperty("SLACK_WEBHOOK_URL", "https://hooks.example.test/services/T/B/x"));

        assertEquals("https://hooks.example.test/services/T/B/x", configured.slackWebhookUrl());
        assertEquals(false, configured.toString().contains("hooks.example.test"));
    }

    // 로컬·운영 모두 읽은 화면 알림 90일, 수신 기록 30일(토픽 보관 기간보다 김)로 정리하고 정리 주기를 켬
    @ParameterizedTest
    @ValueSource(strings = {"application.yml", "application-prod.yml"})
    void notificationRetentionKeepsReadNotificationsFor90DaysAndTransferEventsFor30Days(String filename)
            throws IOException {
        var environment = environment(filename);
        var properties = Binder.get(environment)
                .bind("travelpay.user.notification-retention", NotificationRetentionProperties.class).get();

        assertEquals(Duration.ofDays(90), properties.readNotificationRetention());
        assertEquals(Duration.ofDays(30), properties.transferEventRetention());
        assertEquals(1000, properties.batchSize());
        assertEquals("true", environment.getProperty("travelpay.user.notification-retention.enabled"));
        assertEquals(Duration.ofHours(1), Binder.get(environment)
                .bind("travelpay.user.notification-retention.fixed-delay", Duration.class).get());
    }

    private static TransferNotificationProperties transferNotification(MockEnvironment environment) {
        return Binder.get(environment)
                .bind("travelpay.user.transfer-notification", TransferNotificationProperties.class).get();
    }

    private static ServerProperties serverProperties(String filename) throws IOException {
        return Binder.get(environment(filename)).bindOrCreate("server", ServerProperties.class);
    }

    private static MockEnvironment environment(String filename) throws IOException {
        var environment = new MockEnvironment();
        for (var source : new YamlPropertySourceLoader().load(filename, new ClassPathResource(filename))) {
            environment.getPropertySources().addLast(source);
        }
        return environment;
    }
}
