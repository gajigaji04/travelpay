package com.travelpay.user;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.travelpay.common.auth.AuthHeaders;
import com.travelpay.common.auth.AuthRole;
import com.travelpay.user.application.service.LoginAttemptLimiter;
import com.travelpay.user.application.service.PasswordChangeService;
import com.travelpay.user.common.config.LoginAttemptProperties;
import com.travelpay.user.common.config.PasswordChangeProperties;
import com.travelpay.user.common.security.session.UserSessionPrincipal;
import com.travelpay.user.infrastructure.redis.RedisLoginAttemptLimiter;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.request;

/** 테스트용으로 고른 빈이 아니라 실제 애플리케이션의 컴포넌트 스캔·자동 설정·application.yml로 시작합니다. */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.discovery.enabled=false",
        "spring.cloud.service-registry.auto-registration.enabled=false",
        // 로그 형식은 테스트 JVM 전체에 남습니다. 다른 테스트가 일반 텍스트 로그를 확인하므로 JSON 로그를 켜지 않습니다.
        "logging.structured.format.console="
})
@ExtendWith(OutputCaptureExtension.class)
class UserServiceApplicationTest {

    private static final String REDIS_PASSWORD = UUID.randomUUID().toString();

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-bookworm")
            .withDatabaseName("travelpay_user_app_test")
            .withUsername("travelpay_test")
            .withPassword(UUID.randomUUID().toString())
            .withInitScript("postgres/user-schema.sql")
            .withUrlParam("currentSchema", "user_service");

    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:8.2.11-bookworm"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--requirepass", REDIS_PASSWORD, "--appendonly", "no", "--save", "");

    static {
        POSTGRES.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void connections(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> REDIS_PASSWORD);
    }

    private static final Set<RequestMethod> STATE_CHANGING =
            Set.of(RequestMethod.POST, RequestMethod.PUT, RequestMethod.PATCH, RequestMethod.DELETE);
    private static final Set<String> PUBLIC_MUTATIONS = Set.of("POST /api/v1/users", "POST /api/v1/auth/login");

    @Autowired private ApplicationContext context;

    @Test
    void startsWithoutTheUnusedDefaultUserOrItsGeneratedPassword(CapturedOutput output) {
        assertEquals(Map.of(), context.getBeansOfType(UserDetailsService.class));
        assertFalse(output.getAll().contains("generated security password"));
    }

    @Test
    void wiresTheRedisLoginAttemptLimiterWithTheConfiguredPolicy() {
        assertInstanceOf(RedisLoginAttemptLimiter.class, context.getBean(LoginAttemptLimiter.class));
        assertEquals(new LoginAttemptProperties(5, 20, Duration.ofMinutes(10), Duration.ofMinutes(20)),
                context.getBean(LoginAttemptProperties.class));
    }

    @Test
    void startsOnTheSchemaFlywayMigratedWhileHibernateOnlyValidatesIt() {
        var applied = Arrays.stream(context.getBean(Flyway.class).info().applied())
                .map(migration -> migration.getVersion().getVersion())
                .toList();

        assertEquals(List.of("1", "2", "3", "4", "5"), applied);
        assertEquals("validate", context.getBean(EntityManagerFactory.class).getProperties().get("hibernate.hbm2ddl.auto"));
    }

    @Test
    void wiresPasswordChangeWithTheConfiguredDatabaseTransactionTimeout() {
        assertInstanceOf(PasswordChangeService.class, context.getBean(PasswordChangeService.class));
        assertEquals(Duration.ofSeconds(5), context.getBean(PasswordChangeProperties.class).transactionTimeout());
    }

    // 다른 코드가 참조하지 않는 시작 검사라 등록이 빠져도 컴파일·단위 테스트는 통과하므로 실제 컴포넌트 스캔에 들어가는지 확인
    @Test
    void checksTheDatabaseLimitsAgainstTheSocketReadLimitAtStartup() {
        assertTrue(context.containsBean("databaseTimeoutCheck"));
    }

    // 계정을 지정했으므로 마이그레이션은 앱 연결 풀이 아닌 별도 연결로 실행돼, 풀의 소켓 읽기 상한에 끊기지 않음
    @Test
    void flywayMigratesThroughItsOwnConnectionRatherThanTheAppPool() throws SQLException {
        var migrationDataSource = context.getBean(Flyway.class).getConfiguration().getDataSource();

        assertNotSame(context.getBean(DataSource.class), migrationDataSource);
        assertInstanceOf(SimpleDriverDataSource.class, migrationDataSource);
        // 앱 연결 풀의 잠금 대기 상한도 마이그레이션 연결에는 걸리지 않음
        assertEquals("2s", lockTimeout(context.getBean(DataSource.class)));
        assertEquals("0", lockTimeout(migrationDataSource));
    }

    private static String lockTimeout(DataSource dataSource) throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("show lock_timeout")) {
            result.next();
            return result.getString(1);
        }
    }

    // DB 서버가 멈추면 SQL 취소 요청도 처리되지 않음. 운영 풀 크기(5)만큼의 연결이 실행 상한이 걸린 조회로 동시에 묶여도
    // 취소 대기에 줄 서지 않고 소켓 읽기 상한 근처에서 끊기며(Gateway의 User 라우트 응답 제한 10초 전), DB가 돌아오면 다시 조회됨
    @Test
    void appPoolConnectionsStopWaitingForAFrozenDatabaseAtTheSocketReadLimit() throws Exception {
        var dataSource = context.getBean(DataSource.class);
        var docker = DockerClientFactory.instance().client();
        var connections = new ArrayList<Connection>();
        try (var executor = Executors.newFixedThreadPool(5)) {
            for (var index = 0; index < 5; index++) {
                connections.add(dataSource.getConnection());
                assertEquals(1, selectOne(connections.getLast(), 0));
            }
            docker.pauseContainerCmd(POSTGRES.getContainerId()).exec();
            try {
                var started = System.nanoTime();
                var reads = connections.stream()
                        .map(connection -> executor.submit(() -> assertThrows(SQLException.class, () -> selectOne(connection, 1))))
                        .toList();
                // 상한이 없으면 DB가 돌아올 때까지 기다리므로, 시험이 멈추지 않게 넉넉히 기다린 뒤 실패로 봄
                for (var read : reads) {
                    read.get(15, TimeUnit.SECONDS);
                }
                var elapsed = Duration.ofNanos(System.nanoTime() - started);

                // 소켓 읽기 상한(7초)에 끝나고, 멈춘 DB로 간 취소 요청을 기다려도 그 대기(1초)만큼만 늘어남
                assertTrue(elapsed.compareTo(Duration.ofMillis(6500)) >= 0 && elapsed.compareTo(Duration.ofSeconds(9)) < 0,
                        elapsed.toString());
            }
            finally {
                docker.unpauseContainerCmd(POSTGRES.getContainerId()).exec();
            }
        }
        finally {
            for (var connection : connections) {
                connection.close();
            }
        }
        try (var connection = dataSource.getConnection()) {
            assertEquals(1, selectOne(connection, 0));
        }
    }

    // 실제 요청 경로에서도 멈춘 DB는 Gateway의 User 라우트 응답 제한(10초) 전에 503으로 끝나고, DB가 돌아오면 다시 응답함.
    // 같은 스레드가 바로 전에 쓴 연결은 풀이 확인 없이 다시 내주므로 첫 요청은 실행 중 멈춘 조회(소켓 상한·롤백 실패)를 거치고,
    // 이어 온 요청은 쉬던 연결의 확인·새 연결이 연결 대기 상한에 막히는 경로를 거침
    @Test
    void aFrozenDatabaseAnswers503BeforeTheGatewayResponseLimit() throws Exception {
        var mvc = MockMvcBuilders.webAppContextSetup((WebApplicationContext) context).apply(springSecurity()).build();
        var docker = DockerClientFactory.instance().client();
        try (var executor = Executors.newSingleThreadExecutor()) {
            Callable<MockHttpServletResponse> terms = () ->
                    mvc.perform(request(HttpMethod.GET, "/api/v1/terms")).andReturn().getResponse();
            assertEquals(200, executor.submit(terms).get(15, TimeUnit.SECONDS).getStatus());
            docker.pauseContainerCmd(POSTGRES.getContainerId()).exec();
            try {
                for (var attempt = 0; attempt < 2; attempt++) {
                    var started = System.nanoTime();
                    // 상한이 없으면 DB가 돌아올 때까지 기다리므로, 시험이 멈추지 않게 넉넉히 기다린 뒤 실패로 봄
                    var response = executor.submit(terms).get(15, TimeUnit.SECONDS);
                    var elapsed = Duration.ofNanos(System.nanoTime() - started);

                    assertEquals(503, response.getStatus(), "attempt " + attempt);
                    assertTrue(response.getContentAsString(UTF_8).contains("COMMON_SERVICE_UNAVAILABLE"));
                    assertTrue(elapsed.compareTo(Duration.ofSeconds(9)) < 0, "attempt " + attempt + ": " + elapsed);
                }
            }
            finally {
                docker.unpauseContainerCmd(POSTGRES.getContainerId()).exec();
            }
            assertEquals(200, executor.submit(terms).get(15, TimeUnit.SECONDS).getStatus());
        }
    }

    // 실제 앱의 Hikari 연결·기본 관리자 실행 기한(5초)을 사용. 다른 연결의 대상 행 잠금은 개별 잠금 상한(2초)에 503이 되고,
    // 실패한 트랜잭션이 먼저 잡았던 관리자 행 잠금도 풀며 대상 상태·세션 버전과 연결의 정상 사용을 보존해야 함
    @Test
    void suspendingALockedUserReturns503AtTheAppPoolLockLimitAndRollsBack() throws Exception {
        var dataSource = context.getBean(DataSource.class);
        assertInstanceOf(HikariDataSource.class, dataSource);
        var jdbc = new JdbcTemplate(dataSource);
        var actorId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        var targetId = UUID.fromString("00000000-0000-0000-0000-000000000002");
        var mvc = MockMvcBuilders.webAppContextSetup((WebApplicationContext) context).apply(springSecurity()).build();
        var admin = UsernamePasswordAuthenticationToken.authenticated(
                new UserSessionPrincipal(actorId, AuthRole.MASTER, Instant.now(), 0), null,
                List.of(new SimpleGrantedAuthority("ROLE_MASTER")));
        var insert = """
                insert into user_service.users (id, real_name, status, role, phone, phone_verified_at, created_at, updated_at)
                values (?, '잠금 시험', 'ACTIVE', ?, ?, now(), now(), now())
                """;
        jdbc.update(insert, actorId, "MASTER", "010-9999-0001");
        jdbc.update(insert, targetId, "USER", "010-9999-0002");
        try (var lock = dataSource.getConnection(); var executor = Executors.newSingleThreadExecutor()) {
            lock.setAutoCommit(false);
            try (var statement = lock.prepareStatement("select id from user_service.users where id = ? for update")) {
                statement.setObject(1, targetId);
                statement.executeQuery().close();
            }
            try {
                executor.submit(() -> {
                    long backend;
                    try (var connection = dataSource.getConnection()) {
                        backend = backendPid(connection);
                    }
                    var started = System.nanoTime();
                    var result = mvc.perform(request(HttpMethod.PATCH, "/api/v1/admin/users/" + targetId + "/status")
                            .with(authentication(admin)).with(csrf()).contentType("application/json")
                            .content("{\"status\":\"SUSPENDED\"}")).andReturn();
                    var elapsed = Duration.ofNanos(System.nanoTime() - started);

                    assertEquals(503, result.getResponse().getStatus());
                    assertTrue(result.getResponse().getContentAsString(UTF_8).contains("COMMON_SERVICE_UNAVAILABLE"));
                    assertEquals("55P03", sqlState(result.getResolvedException()));
                    assertTrue(elapsed.compareTo(Duration.ofMillis(1800)) >= 0
                            && elapsed.compareTo(Duration.ofMillis(3500)) < 0, elapsed.toString());
                    // 같은 스레드가 다시 받은 물리 연결도 유지되어, 오류 뒤 새 문장을 정상 실행해야 함
                    try (var connection = dataSource.getConnection()) {
                        assertEquals(backend, backendPid(connection));
                        assertEquals(1, selectOne(connection, 0));
                    }
                    return null;
                }).get(10, TimeUnit.SECONDS);
                assertEquals("ACTIVE", jdbc.queryForObject(
                        "select status from user_service.users where id = ?", String.class, targetId));
                assertEquals(0L, jdbc.queryForObject(
                        "select session_version from user_service.users where id = ?", Long.class, targetId));
                // 저장소가 ID 순서로 먼저 잠근 관리자 행도 실패한 트랜잭션의 롤백으로 즉시 다시 잠글 수 있어야 함
                try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                        "select id from user_service.users where id = ? for update nowait")) {
                    statement.setObject(1, actorId);
                    statement.executeQuery().close();
                }
            }
            finally {
                lock.rollback();
            }
        }
        finally {
            jdbc.update("delete from user_service.users where id in (?, ?)", actorId, targetId);
        }
    }

    // JPA의 400ms hint가 Hibernate에서 0초(없음)로 반올림되는 상태를 즉시 만듦. 남은 TX 시간이 0.5초 미만인 질의와 같은
    // 제한 없음 경계에서 DB lock_timeout(2초)이 잠금 대기를 끝내야 함. 실제 1.6초 선행 작업이나 프로덕션 테스트 훅은 필요 없음
    @Test
    void aSubsecondQueryHintRoundedToZeroStillGivesUpOnALockAtTheLockLimit() throws Exception {
        var dataSource = context.getBean(DataSource.class);
        var entityManager = SharedEntityManagerCreator.createSharedEntityManager(
                context.getBean(EntityManagerFactory.class));
        var reads = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        reads.setReadOnly(true);
        reads.setTimeout(2);
        try (var lock = dataSource.getConnection()) {
            lock.setAutoCommit(false);
            try (var statement = lock.createStatement()) {
                statement.execute("lock table user_service.terms_documents in access exclusive mode");
            }
            try {
                var queryStarted = new long[1];
                var failure = assertThrows(RuntimeException.class, () -> reads.executeWithoutResult(status -> {
                    var query = entityManager.createQuery("select count(t) from TermsDocument t")
                            .setHint("jakarta.persistence.query.timeout", 400);
                    assertEquals(0, query.unwrap(org.hibernate.query.Query.class).getTimeout());
                    queryStarted[0] = System.nanoTime();
                    query.getSingleResult();
                }));
                var waited = Duration.ofNanos(System.nanoTime() - queryStarted[0]);

                assertEquals("55P03", sqlState(failure), failure.toString());
                assertTrue(waited.compareTo(Duration.ofMillis(1800)) >= 0
                        && waited.compareTo(Duration.ofSeconds(3)) < 0, waited.toString());
            }
            finally {
                lock.rollback();
            }
        }
    }

    private static long backendPid(Connection connection) throws SQLException {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("select pg_backend_pid()")) {
            result.next();
            return result.getLong(1);
        }
    }

    private static String sqlState(Throwable failure) {
        for (var current = failure; current != null; current = current.getCause()) {
            if (current instanceof SQLException sqlException) {
                return sqlException.getSQLState();
            }
        }
        return null;
    }

    private static int selectOne(Connection connection, int queryTimeoutSeconds) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(queryTimeoutSeconds);
            try (var result = statement.executeQuery("select 1")) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    // 로그인 세션이 없으면 CSRF를 확인하지 않으므로, 로그인 없이 상태를 바꾸는 API는 SecurityConfiguration의 공개 목록에
    // 있어야 CSRF를 확인합니다. 실제 컨트롤러의 상태 변경 API 전체를 세션·CSRF 없이 불러, 목록 밖의 API가 로그인 없이 열리지 않았는지 봅니다.
    @Test
    void everyStateChangingApiNeedsALoginUnlessItIsAPublicApiThatChecksCsrf() throws Exception {
        var mvc = MockMvcBuilders.webAppContextSetup((WebApplicationContext) context).apply(springSecurity()).build();
        var mappings = context.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);
        var checked = new ArrayList<String>();
        for (var mapping : mappings.getHandlerMethods().keySet()) {
            // 메서드를 정하지 않은 매핑은 모든 메서드를 받으므로 상태 변경 메서드 전체로 봅니다.
            var methods = mapping.getMethodsCondition().getMethods().isEmpty()
                    ? STATE_CHANGING : mapping.getMethodsCondition().getMethods();
            for (var method : methods) {
                if (!STATE_CHANGING.contains(method)) {
                    continue;
                }
                for (var pattern : mapping.getPathPatternsCondition().getPatternValues()) {
                    var api = method + " " + pattern;
                    var path = pattern.replaceAll("\\{[^}]+}", UUID.randomUUID().toString());
                    // 세션 확인 API는 원래 메서드 헤더가 없으면 인증 전에 400이므로 함께 보냅니다(다른 API는 쓰지 않음).
                    var response = mvc.perform(request(HttpMethod.valueOf(method.name()), path)
                            .header(AuthHeaders.ORIGINAL_METHOD, "POST")).andReturn().getResponse();
                    var publicApi = PUBLIC_MUTATIONS.contains(api);
                    assertEquals(publicApi ? 403 : 401, response.getStatus(), api);
                    assertTrue(response.getContentAsString(UTF_8).contains(
                            publicApi ? "COMMON_CSRF_INVALID" : "COMMON_UNAUTHORIZED"), api);
                    checked.add(api);
                }
            }
        }
        // 대상을 하나도 찾지 못하면 이 시험은 아무것도 보지 않으므로, 아는 API가 포함됐는지 먼저 봅니다.
        assertThat(checked).contains("POST /api/v1/users", "POST /api/v1/auth/login", "POST /api/v1/auth/logout",
                "PATCH /api/v1/users/me/password", "POST /internal/v1/auth/session");
    }
}
