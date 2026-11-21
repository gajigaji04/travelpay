package com.travelpay.user.common.security;

import java.util.Arrays;

import com.travelpay.common.auth.AuthHeaders;
import com.travelpay.user.application.service.CurrentMemberService;
import com.travelpay.user.common.security.access.CurrentMemberAuthorizationManager;
import com.travelpay.user.common.security.access.OriginalMethod;
import com.travelpay.user.common.security.access.OriginalMethodHeaderFilter;
import com.travelpay.user.common.security.access.RedisSessionErrorResponseFilter;
import com.travelpay.user.common.security.access.RestAccessDeniedHandler;
import com.travelpay.user.common.security.access.RestAuthenticationEntryPoint;
import com.travelpay.user.common.security.access.SecurityErrorResponseWriter;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.context.DelegatingSecurityContextRepository;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.context.RequestAttributeSecurityContextRepository;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.security.web.csrf.CsrfFilter;
import org.springframework.security.web.csrf.CsrfTokenRepository;
import org.springframework.security.web.csrf.HttpSessionCsrfTokenRepository;
import org.springframework.security.web.savedrequest.NullRequestCache;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.session.web.http.SessionRepositoryFilter;

/** User API의 비밀번호 해시·세션 인증·접근 권한·CSRF 및 보안 오류 응답 규칙입니다. */
@Configuration(proxyBeanMethods = false)
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfiguration {

    private static final String LOGOUT_PATH = "/api/v1/auth/logout";
    private static final String SESSION_VERIFICATION_PATH = "/internal/v1/auth/session";
    // 로그인 없이 쓰는 상태 변경 API. 인가와 CSRF 규칙이 함께 쓰며, 여기 있어야 로그인 세션 없이도 CSRF를 확인함
    private static final String[] PUBLIC_MUTATION_PATHS = {"/api/v1/users", "/api/v1/auth/login"};

    private final RestAuthenticationEntryPoint authenticationEntryPoint;
    private final RestAccessDeniedHandler accessDeniedHandler;

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http, CsrfTokenRepository csrfTokenRepository,
            SecurityContextRepository securityContextRepository, CurrentMemberService currentMemberService,
            SecurityErrorResponseWriter errorResponseWriter) throws Exception {
        http
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .requestCache(cache -> cache.requestCache(new NullRequestCache()))
                // 명세 6-5의 인증·개인정보 응답 no-store를 컨트롤러마다 두지 않고 모든 응답에 적용합니다.
                .headers(headers -> headers.cacheControl(Customizer.withDefaults()))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED))
                .securityContext(context -> context.securityContextRepository(securityContextRepository))
                .exceptionHandling(errors -> errors
                        .authenticationEntryPoint(authenticationEntryPoint)
                        .accessDeniedHandler(accessDeniedHandler))
                .csrf(csrf -> csrf.csrfTokenRepository(csrfTokenRepository)
                        .requireCsrfProtectionMatcher(csrfProtectedRequests()))
                .addFilterBefore(new OriginalMethodHeaderFilter(sessionVerificationRequests(), errorResponseWriter),
                        CsrfFilter.class)
                .authorizeHttpRequests(requests -> requests
                        .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/v1/auth/csrf", "/api/v1/terms").permitAll()
                        .requestMatchers(HttpMethod.GET, "/actuator/health", "/actuator/health/**",
                                "/actuator/prometheus").permitAll()
                        // 로컬 API 문서(Swagger). 배포 설정은 springdoc을 꺼서 이 주소들이 없음(404)
                        .requestMatchers(HttpMethod.GET, "/v3/api-docs/**", "/swagger-ui.html", "/swagger-ui/**")
                        .permitAll()
                        .requestMatchers(HttpMethod.POST, PUBLIC_MUTATION_PATHS).permitAll()
                        // 팀 결정(2026-10-31): 서비스 간 호출은 서비스 인증 없이 내부망 격리로 보호하므로 세션 없이 엽니다.
                        // 외부 차단은 Gateway·배포 망이 맡고, 회원 헤더 검사·충전 예금주 본인 확인·수취인 실명 조회 횟수 제한은 컨트롤러와 응용 서비스가 맡습니다.
                        .requestMatchers(HttpMethod.GET, "/internal/v1/users/{userId}/real-name",
                                "/internal/v1/users/{userId}/funding-identity", "/internal/v1/users/display-names").permitAll()
                        .requestMatchers(HttpMethod.POST, LOGOUT_PATH)
                        .access(CurrentMemberAuthorizationManager.sessionMembers(currentMemberService))
                        .requestMatchers("/api/v1/admin/**")
                        .access(CurrentMemberAuthorizationManager.masters(currentMemberService))
                        // 그 밖의 요청(세션 확인 API 포함)은 지금 DB 기준 ACTIVE 회원만 통과
                        .anyRequest().access(CurrentMemberAuthorizationManager.activeMembers(currentMemberService)));
        return http.build();
    }

    @Bean
    FilterRegistrationBean<RedisSessionErrorResponseFilter> redisSessionErrorResponseFilter(
            SecurityErrorResponseWriter errorResponseWriter) {
        var registration = new FilterRegistrationBean<>(new RedisSessionErrorResponseFilter(errorResponseWriter));
        registration.setOrder(SessionRepositoryFilter.DEFAULT_ORDER - 1);
        registration.setDispatcherTypes(DispatcherType.REQUEST);
        registration.addUrlPatterns("/*");
        return registration;
    }

    @Bean
    SecurityContextRepository securityContextRepository() {
        return new DelegatingSecurityContextRepository(
                new RequestAttributeSecurityContextRepository(), new HttpSessionSecurityContextRepository());
    }

    @Bean
    HttpSessionCsrfTokenRepository csrfTokenRepository() {
        var repository = new HttpSessionCsrfTokenRepository();
        repository.setHeaderName(AuthHeaders.CSRF_TOKEN);
        return repository;
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new ValidatingPasswordEncoder(PasswordEncoderFactories.createDelegatingPasswordEncoder());
    }

    // CSRF를 확인할 요청을 고름. 공개 가입·로그인은 항상 확인하고, 아래는 세션이 없으면 확인하지 않고 인가 단계의 401로 넘김(빈 세션도 안 만듦)
    // - 로그아웃: 세션이 있을 때만
    // - 세션 확인 API: 원래 요청이 상태 변경 메소드이고 로그인 세션이 있을 때만. 헤더를 못 읽으면 확인하는 쪽
    // - 그 밖의 상태 변경 요청: 로그인 세션이 있을 때만(Gateway가 세션 확인 API 없이 넘기는 User 요청도 같은 결과)
    private static RequestMatcher csrfProtectedRequests() {
        var logout = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.POST, LOGOUT_PATH);
        var sessionVerification = sessionVerificationRequests();
        var publicMutations = publicMutationRequests();
        return request -> {
            if (sessionVerification.matches(request)) {
                return OriginalMethod.from(request).map(OriginalMethod::changesState).orElse(true)
                        && hasLoginSession(request);
            }
            if (!CsrfFilter.DEFAULT_CSRF_MATCHER.matches(request)) {
                return false;
            }
            if (publicMutations.matches(request)) {
                return true;
            }
            if (logout.matches(request)) {
                return request.getSession(false) != null;
            }
            return hasLoginSession(request);
        };
    }

    // 로그인 전 CSRF 조회로 생긴 세션은 로그인 정보(SecurityContext)가 없으므로 로그인 세션이 아님
    private static boolean hasLoginSession(HttpServletRequest request) {
        var session = request.getSession(false);
        return session != null
                && session.getAttribute(HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY) != null;
    }

    private static RequestMatcher sessionVerificationRequests() {
        return PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.POST, SESSION_VERIFICATION_PATH);
    }

    private static RequestMatcher publicMutationRequests() {
        var matchers = Arrays.stream(PUBLIC_MUTATION_PATHS)
                .<RequestMatcher>map(path -> PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.POST, path))
                .toList();
        return new OrRequestMatcher(matchers);
    }
}
