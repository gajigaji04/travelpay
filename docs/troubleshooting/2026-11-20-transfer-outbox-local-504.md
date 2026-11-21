# 송금 Kafka Outbox 로컬 검증 중 504 분석

- 일시: 2026-11-20 16:37:47 (KST)
- 브랜치: `feat/353-transfer-kafka` (커밋 `8e049ae`)
- 상태: 원인 확인 완료 · 코드 수정 없음 · **재요청으로 해결(16:50:41 송금 `COMPLETED`), 소비자 수신까지 확인(17:18)**

## 요약

송신자 지갑 조회는 성공했고, 이어서 호출한 **수취인 회원 조회(User 서비스)에서 Payment의 읽기 타임아웃 1초를 넘겨 504**가 났습니다.
원장은 호출되지 않았으므로 **돈은 움직이지 않았고**, 재요청해도 안전합니다.

## 환경

| 서비스 | 포트 | 기동 시각 |
|---|---|---|
| Eureka | 8761 | - |
| User | 8081 | 16:28:35 |
| Wallet | 8082 | 16:28:54 |
| Payment | 8083 | 16:29:20 |
| Ledger | 8085 | 16:28:32 |
| Kafka UI | 8989 | - |

- 테스트 데이터: 송신자 KRW 20,000원, 수취인 KRW 80,000원, 송금액 1,000원
- 증상: 응답 504, `payment_service.transfer`에 행 없음, `payment_service.transfer_outbox` 0건
- 모든 서비스가 요청 이전에 기동했고 이후 재시작이 없어, 아래 지표는 이번 요청 구간을 모두 담고 있습니다.

## 1. 504가 난 지점

### 확인된 사실 (`/actuator/prometheus`, 로컬 로그)

| 서비스 | 지표 | 의미 |
|---|---|---|
| Payment `POST /api/v1/transfers` | 1건, **504**, 1.66초 | 이번 요청 |
| Wallet `GET /internal/v1/wallets` | **1건만**, 200, 0.13초 | 송신자 지갑 조회만 있음. 수취인 지갑 조회는 호출되지 않음 |
| User `GET /internal/v1/users/{id}/real-name` | 1건, **200**, 서버 측 0.87초 | User는 응답을 끝까지 만들었지만 Payment가 먼저 포기함 |
| Ledger `POST /internal/v1/ledger/transactions` | **0건** | 원장 거래 등록이 없었음 |
| Payment `spring_kafka_template` | 0건 | Kafka 발행 없음 |

- 로그상 Wallet과 User가 16:37:47에 0.46초 간격으로 기동 후 첫 요청을 받았습니다(`DispatcherServlet` 초기화 시각 .233 / .694).
  순서가 `TransferService.request()`의 흐름과 같습니다: 송신자 지갑(120행) → 수취인 회원(123행) → 수취인 지갑(126행).
- 실패 지점은 `TransferService.java:123`의 `userReader.findRecipient()`입니다.
  User가 200을 돌려줬는데도 504가 났으므로, 원인은 상대가 보낸 504가 아니라 Payment 쪽 `SocketTimeoutException`입니다.
  `FeignErrors.toCommonException`이 이 예외를 `COMMON_GATEWAY_TIMEOUT`(504)으로 바꿉니다.
  User가 요청을 받아 처리했으니 연결 타임아웃이 아니라 읽기 타임아웃입니다.
- Payment 로그에 아무것도 남지 않은 것은 정상입니다. 공통 `GlobalExceptionHandler`는 `BusinessException`을 로그 없이 응답만 합니다.
- `payment-service/infra/monitoring/.local-logs/payment-service.log`(16:12)는 테스트 실행(`Test worker`) 기록이라 이번 요청과 무관합니다.

### 추측 (지표로 일부만 확인)

User가 느렸던 이유는 콜드 스타트로 보입니다. 이번이 User 서비스의 첫 요청이었습니다.

- 지표로 확인된 부분은 Redis 명령(HELLO, CLIENT, EVALSHA 실패 후 EVAL) 약 60ms와 DB 커넥션 사용 98ms뿐입니다.
- 나머지 약 0.7초는 지연 초기화, 클래스 로딩, JIT 같은 첫 요청 비용으로 추정합니다.
- 서버 측 0.87초에는 Tomcat 단계 처리와 네트워크 시간이 포함되지 않습니다. 이 시간이 더해져 Payment가 체감한 시간이 1초를 넘었을 가능성이 큽니다.

## 2. 서비스 간 통신과 Feign 타임아웃 설정

`payment-service/src/main/resources/application.yml:47-59`

| contextId | 대상 | connect | read | 서비스 찾는 방식 |
|---|---|---|---|---|
| `paymentWalletApi` | wallet-service | 500ms | **1000ms** | Eureka만 사용 (url 설정 없음) |
| `paymentUserApi` | user-service | 500ms | **1000ms** | `USER_SERVICE_URL`, 비어 있으면 Eureka |
| `paymentLedgerApi` | ledger-service | 500ms | 6000ms | `LEDGER_SERVICE_URL`, 비어 있으면 Eureka |

- Eureka 등록은 모든 서비스가 `hostname: localhost`라서 주소 문제는 아닙니다. Wallet 호출이 0.13초에 성공한 것으로도 확인됩니다.
- 단계별로 응답 코드가 다릅니다.
  - 지갑·회원 조회 단계: 시간 초과면 504, 연결 실패면 503
  - 원장 단계: 실패해도 PROCESSING으로 저장하고 **202**로 응답합니다(`TransferController`).
- 그래서 "504인데 송금 행이 없다"는 조합 자체가 원장 호출 전에 실패했다는 뜻입니다.

## 3. 송금과 Outbox가 저장되지 않은 이유

설계대로 동작한 결과입니다.

- 송금 행은 모든 사전 검증을 통과한 뒤 `TransferService.java:134` `saveAndFlush`에서 처음 저장됩니다. 이번 요청은 123행에서 예외로 끝나 저장까지 가지 못했습니다.
- Outbox는 원장 결과가 COMPLETED일 때만 상태 변경과 같은 트랜잭션에서 기록됩니다(`registerOnLedger`, 381~388행). 원장 호출이 없었으니 Outbox도 생기지 않습니다.
- 남은 부작용은 User의 수취인 조회 호출 제한 카운트가 1 늘어난 것뿐입니다(한도 60). 영향은 없습니다.

## 4. 최소 수정 방안

1. **코드 수정 없이 재요청.** User는 이미 한 번 요청을 처리해 초기화가 끝났으므로 이번에는 통과할 가능성이 큽니다.
   1초는 기존에 합의된 값이라 Outbox 검증 브랜치에서 바꾸지 않습니다.
2. **다시 504가 나면, 로컬 실행 설정에서만 환경 변수로 늘리기.** 커밋하지 않습니다.

   ```
   SPRING_CLOUD_OPENFEIGN_CLIENT_CONFIG_PAYMENTUSERAPI_READ_TIMEOUT=3000
   SPRING_CLOUD_OPENFEIGN_CLIENT_CONFIG_PAYMENTWALLETAPI_READ_TIMEOUT=3000
   ```

   서비스를 재시작할 때마다 첫 요청에서 같은 일이 생길 수 있습니다. 재시작했다면 송금 전에 수취인 확인 API를 한 번 호출해 두는 방법도 있습니다.

## 5. 재검증 절차

1. 새 `Idempotency-Key`로 1,000원을 송금합니다.
   - 기대 결과: **201**, `status=COMPLETED`
   - 202(PROCESSING)가 나오면 원장 단계 문제이므로 재요청하지 말고 복구 스케줄러가 처리하게 둡니다.
2. 송금 행 확인
   ```sql
   SELECT id, status, attempt_count FROM payment_service.transfer ORDER BY created_at DESC LIMIT 1;
   ```
   → `COMPLETED`
3. Outbox 확인
   ```sql
   SELECT * FROM payment_service.transfer_outbox ORDER BY created_at DESC LIMIT 1;
   ```
   → 발행 주기가 1초라 `PENDING`에서 금방 **`PUBLISHED`**로 바뀌어야 합니다.
4. Kafka UI(`localhost:8989`)의 `payment.transfer.completed` 토픽에서 메시지 1건과 키·본문을 확인합니다.
5. Payment의 `spring_kafka_template_seconds_count{result="success"}`가 1이 됐는지 확인합니다.
6. 잔액 확인: 송신자 19,000원, 수취인 81,000원.

## 결과 (2026-11-20 갱신)

### 재요청

타임아웃 설정을 바꾸지 않고 재요청해 통과했습니다(4절 1번 방안). 1,000원 송금이 `COMPLETED`가 됐습니다.

| 확인 항목 | 결과 |
|---|---|
| 송금 | `ac625422-43d6-4e54-978e-17b7d7d81822`, 완료 16:50:41 KST |
| Outbox | `PUBLISHED`, `retry_count` 0, 기록 후 약 1.4초 뒤 발행 |
| 토픽 메시지 | 파티션 0, 키 = `transferId`, 헤더 `eventId` = Outbox 행 `id`(`d1fc7bae-…`), 값 6필드 |

5절의 잔액(6번)과 `spring_kafka_template` 지표(5번)는 이번에 기록하지 않았습니다.

### 소비자 (처음 쓸 때는 없었음 → 구현함)

처음 분석할 때는 `payment.transfer.completed`를 구독하는 `@KafkaListener`가 없었습니다(당시 Consumer는 wallet-service의 충전용 `ChargeSagaConsumer` 하나). 그래서 user-service에 소비자를 만들었습니다. 받은 이벤트를 eventId로 중복 제거하고 Slack으로 알립니다.

- 17:18 KST에 소비자를 넣은 jar로 user-service를 재시작했고, 위 이벤트를 받아 `transfer_notifications`에 기록한 뒤 Slack 전송까지 성공했습니다(`notified_at` 기록, lag 0).
- 컨슈머 그룹이 처음 붙어 토픽을 처음부터 읽었기 때문에, 알림은 송금 완료 약 28분 뒤에 나갔습니다.
- 동작과 설정: [User 가이드의 송금 완료 알림](../guide/user-service.md#송금-완료-알림-kafka--slack). 확인 기록: 같은 문서의 「로컬 수동 확인 (2026-11-20)」
