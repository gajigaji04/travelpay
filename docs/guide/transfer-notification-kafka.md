# 송금 완료 알림 Kafka 로컬 실행·장애 대응 가이드

송금 완료 이벤트가 payment-service에서 user-service까지 가는 길을 로컬에서 띄우고, 장애 상황을 재현해 확인하는 방법입니다(2026-11-21, 포크).
이벤트 계약·발행기 동작은 [송금 가이드](payment-transfer.md#송금-완료-이벤트-kafka)에, 소비자·Slack·화면 알림 동작은 [User 가이드](user-service.md#송금-완료-알림-kafka--slack)에 있습니다. 이 문서는 둘을 이어서 돌리고 고장 내 보는 절차만 다룹니다.

> **확인 범위**: 장애 시나리오의 동작은 대부분 자동 테스트(Testcontainers PostgreSQL·Kafka)로 확인했습니다. 로컬에서 서비스를 띄워 직접 확인한 것은 정상 흐름(2026-11-20)과 소비자를 늦게 켠 경우뿐입니다. 각 시나리오의 **근거** 칸에 어느 쪽인지 적었습니다. "예상"이라고 적은 것은 코드와 설정으로 추론한 동작이며 아직 아무도 확인하지 않았습니다.

## 흐름

```
송금 COMPLETED ──(같은 트랜잭션)──▶ transfer_outbox (PENDING)
                                        │ 발행기 1초마다, SKIP LOCKED
                                        ▼
                          Kafka payment.transfer.completed (키 transferId, 헤더 eventId)
                                        │ 컨슈머 그룹 user-transfer-notification
                                        ▼
            user-service: transfer_notifications(eventId 중복 거르기) + user_notifications(화면 알림) → Slack
                                        │ 형식 오류 / 재시도 2번 소진
                                        ▼
                          Kafka payment.transfer.completed.DLT  ──(운영자 확인 후 수동 재투입)──▶ 원 토픽
```

| 저장 위치 | 보관 | 정리 주체 |
|---|---|---|
| `payment_service.transfer_outbox` `PUBLISHED` | 발행 후 7일 | payment `TransferOutboxCleanupScheduler`(1시간마다) |
| `payment_service.transfer_outbox` `PENDING` | 발행될 때까지 | 지우지 않음 |
| 토픽 `payment.transfer.completed` | Kafka 기본 7일 | Kafka |
| 토픽 `payment.transfer.completed.DLT` | 30일 | Kafka |
| `user_service.transfer_notifications` | 받은 뒤 30일(알림 여부 무관) | user `NotificationRetentionScheduler`(1시간마다) |
| `user_service.user_notifications` | 읽은 것만 발생 후 90일 | 같음 |

## 로컬 실행

### 1. 인프라

```bash
docker compose up -d postgres redis-session kafka kafka-ui
docker compose ps        # kafka가 healthy인지 확인
```

- 브로커는 KRaft 1대이고 **토픽 자동 생성이 꺼져 있습니다**(`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`). 원 토픽은 payment-service가(발행기가 켜져 있을 때), DLT는 user-service가(소비자가 켜져 있을 때) 기동하면서 만듭니다. 처음 띄우는 환경(Kafka 데이터 볼륨이 새것)에서 user를 먼저 띄우면 원 토픽이 아직 없어 소비자가 파티션을 받지 못하고 경고를 남기다가, payment가 토픽을 만든 뒤 메타데이터 갱신(기본 최대 5분) 때 붙습니다(**예상**, 미확인). README 순서(User → … → Payment)대로 띄워도 되지만 첫 알림이 늦을 수 있으니, 처음 한 번은 payment를 먼저 띄우거나 띄운 뒤 user를 재시작합니다. 토픽이 한 번 생기면 `kafka-data` 볼륨에 남아 순서와 관계없습니다.
- 접속 주소: 앱(호스트) `localhost:9092`, 컨테이너 안 `localhost:29092`, Kafka UI `http://localhost:8989`.
- **다른 프로젝트가 포트를 쓰고 있을 때**: `.env`에서 `POSTGRES_PORT`(5432)·`KAFKA_PORT`(9092)·`KAFKA_UI_PORT`(8989)를 바꿉니다. `*_DB_URL`은 `.env` 안에서 `${POSTGRES_PORT}`를 쓰므로 `source .env`로 함께 바뀌고, Kafka를 바꿨으면 `KAFKA_BOOTSTRAP_SERVERS=localhost:<포트>`도 넣습니다. Gateway 8080이 겹치면 `--server.port=18088`처럼 바꿉니다(18080은 관리 포트라 안 됨, [로컬 설정 가이드](local-settings.md)).
- `KAFKA_PORT`를 바꾸면 컨테이너가 알려 주는 주소(`PLAINTEXT_HOST://localhost:${KAFKA_PORT}`)도 함께 바뀌므로 compose를 다시 올립니다(`docker compose up -d kafka`). 컨테이너 안 주소 `localhost:29092`는 그대로입니다.

### 2. 앱

필요한 서비스: Eureka, ledger, wallet, user, payment (송금을 끝까지 보내려면 전부). 각 터미널에서:

```bash
set -a; source .env; set +a
export SLACK_WEBHOOK_URL='<Slack Incoming Webhook 주소>'   # user-service만. 없으면 기록·화면 알림만 하고 Slack은 보내지 않음
./gradlew :payment-service:bootRun      # 처음에는 먼저 (원 토픽 생성)
./gradlew :user-service:bootRun
```

| 설정 | 로컬 기본 | 끄는 경우 |
|---|---|---|
| `transfer.outbox.publisher.enabled` (payment) | `true` | Kafka 없이 띄울 때. 행은 `PENDING`으로 쌓임 |
| `transfer.outbox.monitor.enabled`·`cleanup.enabled` (payment) | `true` | 테스트에서만 끔 |
| `travelpay.user.transfer-notification.consumer-enabled` (user) | `true` | Kafka 없이 띄울 때. 이벤트는 토픽에 남음 |
| `travelpay.user.notification-retention.enabled` (user) | `true` | 테스트에서만 끔 |

끌 때는 실행 인자로 넘깁니다. 예: `./gradlew :payment-service:bootRun --args='--transfer.outbox.publisher.enabled=false'`.

### 3. 정상 흐름 확인

송금 하나를 `COMPLETED`까지 보낸 뒤(송금 가이드 [호출 순서](payment-transfer.md#호출-순서)) 아래를 봅니다. Git Bash에서는 `docker compose exec` 경로가 바뀌지 않게 먼저 `export MSYS_NO_PATHCONV=1`을 합니다.

```bash
# Outbox: PUBLISHED, published_at이 생성 후 1~2초 안
docker compose exec postgres psql -U travelpay_admin -d travelpay -c \
  "select id, transfer_id, status, retry_count, created_at, published_at from payment_service.transfer_outbox order by created_at desc limit 5"

# 토픽 메시지(키·헤더 포함)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic payment.transfer.completed --from-beginning --timeout-ms 8000 \
  --property print.key=true --property print.headers=true --property print.partition=true

# 소비 위치: LAG 0
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
  --describe --group user-transfer-notification

# 수신 기록·화면 알림
docker compose exec postgres psql -U travelpay_admin -d travelpay -c \
  "select event_id, transfer_id, received_at, notified_at from user_service.transfer_notifications order by received_at desc limit 5"
docker compose exec postgres psql -U travelpay_admin -d travelpay -c \
  "select user_id, type, transfer_id, read_at from user_service.user_notifications order by created_at desc limit 5"

# 지표
curl -s localhost:8083/actuator/prometheus | grep -E '^transfer_outbox_pending'
curl -s localhost:8081/actuator/prometheus | grep -E '^transfer_notification_dead_letter_total'
```

- 콘솔 컨슈머 끝의 `Error processing message, terminating consumer process`는 `--timeout-ms`가 지나 끝난다는 뜻이라 무시합니다.
- `psql` 계정은 `.env`의 `POSTGRES_USER`·`POSTGRES_DB`입니다(위는 `.env.example` 값).
- DLT 카운터는 한 번도 보내지 않았으면 지표 줄이 없거나 0입니다(`kind`별로 기동 때 0으로 등록).
- 정상이면 Grafana(`compose.monitoring.yaml`) **비동기 처리** 행의 미발행 건수·나이는 0 근처, DLT 패널은 비어 있습니다.

2026-11-20 로컬 확인 기록: [송금 가이드](payment-transfer.md#로컬-수동-확인-2026-11-20)(토픽 값·키·헤더·Outbox 1.4초 발행), [User 가이드](user-service.md#로컬-수동-확인-2026-11-20)(수신·Slack·lag 0).

## 장애 시나리오

| # | 상황 | 기대 동작 | 알아채는 곳 | 근거 |
|---|---|---|---|---|
| S1 | 운영 중 Kafka 정지 | 송금은 그대로 `COMPLETED`. Outbox는 `PENDING`으로 남고 주기마다 `retry_count` +1. Kafka가 돌아오면 쌓인 건을 같은 `eventId`로 발행 | payment WARN `송금 완료 이벤트 발행 실패`, `transfer_outbox_pending_oldest_age_seconds` 증가, 5분 넘으면 경보 | 테스트 `TransferOutboxPublisherIntegrationTest`(Kafka 멈춤 중 완료·행 유지·복구 후 발행). 지표·경보는 `TransferOutboxMaintenanceIntegrationTest`(값만, 경보 발동은 미확인) |
| S2 | Kafka 없이 payment 기동 | 기동은 되지만 토픽 선언이 실패해 늦어지고(관리 클라이언트 대기), 발행 실패 WARN이 1초마다 남음 | 기동 로그, S1과 같음 | **예상**(미확인). 피하려면 `transfer.outbox.publisher.enabled=false` |
| S3 | Kafka 없이 user 기동(소비자 켬) | 앱은 뜨고 소비자는 브로커 연결을 계속 다시 시도. DLT 토픽 선언도 실패해 기동이 늦어짐. Kafka가 오면 저장된 오프셋부터 읽음 | 연결 실패 WARN 반복 | **예상**(미확인). 피하려면 `consumer-enabled=false` |
| S4 | user-service 정지 중 송금 | payment는 정상 발행. 이벤트는 토픽에 남고 user가 다시 뜨면 커밋한 오프셋 다음부터 처리 | 컨슈머 그룹 LAG > 0, 대시보드 `Kafka 소비 지연`은 소비자가 떠 있어야 나옴 | Kafka 기본 동작. 그룹이 **처음** 붙는 경우는 로컬 확인(2026-11-20, `earliest`로 28분 전 이벤트 처리) |
| S5 | Slack 실패(5xx·연결 실패) | 1초 간격 2번 재시도 후 DLT(`failureKind=exhausted`)로 보내고 다음 메시지로. 수신 기록은 `notified_at` null로, 화면 알림은 정상 생성 | user ERROR `송금 완료 알림 처리 포기, DLT로 보냄`, `transfer_notification_dead_letter_total{kind="exhausted"}`, 경보 `송금 완료 알림 DLT 적재` | 테스트 `TransferCompletedConsumerIntegrationTest`(재시도 3번 전송, DLT 헤더, 재투입 후 전송) |
| S6 | 형식이 잘못된 이벤트 | 재시도 없이 DLT(`invalid`), 뒤 메시지는 계속 처리 | S5와 같고 `kind="invalid"` | 같은 테스트(헤더 없음·JSON 깨짐·금액 0) |
| S7 | 같은 이벤트 중복 전달 | 화면 알림·Slack 모두 한 번 | user INFO `이미 알린 송금 완료 이벤트라 건너뜀` | 같은 테스트, `TransferNotificationServiceTest` |
| S8 | 발행은 됐는데 Outbox 상태 변경 실패 | 롤백되어 `PENDING`으로 남고 같은 `eventId`로 다시 발행 → S7로 걸러짐 | `retry_count`는 오르지 않음 | `TransferOutboxPublisherIntegrationTest` |
| S9 | DLT로 보내는 중 Kafka 정지 | 그 메시지를 넘기지 않고 처음부터 다시 처리(Slack 재시도 포함). Kafka가 오면 DLT로 보내고 진행. 그동안 같은 파티션 뒤 메시지는 대기 | ERROR가 반복, LAG 증가 | **예상**(Spring Kafka `DefaultErrorHandler` 동작, 미확인) |
| S10 | user DB 장애 | 수신 기록 트랜잭션이 실패해 S5처럼 재시도 후 DLT(`exhausted`). DB가 돌아온 뒤 재투입하면 처리 | S5와 같음 | **예상**(DB 실패를 넣은 테스트 없음) |
| S11 | 소비자를 오래 끈 뒤 켬(운영 첫 적용 포함) | 토픽에 남은 이벤트(최대 7일)를 한꺼번에 처리해 늦은 알림이 나감 | 대량 INFO 로그, Slack 몰림 | 로컬 확인(2026-11-20) |
| S12 | payment 여러 대 | `SKIP LOCKED`로 서로 다른 행을 발행, 정리·지표도 겹쳐도 같은 결과 | - | 테스트(발행기 2개, 잠긴 행 건너뛰는 정리) |

### 재현 절차

모두 [로컬 실행](#로컬-실행)을 마친 상태에서 합니다. 아래 절차는 2026-11-21에 정리했고 **로컬에서 직접 돌려 보지 않았습니다**. 돌려 본 뒤 결과를 [확인 기록](#확인-기록)에 남깁니다.

**S1 Kafka 정지 중 송금**

```bash
docker compose stop kafka
# 송금 1건 COMPLETED → 응답은 평소와 같아야 함
docker compose exec postgres psql -U travelpay_admin -d travelpay -c \
  "select status, retry_count, created_at from payment_service.transfer_outbox where status = 'PENDING'"
curl -s localhost:8083/actuator/prometheus | grep transfer_outbox_pending   # 30초마다 갱신
docker compose start kafka
# 수십 초 안에 PUBLISHED로 바뀌고, user-service가 알림 처리
```

- 확인할 것: 송금 응답 시간이 평소와 같은지, `retry_count`가 주기마다 오르는지, 5분 넘게 두면 Grafana 경보 `송금 완료 이벤트 5분 넘게 미발행`이 Firing인지, 복구 뒤 Normal로 돌아오는지.
- Kafka를 멈추면 user-service 소비자도 연결 실패 로그를 냅니다(S3과 같음).

**S5 Slack 실패 → DLT → 재투입**

```bash
# 열려 있지 않은 로컬 포트로 Webhook을 돌려 연결 실패를 만듭니다.
export SLACK_WEBHOOK_URL='http://127.0.0.1:9/fake-hook'
./gradlew :user-service:bootRun
# 송금 1건 COMPLETED → 약 2초 뒤 ERROR 로그와 DLT 적재
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic payment.transfer.completed.DLT --from-beginning --timeout-ms 8000 \
  --property print.key=true --property print.headers=true --property print.offset=true
```

- DLT 헤더에 `eventId`, `failureKind:exhausted`, `failureReason:…reason=ResourceAccessException` 같은 값이 보여야 합니다. `kafka_dlt-original-partition`·`-offset`은 숫자를 바이너리로 담아 콘솔에서 깨져 보입니다(정상). `kafka_dlt-exception-message`·`-stacktrace`는 없어야 합니다.
- `transfer_notifications`의 그 `event_id` 행은 `notified_at`이 비어 있고, 화면 알림은 두 행 생겨 있어야 합니다.
- 재투입: user-service를 진짜 Webhook(또는 Webhook 없이)으로 다시 띄운 뒤, DLT에서 본 키·값·`eventId`를 원 토픽에 넣습니다.

```bash
EVENT_ID='<DLT 헤더의 eventId>'
KEY='<DLT 메시지 키(transferId)>'
VALUE='<DLT 메시지 값 JSON 한 줄>'
printf 'eventId:%s\t%s\t%s\n' "$EVENT_ID" "$KEY" "$VALUE" | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 \
  --topic payment.transfer.completed --property parse.key=true --property parse.headers=true
```

- 넣은 뒤 `notified_at`이 채워지면 끝입니다. 이미 알린 이벤트를 넣어도 `eventId`로 걸러져 다시 보내지 않으므로, 여러 번 넣어도 안전합니다.
- 헤더·키·값 구분자는 탭입니다(`printf`의 `\t`). 값 JSON에 탭이 없어야 합니다(발행 값은 한 줄 JSON).
- Webhook 없이 다시 띄우면 Slack 없이 기록만 하므로 `notified_at`은 계속 비어 있습니다. 재투입 확인에는 실제 Webhook이 필요합니다.

**S6 형식 오류 이벤트**

```bash
printf 'eventId:%s\t%s\t%s\n' '00000000-0000-4000-8000-000000000001' 'manual-invalid' '{not json' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 \
    --topic payment.transfer.completed --property parse.key=true --property parse.headers=true
```

- 바로(재시도 없이) DLT에 `failureKind:invalid`, `failureReason:…reason=값 JSON 형식 오류`로 들어가고, 이어서 보낸 정상 송금은 평소대로 처리돼야 합니다.
- `invalid` 메시지는 재투입하지 않습니다. 다시 넣어도 같은 이유로 DLT로 돌아옵니다. 발행 쪽 계약(송금 가이드 [이벤트 계약](payment-transfer.md#이벤트-계약))을 확인합니다.

**S4·S11 소비자 정지·지연 시작**

```bash
# user-service를 끈 채 송금 2~3건 COMPLETED
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
  --describe --group user-transfer-notification      # LAG = 끈 동안 쌓인 건수
# user-service 다시 기동 → LAG 0, 쌓인 건 모두 알림
```

**S2·S3 Kafka 없이 기동**

```bash
docker compose stop kafka
./gradlew :payment-service:bootRun      # 기동 시간·로그 확인 후, 끄는 설정으로도 한 번
./gradlew :user-service:bootRun
```

- 확인할 것: 기동이 끝나는지와 걸린 시간, 반복되는 로그의 양과 간격, 끄는 설정(`publisher.enabled=false`·`consumer-enabled=false`)이면 Kafka 로그가 전혀 없는지.

## DLT 운영

DLT 메시지 구성과 헤더는 [User 가이드의 송금 완료 알림 DLT](user-service.md#송금-완료-알림-dlt)에 있습니다. 운영 절차만 정리합니다.

1. 경보 `송금 완료 알림 DLT 적재` 또는 user ERROR 로그 `송금 완료 알림 처리 포기`로 알아챕니다. 로그에 원본 파티션·오프셋·`transferId`·`kind`가 남습니다.
2. DLT를 읽어 `failureKind`별로 나눕니다(위 S5의 콘솔 컨슈머). 어디까지 봤는지 오프셋을 적어 둡니다. DLT는 처리해도 지우지 않고 30일 뒤 Kafka가 지웁니다.
3. `exhausted`: 원인(Slack·DB)을 해결한 뒤 키·값·`eventId`를 원 토픽에 다시 넣습니다(S5 재투입). 중복은 `eventId`로 걸러집니다. 수신 기록 보관(30일)과 DLT 보관(30일)이 같으므로 30일 안에 처리합니다.
4. `invalid`: 재투입하지 않고 payment 발행 계약 위반을 확인합니다. 원인을 고친 뒤 필요하면 올바른 값으로 새로 넣습니다(같은 `eventId`를 쓰면 중복 거르기에 걸리지 않도록 처음 받은 적이 없는지 확인).

## 운영 적용 전 확인할 것

- 운영 설정은 payment 발행기와 user 소비자가 모두 꺼져 있습니다(`application-prod.yml`). 운영 Kafka 주소(`KAFKA_BOOTSTRAP_SERVERS`)를 인프라와 정한 뒤 켭니다.
- 발행기를 켜면 그동안 쌓인 `PENDING`이 한꺼번에 발행되고, 소비자를 처음 켜면 토픽 처음부터 읽습니다(S11). 오래된 알림을 보내지 않으려면 켜기 전에 `PENDING` 행을 정리할지 정합니다.
- 토픽 복제 수는 로컬 브로커 1대에 맞춰 1입니다(원 토픽·DLT 모두). 운영 값은 인프라와 정합니다.
- Grafana 경보는 로컬 프로비저닝(`prometheus-local`·`spring-jvm`)입니다. 운영에서는 데이터소스·수집 대상을 바꾸고, 미발행 나이 경보는 발행기를 켠 뒤에 옮깁니다(꺼져 있는 동안은 계속 Firing).

## 확인 기록

| 날짜 | 범위 | 결과 |
|---|---|---|
| 2026-11-20 | 정상 흐름(발행 → 소비 → Slack), 소비자 첫 기동 시 지난 이벤트 처리(S11) | 통과. 기록은 위 두 가이드의 「로컬 수동 확인」 |
| 2026-11-21 | 자동 테스트: S1·S5·S6·S7·S8·S12, Outbox 정리·지표 | payment 49클래스 945건, user 81클래스 1,027건 통과(`--rerun`) |
| - | S1 지표·경보 발동, S2, S3, S4, S9, S10, 콘솔 재투입 | 아직 확인하지 않음 |
