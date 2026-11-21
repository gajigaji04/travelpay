<div align="center">

<img src="docs/images/banner.png" alt="TravelPay" width="100%" />

# TravelPay

**여행자를 위한 충전·송금·환전·결제 통합 지갑 플랫폼**

</div>

<br/>

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [주요 기능](#2-주요-기능)
3. [기술 스택](#3-기술-스택)
4. [시스템 아키텍처](#4-시스템-아키텍처)
5. [팀원 및 역할](#5-팀원-및-역할)
6. [핵심 기술 구현](#6-핵심-기술-구현)
7. [성능 테스트 및 트러블슈팅](#7-성능-테스트-및-트러블슈팅)
8. [실행 방법](#8-실행-방법)

<br/>

## 1. 프로젝트 개요

TravelPay는 원화 충전, 회원 간 송금, 외화 환전, 가맹점 결제를 하나의 지갑에서 제공하는 서비스입니다.

응답이 유실되거나 거래가 동시에 몰려도 잔액과 거래 상태가 어긋나지 않도록, 모든 자금 이동을 원장에 한 번만 기록하고 결과를 모르는 거래는 다시 처리해 확정합니다. 끝내 확정하지 못한 거래는 대사와 운영자 보정으로 마무리합니다.

- **인증·보안:** Redis 세션·CSRF·로그인 제한 및 Cloudflare·Gateway 기반 보안 체계 구축
- **원화 충전:** 은행 계좌에서 출금한 만큼 지갑에 적립하고, 적립에 실패하면 계좌로 환불
- **회원 간 송금:** 수취인 정보를 확인하고 회원 간 지갑으로 간편하고 안전하게 송금
- **외화 환전·예약 환전:** 최신 환율로 받은 견적 그대로 환전하고, 목표 환율에 도달하면 자동 체결
- **가맹점 결제·환불·정산:** 매장 결제, 부분 환불, 판매 대금 정산 구현
- **거래 대사·보정:** 거래 내역을 원장 기록과 대조하고, 어긋난 거래는 관리자가 보정

### 프로젝트 목표

**목표 1. 거래 중복을 방지하는 MSA 기반 금융 거래 구조 구축**

- 회원·지갑·결제·환전·원장 서비스를 분리하고, Gateway와 Eureka로 서비스 간 요청을 연결
- 잔액의 기준을 공용 원장으로 일원화하고, 모든 자금 이동을 복식부기(차변·대변)로 기록
- 멱등성 키와 (거래 유형, 거래 ID) 유일 제약으로 중복 요청·중복 원장 기록을 방지
- 계정 잠금 순서와 DB 제약 조건으로 동시 거래의 교착과 초과 출금을 방지

**목표 2. 장애 복구와 성능·정합성 검증을 갖춘 거래 처리 구조 구축**

- 은행·원장 응답이 유실되거나 지연되면 실패로 단정하지 않고, 동일한 요청 ID와 원장 reference를 사용해 재전송하고 중복 반영을 방지
- 복구 스케줄러와 조건부 UPDATE로 중단된 거래를 재처리하고, 서킷 브레이커로 장애 확산을 줄임
- 자동 복구가 어려운 거래는 MANUAL_REVIEW로 분류해 대사와 관리자 보정으로 처리
- 응답 유실·동시 거래·원장 장애 상황에서 처리량·응답 시간·완료 지연과 중복 반영·대사 불일치를 함께 측정하고, 동일한 조건에서 개선 전후를 비교

**목표 3. 계정 탈취 위험과 비인가 거래를 줄이는 다계층 보안 체계 구축**

- Spring Security·Redis 기반 세션 인증, CSRF 검증, 세션 갱신·만료, 로그인 시도 제한으로 계정 탈취 위험과 요청 위조를 줄임
- 회원 상태·권한 검증으로 비인가 접근을 제한하고, Gateway에서 외부 회원 식별 헤더를 제거해 타인 명의 거래 위험을 줄임
- Cloudflare 비밀 헤더 기반 원본 인증과 AWS 보안 그룹 IP 제어로 ALB 직접 접근과 내부 서비스 노출을 제한

<br/>

## 2. 주요 기능

| 영역 | 범위 |
| --- | --- |
| 사용자·인증 | 회원가입·로그인·로그아웃, 회원 정보 조회·관리, 약관 조회·동의·관리, Redis 기반 세션 인증, CSRF 보호·로그인 시도 제한, 관리자 회원 검색·상태 변경·세션 강제 종료 |
| 지갑·충전 | 지갑 개설·통화별 잔액 조회, 원화 충전·실패 시 환불, 충전 대사·관리자 보정 |
| 송금 | 사용자 간 통화별 지갑 송금, 송금 내역 조회(상대방 마스킹 이름), 멱등성 기반 중복 방지 및 원장 장애 시 자동 복구, 송금 완료 알림(Kafka → 화면 알림·Slack) |
| 환전·환율 | 실시간 환율 조회와 견적 기반 환전, 목표 환율 도달 시 자동 체결되는 예약 환전 |
| 결제·환불 | 가맹점 결제, 결제 내역 조회, 부분 환불 처리 |
| 가맹점·정산 | 가맹점 생성, 매출 내역 조회, 정산 |
| 원장 | 송금·결제·환전·충전 자금 이동 기록, 계좌별 잔액 관리 |
| MockBank | 외부 은행 대체, 계좌 출금·환불 |

<br/>

## 3. 기술 스택

### Backend

![Java 21](https://img.shields.io/badge/Java%2021-007396?style=for-the-badge&logo=openjdk&logoColor=white)
![Spring Boot 4.1](https://img.shields.io/badge/Spring%20Boot%204.1-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)
![Spring Security](https://img.shields.io/badge/Spring%20Security-6DB33F?style=for-the-badge&logo=springsecurity&logoColor=white)
![Spring Data JPA](https://img.shields.io/badge/Spring%20Data%20JPA-6DB33F?style=for-the-badge&logo=spring&logoColor=white)
![Spring Cloud Gateway](https://img.shields.io/badge/Spring%20Cloud%20Gateway-6DB33F?style=for-the-badge&logo=spring&logoColor=white)
![Eureka](https://img.shields.io/badge/Eureka-6DB33F?style=for-the-badge&logo=spring&logoColor=white)
![OpenFeign](https://img.shields.io/badge/OpenFeign-6DB33F?style=for-the-badge&logo=spring&logoColor=white)
![Resilience4j](https://img.shields.io/badge/Resilience4j-1A1A1A?style=for-the-badge)

### Database · Messaging

![PostgreSQL 18](https://img.shields.io/badge/PostgreSQL%2018-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Redis 8](https://img.shields.io/badge/Redis%208-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Apache Kafka 4.2](https://img.shields.io/badge/Apache%20Kafka%204.2-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Spring Batch](https://img.shields.io/badge/Spring%20Batch-6DB33F?style=for-the-badge&logo=spring&logoColor=white)

### Infra · Deploy

![AWS ECS](https://img.shields.io/badge/AWS%20ECS-FF9900?style=for-the-badge&logo=amazonecs&logoColor=white)
![AWS RDS](https://img.shields.io/badge/AWS%20RDS-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white)
![AWS ALB](https://img.shields.io/badge/AWS%20ALB-8C4FFF?style=for-the-badge&logo=amazonwebservices&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![Cloudflare](https://img.shields.io/badge/Cloudflare-F38020?style=for-the-badge&logo=cloudflare&logoColor=white)

### Monitoring · Test

![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Loki](https://img.shields.io/badge/Loki-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Testcontainers](https://img.shields.io/badge/Testcontainers-291A3F?style=for-the-badge)
![k6](https://img.shields.io/badge/k6-7D64FF?style=for-the-badge&logo=k6&logoColor=white)

### Frontend

![React 19](https://img.shields.io/badge/React%2019-61DAFB?style=for-the-badge&logo=react&logoColor=black)
![TypeScript 5.8](https://img.shields.io/badge/TypeScript%205.8-3178C6?style=for-the-badge&logo=typescript&logoColor=white)
![Vite 7](https://img.shields.io/badge/Vite%207-646CFF?style=for-the-badge&logo=vite&logoColor=white)
![Vercel](https://img.shields.io/badge/Vercel-000000?style=for-the-badge&logo=vercel&logoColor=white)

<br/>

## 4. 시스템 아키텍처

### 인프라 설계도

<img src="docs/images/infra-simple.png" alt="인프라 설계도" width="100%" />

<details>
<summary><b>롤링 업데이트 선택 근거</b></summary>

TravelPay는 8개 MSA 서비스를 ECS on EC2에서 서비스별 Task 1개로 운영합니다.

제한된 서버 자원과 운영 환경을 고려해 롤링 업데이트를 선택했습니다. 신규 Task의 정상 상태를 확인한 뒤 기존 Task를 교체하며, 배포 실패 시 자동 롤백하도록 설정했습니다.

- `minimumHealthyPercent=100`: 배포 중에도 기존 Task를 내리지 않고 유지
- `maximumPercent=200`: 신규 Task를 기존과 함께 띄운 뒤 교체

카나리·블루그린 배포에 필요한 트래픽 분배와 추가 실행 환경 없이, ECS 기본 배포 기능으로 운영할 수 있다는 점도 고려했습니다.

</details>

### ERD

<img src="docs/images/erd.png" alt="ERD" width="100%" />

[ERD Cloud에서 보기](https://www.erdcloud.com/d/uqEkFQrcvkt94PT6A)

<br/>

## 5. 팀원 및 역할

| 이름 | 역할 | 담당 |
| --- | --- | --- |
| 박현아 | **팀장** · 지갑·충전·원장 | 프로젝트 일정 및 작업 조율, 지갑·충전, 결과 모름 처리·자동 복구, 충전 대사·보정, 공용 원장, MockBank |
| 김이찬 | 공통·인프라 | 공통 모듈·내부 통신, 회원·인증, API Gateway·요청 보안, AWS 인프라·CI/CD, 통합 모니터링 |
| 김지혜 | 프론트엔드·송금 | 프론트엔드 전체, 송금 API·상태 관리, 송금 멱등성·동시성, 송금 장애 대응 |
| 백가영 | 환율·환전 | 환율 수집·조회, 환전 견적·주문, 예약 환전, 원장 장애 격리 |
| 이안근 | 결제 | 가맹점 결제·환불·정산 |

<br/>

## 6. 핵심 기술 구현

### 01. 회원·인증·인프라 (김이찬)

[User](docs/guide/user-service.md) · [Gateway](docs/guide/gateway.md) · [AWS](docs/guide/aws-terraform.md) · [CI/CD](docs/guide/ci-cd.md) · [공통 모듈](docs/guide/common.md)

- MSA 세션 인증 체계: Spring Security·Spring Session Redis로 로그인 세션을 중앙 관리하고, Gateway가 외부 회원 헤더를 제거한 뒤 User에서 검증한 회원 정보만 내부 서비스에 전달
- 회원 권한 관리·강제 로그아웃: 요청마다 DB의 회원 상태·권한을 검증하고, 관리자 정지·강제 로그아웃 시 세션 버전을 변경해 기존 로그인 세션 일괄 무효화
- 인증 보안 강화: 유휴 60분·절대 24시간 세션 만료, 로그인·비밀번호 변경 시 세션 ID 교체 및 CSRF 토큰 재발급, Redis Lua 기반 이메일·IP별 로그인 시도 제한
- Terraform 기반 AWS 인프라 구축: VPC·Private Subnet·보안 그룹으로 접근을 제어하고, ECS on EC2·RDS·ALB·모니터링 인프라를 코드로 구축·관리
- CI/CD 및 배포 자동화: GitHub Actions·OIDC 기반 빌드·테스트·배포 자동화, 변경 서비스만 선택적으로 배포하고 ECS 롤링 업데이트·자동 롤백 적용

### 02. 지갑·충전·원장 (박현아)

[지갑·충전 가이드](docs/guide/wallet-service.md) · [원장 가이드](docs/guide/ledger-service.md) · [Mock Bank 가이드](docs/guide/mock-bank.md)

- 충전 기록과 Outbox 이벤트를 한 트랜잭션으로 저장하고, Kafka 기반 비동기 SAGA로 은행 출금 → 원장 적립 처리
- 결과 모름은 같은 requestId로 재전송해 확정하고, 원장이 확정 거절한 경우만 환불. 판단할 수 없는 건은 MANUAL_REVIEW
- 충전 기록·은행 거래·원장 기록 3자 대사, 관리자 보정 시 은행·원장 상태를 다시 조회해 실행할 수 있는 조치만 허용
- (거래 유형, 업무 ID) 유일 제약으로 원장 중복 기록 방지, 은행별 서킷 브레이커로 한 은행의 장애 격리

### 03. 송금·프론트엔드 (김지혜)

[송금 가이드](docs/guide/payment-transfer.md) · [프론트엔드 가이드](https://github.com/dont-worrys/travelpay-front/tree/main/docs/guide)

- Idempotency-Key·요청 해시·DB UNIQUE 제약으로 중복 송금 방지
- 비관적 잠금, 계정 ID 기준 잠금 순서 고정, DB 잔액 제약으로 동시 송금의 교착·초과 출금 방지
- 타임아웃·5xx는 PROCESSING으로 두고 같은 reference로 복구, 재시도 5회 초과 시 MANUAL_REVIEW에서 원장 거래를 대조해 확정
- 프론트엔드는 결과 불명 시 같은 키·같은 본문으로만 재요청하고 입력을 잠가 중복 거래 방지
- 송금 완료를 상태 변경과 같은 트랜잭션의 Outbox에 기록해 Kafka로 발행하고, user-service가 eventId로 중복을 걸러 보낸·받는 사람 화면 알림과 Slack 알림을 남김(송금 처리와 분리)

### 04. 환율·환전 (백가영)

[환율·환전 가이드](docs/guide/exchange-service.md)

- Redis 기반 환율·견적 처리: 환율은 수집 커밋 뒤 Redis에 갱신해 한 번에(MGET) 조회하고, 200ms 안에 응답이 없으면 DB로 대체. 견적은 Redis에 5분 TTL로 저장(noeviction)하고 1회 사용은 주문 UNIQUE 제약으로 보장해, 요청마다 DB 연결을 쓰지 않음
- 결과 모름 자동 확정·동시성 제어: 원장에 출금·수수료·입금을 주문 ID로 한 번만 기록하고, 결과 모름은 202로 답한 뒤 복구 스케줄러가 같은 요청을 재전송해 확정. 같은 멱등 키 재요청은 원장을 부르지 않고 저장된 결과로 응답하며, 주문 상태는 WHERE status 원자적 UPDATE로 한 번만 반영
- 서버 판정 기반 관리자 보정: 끝내 확정하지 못한 주문은 MANUAL_REVIEW로 두고, 서버가 원장 기록을 조회해 결과를 판정 (관리자는 예상 결과를 확인하고 처리만 실행)
- 원장 장애 전파 차단: 서킷 브레이커로 요청 스레드 고갈을 막고, 예약 체결을 환율 수집 스레드에서 분리해 장애가 환율 조회·견적으로 번지지 않게 함
- 이벤트 기반 예약 자동 체결: 새 환율이 저장될 때만 조건에 닿은 예약을 찾고, SKIP LOCKED로 잠근 뒤 조건을 다시 확인해 체결. 체결 주문은 멱등 키(reservation:{예약 ID})로 두 번 생성되지 않음

### 05. 결제·환불·정산 (이안근)

[결제·정산 가이드](docs/guide/payment.md)

- 원장 reference 멱등성으로 중복 요청·재시도로 인한 이중 결제 방지
- 결과를 알 수 없는 거래는 백오프 재시도 후 원장 조회로 자동 확정, 확정 못 하면 운영자 검토로 전환
- 운영자 검토 건을 실패 처리할 때 원장에 취소 기록을 먼저 남겨, 늦게 도착한 등록 요청 차단
- 원장 호출 서킷 브레이커로 원장 지연이 결제로 번지는 것을 차단
- SKIP LOCKED·advisory lock으로 다중 서버 스케줄러의 중복 처리 방지

<br/>

## 7. 성능 테스트 및 트러블슈팅

> 운영 환경이 아닌 로컬 측정 결과이며, 개선 전후는 같은 조건에서 비교했습니다. 측정 조건과 분석은 서비스별 가이드([`docs/guide/`](docs/guide/))에 있습니다.

| &nbsp;&nbsp;&nbsp;담&#8288;당&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;영&#8288;역&nbsp;&nbsp;&nbsp; | 문제 | 개선 | 결과 |
| --- | --- | --- | --- | --- |
| 김이찬 | 인증 | Gateway·User의 중복 인증 | 중복 인증 제거 + SQL 조회 최적화 | 마이페이지 처리량 약 1.8배 (로컬) |
| 김이찬 | 인증 | 인증 장애가 다른 서비스로 전파 | 서킷 브레이커·타임아웃·동시 호출 제한 | DB 장애 시험에서 지갑 조회 성공률 11% → 94% (로컬) |
| 박현아 | 충전 | 처리량이 Outbox 발행(1초 50건)에 막힘 | Outbox batch 50 → 500, 컨슈머 동시성 1 → 3 (함께 조정) | 완료 처리량 53.74 → 221.56건/초 |
| 박현아 | 충전 | 은행 한 곳의 장애가 정상 은행 충전까지 지연 | 은행별 서킷 브레이커 | 정상 은행 완료 지연 P95 52.37 → 7.24초 |
| 김지혜 | 송금 | 수취인 집중 시 원장 계정 잠금 구간 DB 왕복 | 원장 Hibernate JDBC batch | 성공 TPS 약 13% 증가, 원장 응답 시간 약 19% 감소 |
| 김지혜 | 송금 | 원장 정지·응답 유실·동시 요청 등 장애 | 결과 불명 처리·복구 스케줄러·서킷 브레이커 | E2E 장애 시나리오 12가지에서 이중 차감·음수 잔액·대사 위반 0건 |
| 백가영 | 환율·견적 | 요청마다 DB 연결을 빌려 연결 10개 앞에서 대기 | 환율: 쿼리 통합 → Redis 캐시 / 견적: 저장소 Redis 전환 | 처리량 환율 6,490 → 17,730건/초(약 2.7배), 견적 3,051 → 14,525건/초(약 4.8배) |
| 백가영 | 환전 | 원장 장애가 환율 조회·견적으로 번지고, 회복 뒤 쌓인 주문 확정이 느림 | 서킷 브레이커·예약 체결 스레드 분리 / 복구 스케줄러 연속 처리·회복 직후 재시도 대기 무시 | 장애 중 환율 조회 p50 8.1초 → 0.94ms, 주문 1,000건 확정 600 → 24초(약 25배) |
| 이안근 | 결제 | 원장 지연이 결제 지연으로 전파 | 원장 호출 서킷 브레이커 | 원장 지연 시 결제 p50 20초 → 6ms |
| 이안근 | 결제 | 원장 장애 중 거래 정합성 | 결과 모름 자동 확정·원장 취소 기록 | 원장 장애 대사 테스트에서 잔액 불일치 0건 |

<br/>

## 8. 실행 방법

> 배포 주소: https://travelpay.store
> 자세한 로컬 설정은 [`docs/guide/local-settings.md`](docs/guide/local-settings.md)를 참고하세요.

### 준비물

| 도구 | 용도 |
| --- | --- |
| JDK 21 | 백엔드 실행 |
| Docker Compose | PostgreSQL·Redis·Kafka 실행 |
| Node.js 20.19 이상 | 프론트엔드 실행 ([travelpay-front](https://github.com/dont-worrys/travelpay-front)) |

### 1. 환경 파일

```bash
cp -n .env.example .env
```

- Wallet의 `WALLET_ACCOUNT_NUMBER_KEY`(Base64 32바이트)가 비어 있으면 기동하지 않으므로 `.env.example` 설명대로 채웁니다.
- 로컬에서는 `SPRING_PROFILES_ACTIVE=prod`를 켜지 않습니다.

### 2. 인프라

```bash
docker compose up -d postgres redis-session kafka redis-cache
```

### 3. 서비스

Eureka → User → Ledger → Mock Bank → Wallet → Payment·Exchange → Gateway 순서로 띄웁니다.

```bash
set -a; source .env; set +a
./gradlew :eureka-server:bootRun --args='--spring.config.name=application --spring.profiles.active=default'
# 다른 서비스도 모듈 이름만 바꿔 실행
# user-service, ledger-service, mock-bank, wallet-service, payment-service, exchange-service, gateway
```

| 서비스 | 필수 환경 변수 |
| --- | --- |
| Gateway | `GATEWAY_CORS_ALLOWED_ORIGINS=http://localhost:5173` |
| Wallet | `MOCK_BANK_URL=http://localhost:8090` |
| Exchange | `EXCHANGE_RATE_MODE=mock` |

기동 확인: Eureka `http://localhost:8761`에 서비스가 `UP`으로 보이면 됩니다.

송금 완료 알림(Kafka) 흐름의 확인 명령, 장애 재현, DLT 재투입은 [송금 완료 알림 Kafka 가이드](docs/guide/transfer-notification-kafka.md)에 있습니다.

### 4. 프론트엔드

```bash
git clone https://github.com/dont-worrys/travelpay-front.git
cd travelpay-front
npm install
cp .env.example .env.local
npm run dev   # http://localhost:5173
```