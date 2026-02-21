# Kafka Streams PDF 반영 계획서
*(resource/카프카 핵심가이드-페이지.pdf 기반)*

## 목적
카프카 스트림 처리 챕터 핵심 내용(스트림 개념, 윈도우, 상태, EOS, 복구, 조인)을 현재 프로젝트 구현에 누락/보완 없이 반영한다.

## 1) PDF 핵심 포인트 정리 (14장 요약)
- 스트림은 **무한 데이터셋(unbounded)** 이고, 지속적으로 들어오며 **순서**와 **재생 가능성**이 중요함.
- 스트림 처리의 핵심 설계 축:
  - 이벤트 시간(event time), 로그 추가 시간(log append time), 처리 시간(processing time) 구분
  - 이벤트 단위 처리 vs 상태 유지(aggregation/join)
  - **로컬 상태 + changelog** 기반 복구
  - 스트림-테이블 이원성(stream-table duality) 및 조인 패턴
  - **정확히 한 번 처리(EOS)**의 설계
  - 장애 시 복구 시간을 줄이는 튜닝(체인지로그 압착, 스탠바이 레플리카)
  - 토폴로지 테스트는 `TopologyTestDriver`로 반복 가능한 검증 필요

## 2) 현재 코드와 PDF 간 갭 체크

### 이미 반영됨
- `src/Main.kt`
  - 필터/맵, 윈도우 집계(`TimeWindows.ofSizeAndGrace(...).advanceBy(...)`), KTable 조인, EOS(`exactly_once_v2`) 반영
  - 상태 스토어 `Materialized` + `withLoggingEnabled` 사용
- `src/DailyMoverModels.kt`, `src/Main.kt`
  - PDF 14.3.3(`일별 상승/하락 Top10`) 기반 일별 모멘텀 파이프라인 구현:
    - 심볼별 24시간 이벤트 윈도우 집계
    - open/close 추적을 통한 `moveRate` 계산
    - 방향(`UP`/`DOWN`)별 Top 10 유지 및 상태 기반 출력
- `test/TopologyStep4Test.kt`, `test/TopologyStep5Test.kt`
  - `TopologyTestDriver` 기반 검증 추가됨
- `test/TopologyStep6Test.kt`
  - 일별 모멘텀의 Top10, 비순차 이벤트 반영 및 날짜 경계 분리 동작 검증
- `readme.md` 단계 5 항목 업데이트됨

### 보완 권장(우선 반영)
- 이벤트 시간 기반 처리 명시(타임스탬프/그레이스 정책)
- 장애 복구 튜닝 포인트를 실전 설정값으로 정리
- 토폴로지 최적화/운영 관측성 항목을 코드 및 문서에 추가

## 3) 반영 우선순위 (P0~P2)

### P0 (즉시 반영)
1. **이벤트 타임과 그레이스 정책 명시**
- 현재 윈도우 집계가 `Duration.ZERO` 중심이며, late event 시 동작이 제한적임.
- 제안:
  - 토픽 데이터 타임스탬프 추출기 등록
  - `WindowedBy(...).grace(...)`를 상황에 맞게 설정
- 파일: `src/Main.kt`
  - `buildTradeTopology`의 windowed 집계 구간
  - `Consumed.with(..., ..., TimestampExtractor)` 활용

2. **토폴로지 최적화 모드 노출**
- PDF에서 언급되는 토폴로지 최적화 개념 반영을 위해 명시적 설정 추가 권장.
- 제안:
  - `StreamsConfig.TOPOLOGY_OPTIMIZATION = StreamsConfig.OPTIMIZE`
- 파일: `src/Main.kt` 또는 공통 config 함수

3. **복구 튜닝 값 문서화 + 코드 주석**
- `min.compaction.lag.ms`, 스탠바이 레플리카, state.dir, topology 이름 정책을 설명 주석으로 남김.
- 파일: `src/Main.kt` 주석 + `readme.md`

### P1 (2~3회차 반영)
1. **TopologyTestDriver로 복구 경향 테스트 보강**
- 현재 테스트는 결과값 검증에 초점 → 아래 추가:
  - 늦은 이벤트/비순차 이벤트(Out-of-Order) 테스트
  - 동일 키 윈도우에서 집계 갱신(업데이트) 시나리오
- 파일: `test/TopologyStep5Test.kt`

2. **`TopologyTestDriver` 한계 문구 추가**
- PDF는 캐시 미반영 이슈를 언급 → 테스트 주석에 명시.
- 파일: `test/TopologyStep5Test.kt`

### P2 (안정화)
1. **실운영 실행 체크리스트 추가**
- 브로커 토픽 레벨에서 `cleanup.policy`, `min.compaction.lag.ms`, segment 크기/보존 정책 점검 항목 추가
- 파일: `readme.md` 또는 `notes/ops-checklist.md`

2. **인터랙티브 쿼리/관측성 확장 검토**
- 현재 목표 범위는 아님(PDF 참고용 확장 항목)

## 4) 적용 예시: 코드 주석/변경 방향 (요약)

### src/Main.kt (권장 추가/수정)
- 스트림 처리 핵심 개념 주석:
  - “이 토폴로지는 이벤트 단위 변환 + 상태 기반 윈도우 집계 + 스트림-테이블 조인 구성”
  - “로컬 상태는 RocksDB + changelog로 복구됨”
- 설정 분리:
  - application.properties식으로 `buildStreamsProperties()`에 `TOPOLOGY_OPTIMIZATION`, `num.standby.replicas` 분명히 노출
- 윈도우 정책:
  - `grace` 값 도입 및 그 목적(비순차 이벤트 허용 범위) 문서화

### test/TopologyStep5Test.kt (권장 추가)
- 테스트 케이스:
  - `windowedAggregation_withOutOfOrderUpdates_keepsSingleLogicalWindow`
  - `topologyConfiguration_shouldEnableOptimizationAndEos`
  - `restartRecovery_likeStatefulPath`(시뮬레이션 가능한 범위 내에서)

### readme.md (권장 확장)
- 5단계에 아래 주제를 한 줄씩 추가:
  - 이벤트 시간 + 윈도우 grace
  - 토폴로지 최적화 의미
  - 테스트 커버리지 한계(`TopologyTestDriver`는 캐시 미시뮬레이션)
  - 장애 대응 우선순위(체인지로그, 스탠바이 레플리카, `num.standby.replicas`)

## 5) 승인 기준(Definition of Done)
- [x] `exactly_once_v2`는 유지되며 토폴로지 최적화/상태 설정이 명시적으로 문서화됨
- [x] 윈도우 집계가 이벤트 타임/그레이스 의도를 드러내도록 구성됨
- [x] 복구/복원 관련 파라미터가 코드 상수+README에 일관되게 존재
- [x] Topology 테스트로 핵심 동작과 운영 정책(지연 이벤트/상태 갱신) 일부 보완됨
- [x] PDF 14.3.3의 일별 변동률 Top10 파이프라인(일별 오픈/클로즈 기준) 모델/토폴로지/테스트가 반영됨
