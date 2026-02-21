

## 🛠 1단계: 인프라 및 프로젝트 환경 구축

스트림 처리는 독립적인 클러스터 구성이 핵심입니다. 개발 환경에서도 운영 환경과 유사한 격리된 환경을 구축하는 것부터 시작합니다.

* **Docker Compose 구성**: Zookeeper, Kafka 브로커 1대, 그리고 데이터 확인을 위한 UI 툴(예: Kafdrop)을 설정합니다.
* **Kotlin 프로젝트 설정**: `build.gradle.kts`에 `kafka-streams` 의존성을 추가해 스트림즈를 사용합니다.
* **비판적 포인트**: 왜 카프카 스트림즈는 별도의 처리 클러스터(YARN, Mesos 등) 없이도 실행 가능한지, 라이브러리 방식의 장단점을 논리적으로 정리해 보세요.

---

## 📦 2단계: 데이터 모델링 및 직렬화(Serde) 정의

카프카는 바이트 배열만 저장하므로, 애플리케이션 레벨에서 객체로 변환하는 과정이 반드시 필요합니다.

* **POJO/Data Class 설계**: `Trade`(주식 거래), `TradeStats`(통계), `UserProfile`(사용자 정보) 클래스를 작성합니다.
* **Custom Serde 구현**: Jackson이나 Gson을 사용하여 Kotlin 데이터 클래스를 위한 `Serde`를 생성합니다.
* **부족한 부분 가이드**: 단순히 라이브러리를 쓰기보다, **스키마 진화(Schema Evolution)** 관점에서 Avro나 Protobuf를 쓰지 않고 JSON을 선택했을 때의 리스크를 고민해 보는 것이 좋습니다.
  - 정적 스키마가 없는 JSON은 `null`, 누락 필드, 타입 변형 등에서 수용 규칙이 모호해져 소비자간 호환성 문제가 자주 생깁니다. 실제 운영에서는 필드 추가/삭제 전략과 기본값 정책을 문서화해야 합니다.

---

## ⚙️ 3단계: 단일 이벤트 처리 및 상태 기반 집계 구현

PDF에서 설명한 가장 보편적인 디자인 패턴 두 가지를 구현합니다.

* **필터링 및 변환 (Map/Filter)**: 특정 금액 이상의 거래만 추출하거나 로그 포맷을 변경합니다.
* **윈도우 집계 (Windowed Aggregation)**: 5초 길이의 호핑 윈도우(Hopping Window)를 설정하여 실시간 주가 평균을 구합니다.
* **로컬 상태 관리**: `Materialized`를 사용하여 RocksDB에 상태를 저장하고, 장애 발생 시 체인지로그 토픽을 통해 어떻게 복구되는지 흐름을 파악합니다.
  - 집계 상태는 `Materialized` 기반의 로컬 RocksDB 상태 스토어에 유지되고, 동일한 체인지로그 토픽으로 브로커에 복제됩니다.
  - 장애가 나면 태스크 재시작 시 Changelog를 재생해 RocksDB 캐시를 복구한 뒤, 처리 오프셋과 함께 이어서 집계를 진행합니다.

---

## 🔗 4단계: 스트림-테이블 조인 및 처리 보장 설정

분산 시스템에서 가장 난도가 높은 '조인'과 '정확성'을 다룹니다.

* **KTable 구축**: 사용자 프로필 토픽을 `KTable`로 읽어 들여 최신 상태를 로컬에 캐싱합니다.
* **조인 로직**: 클릭 스트림(`click-events`)과 사용자 프로필(`user-profiles`)을 `leftJoin` 하여 데이터를 확장(Enrichment)합니다.
* **정확히 한 번(EOS) 설정**: `processing.guarantee`를 `exactly_once_v2`로 설정(`trade-streaming-step4`)하여 정확성 보장을 적용합니다.
* **반영 코드**: `src/Main.kt`의 `buildTradeTopology`에 `builder.table(userProfileTopic)` + `clickStream.leftJoin(profileTable, ...)`이 반영되어 있으며,
  결과는 `click-events.enriched` 토픽으로 출력됩니다.
* **비판적 포인트**: 외부 DB를 직접 쿼리하는 방식과 비교했을 때, 스트림-테이블 조인이 성능과 가용성 면에서 왜 유리한지 논리적으로 설명할 수 있어야 합니다.

---

## 🧪 5단계: 토폴로지 테스트 및 운영 최적화

코드의 완결성을 높이고 시니어 엔지니어로서의 관점을 기르는 단계입니다.

* **TopologyTestDriver 활용**: 실제 브로커 없이 로직을 검증하는 단위 테스트를 작성합니다.
* **장애 복구 최적화**: 집계 상태 스토어는 `withLoggingEnabled`로 `min.compaction.lag.ms`를 지정해 체인지로그 압축 동작을 튜닝합니다. (`test/TopologyStep5Test.kt`, `defaultTradeStateLogConfig()`)
* **스탠바이 레플리카**: EOS 기반 토폴로지에 `num.standby.replicas`를 1로 설정해 장애 조치 비용을 낮추기 위한 준비를 수행합니다. (`buildStreamsProperties`)
* **운영 테스트**: 창 집계 결과의 증가치/누적치 계산을 TopologyTestDriver로 검증하고, 핵심 스트림 설정 값이 기본값으로 반영되는지 테스트합니다. (`test/TopologyStep5Test.kt`)

---
