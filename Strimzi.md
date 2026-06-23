Strimzi 기반 CDC 플랫폼 구성 설명서
Oracle → Kafka → PostgreSQL CDC 아키텍처 및 구성 요소 설명

1. 개요
1.1 문서 목적
본 문서는 Azure AKS 환경에 구성된 Strimzi 기반 CDC(Change Data Capture) 플랫폼의 전체 구조와 주요 구성 요소를 설명하기 위한 문서입니다.
해당 구성은 단순히 Kafka Cluster만 운영하기 위한 구조가 아니라, Oracle Source DB의 변경 데이터를 Debezium을 통해 수집하고, Kafka를 거쳐 PostgreSQL Target DB로 전달하기 위한 CDC 플랫폼으로 구성되어 있습니다.
즉, 이 환경의 핵심 목적은 Oracle 데이터를 PostgreSQL로 동기화하기 위한 CDC 파이프라인을 운영하는 것이며, Kafka는 그 과정에서 변경 이벤트를 저장하고 전달하는 중간 이벤트 허브 역할을 수행합니다.
본 문서에서는 위 CDC 플랫폼을 구성하는 AKS, Strimzi Operator, Kafka Cluster, KafkaNodePool, Kafka Connect, Debezium Oracle Source Connector, KafkaConnector, Kafka Topic, Argo CD, Git Repository 구조, AKHQ를 중심으로 각 구성 요소의 역할과 상호 관계를 설명합니다.

1.2 시스템 개요
현재 구성은 다음과 같은 데이터 흐름을 기준으로 동작합니다.
Oracle Source DB
   ↓
Kafka Connect
   └─ Debezium Oracle Source Connector
         ↓
         Kafka Topic
             ↓
             PostgreSQL Sink / Consumer
                 ↓
                 PostgreSQL Target DB
이 흐름에서 중요한 점은 Debezium Oracle Source Connector가 Kafka Connect 위에서 실행된다는 점입니다.
즉, Kafka Connect는 Connector를 실행하고 관리하는 런타임이며, 실제로 Oracle의 변경 로그를 읽고 Kafka Topic으로 CDC 이벤트를 발행하는 주체는 Kafka Connect 내부에서 동작하는 Debezium Connector입니다.

1.3 시스템 구성 요약
구분	내용
실행 환경	Azure AKS
Kafka 운영 방식	Strimzi Operator
Strimzi Helm Chart Version	0.70.0
Kafka Version	3.9.0
Kafka 모드	KRaft Mode
Source DB	Oracle
Target DB	PostgreSQL
CDC 수집 엔진	Debezium Oracle Source Connector
Connector 실행 환경	Kafka Connect
Kafka / Connect 배포 방식	Argo CD
Kafka 플랫폼 Helm Chart 저장 위치	DevOps Repository
Kafka 관련 CR 저장 위치	DevOps Repository
Connector 저장 위치	별도 Connector Repository
모니터링 / 운영 도구	AKHQ

2. 전체 아키텍처
2.1 논리 아키텍처
전체 CDC 플랫폼은 아래와 같은 논리 구조로 구성됩니다.
[Oracle Source DB]
        │
        │ Redo Log / Archive Log 기반 변경 데이터
        ▼
[Kafka Connect Cluster]
        └─ Debezium Oracle Source Connector
               │
               │ CDC 이벤트 생성
               ▼
[Kafka Cluster - Strimzi / KRaft]
        ├─ CDC Topic
        ├─ Connect Internal Topic
        └─ Debezium Schema History Topic
               │
               ▼
[PostgreSQL Sink / Consumer]
               │
               ▼
[PostgreSQL Target DB]
위 구조에서 Kafka는 최종 적재 대상이 아니라, Oracle Source와 PostgreSQL Target 사이에서 변경 이벤트를 안정적으로 전달하고 보관하는 중간 계층입니다.

2.2 배포 및 저장소 구조를 포함한 전체 아키텍처
현재 플랫폼은 단순히 AKS에 직접 리소스를 배포하는 방식이 아니라, Argo CD 기반 GitOps 구조로 운영됩니다.
또한 플랫폼 관련 구성과 Connector 구성이 서로 다른 저장소로 분리되어 있습니다.
GitHub / Git Repository
├─ DevOps Repository
│   ├─ Strimzi Helm Chart
│   ├─ Kafka 관련 CR
│   │   ├─ Kafka
│   │   ├─ KafkaNodePool
│   │   ├─ KafkaConnect
│   │   ├─ KafkaTopic / KafkaUser 등
│   │   └─ 기타 추가 구성 리소스
│   └─ Kustomize / Argo CD 배포 구성
│
└─ Connector Repository
    └─ KafkaConnector / Debezium Connector 설정
Argo CD
├─ DevOps Repository 기반 Kafka 플랫폼 배포
└─ Connector Repository 기반 Connector 배포
Azure AKS
├─ Strimzi Operator
├─ Kafka Cluster
├─ Kafka Connect
├─ Debezium Connector
└─ AKHQ
즉 현재 구성은 다음과 같이 역할이 분리되어 있습니다.
	• DevOps Repository
→ Strimzi Helm Chart, Kafka 플랫폼 리소스, 추가 CR, 배포 구성 관리
	• Connector Repository
→ Oracle CDC Connector 정의 및 설정 관리
	• Argo CD
→ 두 저장소의 선언형 구성을 AKS에 반영

3. AKS 기반 실행 환경
3.1 Azure AKS의 역할
AKS는 Strimzi Kafka, Kafka Connect, Debezium Connector, AKHQ가 실행되는 Kubernetes 기반 환경입니다.
이 플랫폼에서 AKS는 단순 실행 환경이 아니라, Kafka와 CDC 워크로드의 안정성에 직접 영향을 주는 핵심 인프라 계층입니다.
특히 Kafka와 Kafka Connect는 장시간 실행되며 상태를 가지는 워크로드이기 때문에, NodePool 구성, 스토리지 구성, 스케줄링 정책이 매우 중요합니다.
AKS 관점에서 중요하게 봐야 하는 요소는 다음과 같습니다.
	• Kafka Broker가 배치되는 NodePool
	• Kafka Controller가 배치되는 NodePool
	• Kafka Connect가 배치되는 NodePool
	• AKHQ가 배치되는 NodePool
	• PVC / StorageClass
	• nodeSelector / affinity / anti-affinity
	• taint / toleration
	• PodDisruptionBudget
	• 노드 유지보수 시 재스케줄 영향

3.2 NodePool 구성
Kafka와 Kafka Connect는 역할과 특성이 다르므로, 실제 운영 환경에서는 NodePool 배치 구조를 명확히 이해하는 것이 중요합니다.
일반적으로 다음과 같은 관점으로 구분할 수 있습니다.
NodePool	역할
System NodePool	AKS 기본 시스템 컴포넌트 실행
Kafka Broker NodePool	Kafka Broker Pod 실행
Kafka Controller NodePool	KRaft Controller Pod 실행
Kafka Connect NodePool	Kafka Connect 및 Debezium Connector 실행
운영/모니터링 NodePool	AKHQ 등 운영성 컴포넌트 실행 시 사용 가능
실제 환경에서는 Broker와 Controller가 동일 NodePool에 배치될 수도 있고, 분리될 수도 있습니다.
Kafka Connect 역시 Kafka와 동일 NodePool에 둘 수도 있지만, 운영 안정성과 리소스 분리를 위해 별도 NodePool로 구성하는 경우가 많습니다.

4. GitOps 및 저장소 구조
4.1 저장소 분리 개요
현재 플랫폼은 단일 저장소에서 모든 리소스를 관리하는 구조가 아니라, 역할에 따라 저장소가 분리되어 있습니다.
이 구조는 플랫폼 운영과 CDC 작업 단위 관리를 분리하기 위한 목적을 가집니다.
현재 저장소 구조는 크게 아래 두 영역으로 나뉩니다.
	1. DevOps Repository
	2. Connector Repository

4.2 DevOps Repository의 역할
DevOps Repository는 Kafka 플랫폼 자체를 구성하는 리소스와 배포 구조를 관리하는 저장소입니다.
현재 DevOps Repository에는 다음 요소들이 포함됩니다.
1) Strimzi Helm Chart
Strimzi Helm Chart를 외부에서 직접 참조하는 방식이 아니라, 사내 DevOps Repository에 Chart 자체를 저장하여 관리합니다.
즉, Strimzi 설치 기준점이 외부 공개 저장소가 아니라 사내 Git Repository에 포함된 Helm Chart입니다.
2) Kafka 플랫폼 관련 CR
Strimzi 설치 이후 실제 Kafka 플랫폼을 구성하는 Custom Resource 역시 DevOps Repository에 저장되어 있습니다.
예를 들면 다음과 같은 리소스가 포함될 수 있습니다.
	• Kafka
	• KafkaNodePool
	• KafkaConnect
	• KafkaTopic
	• KafkaUser
	• 기타 플랫폼 운영을 위한 추가 CR
즉, Strimzi Operator 설치용 Helm Chart와 그 위에 배포되는 Kafka 플랫폼 리소스가 모두 같은 DevOps Repository에서 관리되는 구조입니다.
3) Kustomize 및 Argo CD 배포 구성
환경별 Overlay, 공통 리소스 조합, Argo CD Application 구성 역시 DevOps Repository에 포함됩니다.

4.3 Connector Repository의 역할
Connector Repository는 실제 Oracle CDC 작업 단위를 정의하는 KafkaConnector 리소스와 Debezium 설정을 관리하는 저장소입니다.
Kafka 플랫폼 자체와 달리 Connector는 Source DB별로 개별적인 설정이 필요하므로, 별도 저장소로 분리하여 관리합니다.
Connector Repository에는 일반적으로 다음 요소가 포함됩니다.
	• KafkaConnector YAML
	• Debezium Oracle Source Connector 설정
	• Source DB별 Connector 정의
	• 환경별 Overlay
	• Connector 배포 관련 리소스
즉, Kafka 플랫폼 구성과 Source별 CDC 작업 정의를 저장소 단위로 분리한 구조입니다.

4.4 저장소 구조 예시
현재 구조를 논리적으로 정리하면 다음과 같이 이해할 수 있습니다.
DevOps Repository
├─ helm/
│   └─ strimzi/
│       └─ Strimzi Helm Chart
│
├─ kafka/
│   ├─ Kafka CR
│   ├─ KafkaNodePool CR
│   ├─ KafkaConnect CR
│   ├─ KafkaTopic / KafkaUser 등
│   └─ 기타 Kafka 플랫폼 관련 CR
│
├─ kustomize/
│   ├─ base/
│   └─ overlays/
│
└─ argocd/
    └─ Kafka 플랫폼 배포용 Application 정의
Connector Repository
├─ connectors/
│   ├─ Oracle Source별 KafkaConnector
│   └─ Debezium 설정
│
├─ kustomize/
│   ├─ base/
│   └─ overlays/
│
└─ argocd/
    └─ Connector 배포용 Application 정의

5. Argo CD 배포 구조
5.1 Argo CD 역할
Argo CD는 Git Repository에 선언된 리소스를 AKS에 반영하는 GitOps 배포 도구입니다.
현재 구성에서는 배포 대상이 크게 세 가지로 분리되어 있습니다.
	1. Strimzi Operator 설치용 Helm
	2. Kafka 플랫폼 CR 배포용 Kustomize
	3. CDC Connector 배포용 Kustomize
즉, Argo CD는 단순히 하나의 애플리케이션만 배포하는 것이 아니라, Operator 설치, Kafka 플랫폼 구성, Connector 구성을 각각 분리된 단위로 동기화합니다.

5.2 배포 단위 분리
현재 Argo CD 배포 구조는 다음과 같이 이해할 수 있습니다.
배포 구분	관리 방식	저장소	배포 대상
Strimzi Operator	Helm Chart	DevOps Repository	Strimzi Cluster Operator
Kafka 플랫폼 CR	Kustomize	DevOps Repository	Kafka, KafkaNodePool, KafkaConnect, KafkaTopic, KafkaUser, 기타 추가 CR
CDC Connector	Kustomize	Connector Repository	KafkaConnector, Debezium Oracle Source Connector 설정

5.3 Strimzi Operator 설치용 Helm
Strimzi Operator는 Helm Chart를 통해 설치됩니다.
해당 Helm Chart는 외부 Chart Repository를 직접 참조하는 방식이 아니라, DevOps Repository에 저장된 Helm Chart를 기준으로 배포됩니다.
이 배포 단위의 목적은 Kafka 관련 Custom Resource를 관리하는 Strimzi Cluster Operator를 AKS에 설치하는 것입니다.
주요 대상은 다음과 같습니다.
	• Strimzi Cluster Operator
	• Strimzi 관련 CRD
	• Operator Deployment
	• Operator RBAC
	• Operator ServiceAccount
	• Operator 설정
즉, 이 계층은 Kafka 플랫폼을 실제로 운영하기 위한 Strimzi 관리 기반을 설치하는 단계입니다.

5.4 Kafka 플랫폼 CR 배포용 Kustomize
Strimzi Operator 설치 이후, 실제 Kafka 플랫폼을 구성하는 Custom Resource는 Kustomize를 통해 배포됩니다.
이 리소스들은 Helm Chart와 동일한 DevOps Repository에 저장되어 있으며, Kafka 플랫폼의 실제 구성을 정의합니다.
주요 대상은 다음과 같습니다.
	• Kafka
	• KafkaNodePool
	• KafkaConnect
	• KafkaTopic
	• KafkaUser
	• 기타 Kafka 플랫폼 운영에 필요한 추가 CR
즉, Helm은 Operator 설치에 사용되고, Kustomize는 Operator가 관리할 Kafka 플랫폼 리소스를 배포하는 데 사용됩니다.

5.5 CDC Connector 배포용 Kustomize
CDC Connector 역시 별도 저장소에 정의된 리소스를 Kustomize 방식으로 배포합니다.
Connector Repository에는 실제 Oracle Source DB를 CDC하기 위한 KafkaConnector 리소스와 Debezium Oracle Source Connector 설정이 포함되며, Argo CD는 해당 저장소의 Kustomize 구성을 기준으로 Connector를 배포합니다.
주요 대상은 다음과 같습니다.
	• KafkaConnector
	• Debezium Oracle Source Connector 설정
	• Source DB별 Connector 정의
	• Connector별 topic.prefix
	• Snapshot 설정
	• Schema History Topic 설정
	• Table include / exclude 설정
즉, Connector Repository는 Kafka 플랫폼 자체가 아니라, 실제 Oracle CDC 작업 단위를 Kustomize 기반으로 관리하는 저장소입니다.

5.6 배포 구조 요약
전체 배포 구조는 다음과 같이 정리할 수 있습니다.
Argo CD
├─ Strimzi Operator 배포
│   └─ DevOps Repository의 Helm Chart 사용
│
├─ Kafka 플랫폼 CR 배포
│   └─ DevOps Repository의 Kustomize 구성 사용
│
└─ CDC Connector 배포
    └─ Connector Repository의 Kustomize 구성 사용
이 구조를 통해 Operator 설치, Kafka 플랫폼 구성, CDC Connector 배포가 서로 분리되어 관리됩니다.

6. Strimzi 기반 Kafka 운영 구조
6.1 Strimzi Operator 개요
Strimzi는 Kubernetes 환경에서 Kafka를 운영하기 위한 Operator 기반 프레임워크입니다.
Strimzi를 사용하면 Kafka 관련 구성을 Kubernetes Custom Resource 형태로 선언할 수 있으며, Strimzi Operator가 이를 실제 StatefulSet, Service, Secret, ConfigMap 등으로 변환하여 관리합니다.
이 플랫폼에서는 Strimzi가 Kafka와 Kafka Connect의 운영 기반 역할을 담당합니다.

6.2 Strimzi Operator의 역할
Strimzi Operator는 Kafka 관련 리소스를 감시하고 필요한 리소스를 생성·변경하는 컨트롤러입니다.
주요 관리 대상은 다음과 같습니다.
	• Kafka
	• KafkaNodePool
	• KafkaTopic
	• KafkaUser
	• KafkaConnect
	• KafkaConnector
즉, 사용자는 Kafka나 Connect 구성을 YAML로 선언하고, Strimzi Operator가 실제 Kubernetes 리소스로 반영하는 방식으로 운영합니다.

7. Kafka Cluster 구성
7.1 Kafka Cluster의 역할
Kafka Cluster는 Oracle Source에서 수집된 CDC 이벤트를 저장하고, PostgreSQL 적재 프로세스로 전달하는 중심 메시지 플랫폼입니다.
이 플랫폼에서 Kafka는 다음과 같은 데이터를 저장합니다.
	• Oracle 변경 이벤트가 적재되는 CDC Topic
	• Kafka Connect의 내부 상태 Topic
	• Debezium의 Schema History Topic
	• 필요 시 장애 처리용 보조 Topic
즉 Kafka는 단순 메시지 브로커가 아니라, CDC 이벤트를 안정적으로 전달하고 재처리 가능성을 보장하는 이벤트 저장소 역할을 수행합니다.

7.2 Kafka Version 및 KRaft Mode
현재 Kafka는 3.9.0 버전이며, KRaft 모드로 구성되어 있습니다.
KRaft 모드는 기존 Kafka에서 사용하던 ZooKeeper 없이 Kafka 자체가 메타데이터를 관리하는 방식입니다.
따라서 현재 구성에서는 별도의 ZooKeeper Cluster가 존재하지 않으며, Kafka Controller가 메타데이터를 관리합니다.
KRaft 모드의 주요 특징은 다음과 같습니다.
	• ZooKeeper를 사용하지 않습니다.
	• Kafka Controller가 메타데이터를 관리합니다.
	• Controller Quorum 유지가 중요합니다.
	• Broker와 Controller 역할을 논리적으로 분리할 수 있습니다.
	• Strimzi의 KafkaNodePool을 통해 역할별 구성이 가능합니다.

8. KafkaNodePool 구성
8.1 KafkaNodePool 개념
KafkaNodePool은 Strimzi에서 Kafka Broker와 Controller를 역할별로 관리하기 위한 리소스입니다.
KRaft 기반 Kafka에서는 Broker와 Controller의 역할을 논리적으로 구분할 수 있으므로, NodePool 개념이 중요해집니다.
예를 들어 아래와 같은 구조로 이해할 수 있습니다.
KafkaNodePool
├─ controller
│   └─ KRaft 메타데이터 관리
└─ broker
    └─ Kafka 메시지 저장 및 Client 요청 처리

9. Kafka Connect 구성
9.1 Kafka Connect의 역할
Kafka Connect는 Connector를 실행하는 런타임입니다.
즉 Debezium Oracle Source Connector가 실제로 실행되는 환경이며, Connector의 상태와 설정, 오프셋을 관리하는 역할을 수행합니다.
Kafka Connect는 CDC 파이프라인의 실행 엔진으로 이해하시면 됩니다.

9.2 Kafka Connect와 Debezium Connector의 관계
Kafka Connect와 Debezium Connector는 서로 다른 단계가 아니라, “런타임과 그 위에서 실행되는 작업” 관계입니다.
즉 아래와 같이 이해하는 것이 정확합니다.
Kafka Connect Worker
└─ Debezium Oracle Source Connector Task
    ├─ Oracle 변경 로그 읽기
    └─ Kafka Topic으로 CDC 이벤트 발행
따라서 데이터 흐름을 표현할 때는
**“Oracle → Debezium Connector → Kafka Connect → Kafka”**로 이해하면 안 되고,
**“Kafka Connect 위에서 실행되는 Debezium Connector가 Oracle 변경 로그를 읽어 Kafka Topic으로 이벤트를 발행한다”**고 이해해야 합니다.

10. Debezium Oracle Source Connector
10.1 Debezium의 역할
Debezium은 CDC(Change Data Capture)를 수행하는 오픈소스 커넥터 프레임워크입니다.
현재 구성에서는 Debezium의 Oracle Source Connector를 사용하여 Oracle 변경 데이터를 Kafka로 수집합니다.
Debezium Oracle Source Connector의 주요 역할은 다음과 같습니다.
	• Oracle 변경 로그 읽기
	• Insert / Update / Delete 이벤트 추출
	• CDC 이벤트 메시지 생성
	• Kafka Topic으로 이벤트 발행
	• Snapshot 수행
	• Offset 저장 및 재시작 복구
	• Schema History 관리

11. KafkaConnector 구성
11.1 KafkaConnector의 의미
KafkaConnector는 실제 CDC 작업 단위를 정의하는 리소스입니다.
쉽게 말하면 **“어떤 Oracle Source를 어떤 설정으로 CDC할 것인지”**를 정의하는 객체입니다.
예를 들어 다음과 같은 항목이 KafkaConnector 설정에 포함될 수 있습니다.
	• Oracle 접속 정보
	• Connector 클래스
	• topic.prefix
	• Snapshot 정책
	• Schema History Topic
	• 테이블 필터링 규칙
	• Converter 설정

11.2 KafkaConnector 저장 위치
현재 KafkaConnector는 Kafka 플랫폼을 구성하는 DevOps Repository가 아니라, 별도 Connector Repository에서 관리됩니다.
이는 Kafka 플랫폼의 공통 구성과 실제 Source별 CDC 작업 정의를 분리하기 위한 구조입니다.
즉 다음과 같이 구분할 수 있습니다.
	• DevOps Repository
→ Kafka 플랫폼 자체
	• Connector Repository
→ 실제 Oracle Source별 CDC 작업 정의

12. Oracle → Kafka → PostgreSQL 데이터 흐름
12.1 전체 처리 흐름
이 플랫폼의 실제 데이터 흐름은 아래와 같습니다.
1. Oracle Source DB에서 데이터 변경이 발생합니다.
2. Oracle Redo Log / Archive Log에 변경 내용이 기록됩니다.
3. Kafka Connect에서 실행 중인 Debezium Oracle Source Connector가 해당 변경 로그를 읽습니다.
4. Debezium Connector가 변경 내용을 CDC 이벤트로 변환합니다.
5. Debezium Connector가 Kafka Topic으로 CDC 이벤트를 발행합니다.
6. PostgreSQL Sink / Consumer가 Kafka Topic을 구독합니다.
7. Sink / Consumer가 이벤트를 PostgreSQL Target DB에 반영합니다.

13. Topic 구성
13.1 CDC Topic
CDC Topic은 Oracle 변경 이벤트가 저장되는 Topic입니다.
일반적으로 특정 Oracle Schema / Table 단위로 생성되며, 이후 PostgreSQL 적재 대상과 매핑됩니다.
예를 들어 다음과 같은 형식을 가질 수 있습니다.
<topic.prefix>.<schema>.<table>
예시:
oracle-cdc.HR.EMPLOYEES
oracle-cdc.SALES.ORDERS

13.2 Kafka Connect Internal Topic
Kafka Connect는 내부 상태를 저장하기 위해 Kafka Topic을 사용합니다.
Topic	역할
connect-offsets	Connector 처리 위치 저장
connect-configs	Connector 설정 저장
connect-status	Connector 상태 저장

13.3 Schema History Topic
Debezium Oracle Source Connector는 Oracle 스키마 변경 이력을 관리하기 위해 Schema History Topic을 사용할 수 있습니다.
Schema History Topic은 다음과 같은 목적을 가집니다.
	• 테이블 구조 변경 이력 저장
	• CDC 이벤트 해석 시 스키마 버전 참조
	• Connector 재시작 시 기존 스키마 상태 복구

14. Snapshot 및 Offset 구조
14.1 Snapshot
Debezium은 Connector 최초 실행 시 Snapshot을 수행할 수 있습니다.
Snapshot은 Oracle 테이블의 현재 데이터를 한 번 읽어 Kafka에 적재하는 과정입니다.
이후 Snapshot이 완료되면, Debezium은 Oracle 변경 로그를 계속 읽으며 CDC 이벤트를 생성합니다.

14.2 Offset
Offset은 Connector가 Oracle 변경 로그를 어디까지 읽었는지를 나타내는 처리 위치 정보입니다.
Kafka Connect는 Offset을 Kafka Topic에 저장하며, 이를 통해 Connector 재시작 시 마지막 처리 위치부터 다시 읽을 수 있습니다.

15. PostgreSQL Sink / Consumer
15.1 PostgreSQL 적재 계층의 역할
PostgreSQL은 이 CDC 플랫폼의 최종 적재 대상입니다.
Kafka에 적재된 Oracle CDC 이벤트는 PostgreSQL Sink 또는 Consumer를 통해 PostgreSQL에 반영됩니다.
이 계층의 역할은 다음과 같습니다.
	• Kafka Topic 구독
	• CDC 이벤트를 PostgreSQL 구조에 맞게 변환
	• Insert / Update / Delete 반영
	• Upsert 처리
	• 오류 발생 시 재처리 또는 예외 처리

16. AKHQ 기반 모니터링 및 운영
16.1 AKHQ 개요
현재 Kafka 운영 및 모니터링 도구로 AKHQ를 사용하고 있습니다.
AKHQ는 Kafka Cluster를 웹 UI로 조회하고 운영할 수 있도록 도와주는 도구로, Kafka Topic과 Consumer Group, 메시지 상태를 시각적으로 확인할 수 있는 관리 콘솔 역할을 수행합니다.

16.2 AKHQ의 역할
이 플랫폼에서 AKHQ는 다음과 같은 용도로 활용할 수 있습니다.
	• Kafka Cluster 상태 확인
	• Topic 목록 조회
	• Topic별 메시지 적재 현황 확인
	• Consumer Group 상태 확인
	• 특정 Topic 메시지 조회
	• Connector 동작 이후 Kafka 적재 여부 1차 확인
	• CDC 이벤트 흐름 점검 시 운영 보조 도구로 활용
즉 AKHQ는 Kafka 운영 관점의 가시성을 제공하는 UI 도구로 이해하시면 됩니다.

16.3 AKHQ와 CDC 운영의 관계
CDC 플랫폼 관점에서 AKHQ는 다음과 같은 지점에서 유용합니다.
	1. Debezium Connector가 Kafka Topic에 실제 이벤트를 적재하고 있는지 확인
	2. 특정 CDC Topic에 메시지가 정상적으로 쌓이고 있는지 확인
	3. Consumer Group이 Kafka 메시지를 정상적으로 소비하고 있는지 확인
	4. Kafka 내부 상태를 빠르게 조회하는 운영 UI로 활용
다만 AKHQ는 Kafka 상태를 확인하는 도구이지, Oracle Connector 내부 동작이나 PostgreSQL 적재 성공 여부 자체를 모두 보장해 주는 도구는 아닙니다.
따라서 CDC 운영 관점에서는 Connector 상태, Kafka 적재 상태, PostgreSQL 반영 상태를 구분해서 확인해야 합니다.

17. 구성 요소 간 관계 정리
이 플랫폼의 전체 구조를 다시 정리하면 다음과 같습니다.
Azure AKS
├─ Strimzi Operator
│   ├─ Kafka Cluster 관리
│   ├─ KafkaNodePool 관리
│   ├─ KafkaConnect 관리
│   └─ KafkaConnector 관리
│
├─ Kafka Cluster (KRaft)
│   ├─ Broker NodePool
│   ├─ Controller NodePool
│   ├─ CDC Topic
│   ├─ Connect Internal Topic
│   └─ Schema History Topic
│
├─ Kafka Connect
│   └─ Debezium Oracle Source Connector 실행
│
├─ PostgreSQL Sink / Consumer
│   └─ Kafka CDC 이벤트를 PostgreSQL에 반영
│
└─ AKHQ
    └─ Kafka 운영 및 모니터링 UI
그리고 배포 구조는 아래와 같이 이해할 수 있습니다.
DevOps Repository
├─ Strimzi Helm Chart
├─ Kafka / KafkaNodePool / KafkaConnect 등 플랫폼 CR
├─ 기타 추가 CR
└─ Kustomize / Argo CD 배포 구성
Connector Repository
└─ KafkaConnector / Debezium Connector 설정
Argo CD
├─ DevOps Repository 동기화
└─ Connector Repository 동기화

18. 핵심 정리
이 플랫폼의 핵심은 다음과 같습니다.
	1. Source는 Oracle입니다.
Oracle의 변경 로그가 CDC의 출발점입니다.
	2. Debezium Oracle Source Connector는 Kafka Connect 위에서 실행됩니다.
Kafka Connect는 실행 런타임이며, 실제 CDC 이벤트를 생성하여 Kafka로 발행하는 주체는 Debezium Connector입니다.
	3. Kafka는 CDC 이벤트를 저장하고 전달하는 중간 이벤트 허브입니다.
Oracle과 PostgreSQL 사이의 이벤트 버퍼 및 스트리밍 계층 역할을 수행합니다.
	4. PostgreSQL은 최종 적재 대상입니다.
Kafka 이벤트는 Sink 또는 Consumer를 통해 PostgreSQL에 반영됩니다.
	5. Strimzi는 Kafka와 Kafka Connect를 Kubernetes 리소스로 운영하기 위한 기반입니다.
Kafka, KafkaNodePool, KafkaConnect, KafkaConnector를 선언형으로 관리합니다.
	6. Kafka 플랫폼 관련 리소스는 DevOps Repository에서 관리됩니다.
Strimzi Helm Chart, Kafka / KafkaNodePool / KafkaConnect 등의 CR, 추가 배포 리소스가 모두 DevOps Repository에 저장됩니다.
	7. Connector는 별도 Connector Repository에서 관리됩니다.
실제 Oracle Source별 CDC 작업 정의와 Debezium Connector 설정은 Connector Repository에 저장됩니다.
	8. Argo CD가 두 저장소를 기반으로 GitOps 배포를 수행합니다.
DevOps Repository는 Kafka 플랫폼 배포에, Connector Repository는 Connector 배포에 사용됩니다.
	9. AKHQ는 Kafka 운영 및 모니터링 UI 역할을 수행합니다.
Topic, Consumer Group, 메시지 적재 상태를 확인하는 운영 도구로 사용됩니다.
즉, 이 구성은 단순 Kafka 운영 환경이 아니라, Azure AKS 위에 Strimzi를 기반으로 구축된 Oracle → Kafka → PostgreSQL CDC 플랫폼이며,
플랫폼 리소스와 Connector 리소스를 분리한 GitOps 구조, 그리고 AKHQ 기반 운영 가시성을 함께 갖춘 형태라고 정리할 수 있습니다.

출처: <https://chatgpt.com/c/6a38faa5-27f4-83ee-92b5-2eb1cd4fe5ba> 
