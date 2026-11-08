# 크레텍 실시간 재고·발주 AI Agent 실습 (v2)

## 목적과 아키텍처

기존 `order-simulator` **그대로 사용** → `orderdb.orders` INSERT → Debezium MySQL CDC → Kafka (`orderscdc.orderdb.orders`) → Python Consumer → 실습용 SQLite 재고 투영 → 규칙 엔진(안전재고/ROP/목표재고) → Ollama qwen3:8b AI Agent가 실제 Tool Calling → `PURCHASE`/`WATCH` 판단 → 독립 규칙 엔진 대조 → 발주 **승인 대기 초안** + FastAPI 대시보드.

이 예제의 구매 발주는 ERP 구매발주가 아니라 `po_drafts` 테이블에 적재된 **승인 전 제안**입니다. ERP의 실제 매입 발주, 대금 지출, 외부 시스템 호출은 구현하지 않습니다. 승인 및 ERP API는 추후 별도 구현하세요.

## 1. 데모 실행 (Windows PowerShell)

1. GPU 서버 Ollama가 PC 로컬 15000 포트에 포워딩되어 있어야 합니다.

   ```powershell
   kubectl -n default port-forward svc/ollama 15000:11434
   ```

2. 다른 PowerShell 터미널에서 프로젝트 압축 해제 폴더로 이동:

   ```powershell
   py -m pip install -r requirements.txt
   $env:RUN_MODE='demo'
   $env:OLLAMA_URL='http://localhost:15000'
   $env:OLLAMA_MODEL='qwen3:8b'
   py -m uvicorn app:app --host 127.0.0.1 --port 8080
   ```

3. `http://127.0.0.1:8080` 접속 → 상품을 선택하고 `모의 주문 발생`. 재고가 감소하고 재주문점 도달 시 발주 초안이 만들어집니다.
4. AI 모델 응답은 비동기이므로 분석 완료까지 새로고침 없이 기다릴 수 있으며, 대시보드는 4초 주기로 자동 갱신합니다.
5. **확인:** `AI Agent 판단과 독립 정책 검증`의 `에이전트 도구 호출 이력`에서 `get_inventory` 등을 볼 수 있어야 합니다. Ollama에 연결되지 않으면 실패 상태가 표시되고, 정책 엔진의 판단은 별도로 유지됩니다. AI가 성공했다고 가짜 표시하지 않습니다.

재실습 초기화를 원하면 서버를 종료한 뒤 `inventory_agent.db` 파일을 삭제하세요. 실제 운영 데이터와는 분리된 데모 상태 파일입니다.

## 2. 기존 Kafka 연결

### 먼저 기존 orders 스키마 확인

```sql
SHOW CREATE TABLE orderdb.orders;
SELECT * FROM orderdb.orders ORDER BY 1 DESC LIMIT 5;
```

**필수 확인:** 주문 건 식별자 `id` 또는 `order_id`, `product_id` 또는 `product_code`, `quantity` 또는 `qty`가 Debezium `after`에 있어야 합니다. 사용자 시스템의 실제 컬럼명이 다르면 `engine.py`의 `parse_cdc` 필드 매핑을 변경하세요. `orders`에 한 행이 주문 헤더이고 품목이 별도 테이블인 구조라면 `order_items` CDC 또는 주문 헤더/상세 결합 로직이 필요합니다. 현재 코드는 **한 이벤트가 한 상품 주문행**인 실습 구조를 가정합니다. 이벤트 형식은 Debezium JSON envelope(스키마 포함 또는 제외)이며 `op='c'`만 처리합니다. 기존 프로듀서가 `op` 없는 JSON을 내보내면 변환 코드 수정이 필요합니다.

### 실제 Kafka 모드

```powershell
$env:RUN_MODE='kafka'
$env:KAFKA_BOOTSTRAP='YOUR_KAFKA_HOST:9092'
$env:KAFKA_TOPIC='orderscdc.orderdb.orders'
$env:KAFKA_GROUP='cretec-inventory-agent-v2'
$env:KAFKA_OFFSET_RESET='latest'
$env:OLLAMA_URL='http://localhost:15000'
py -m uvicorn app:app --host 127.0.0.1 --port 8080
```

이 모드에서는 Demo 버튼을 숨깁니다. 기존 order-simulator에서 MySQL `orderdb.orders`를 INSERT하면 **기존 Debezium 연결이 정상적일 경우에만** Kafka 이벤트가 들어옵니다. 반드시 Kafka 브로커 접속 주소와 컨슈머 권한을 확인하세요. Kafka를 직접 PC에 포트포워딩할 때는 `advertised.listeners`가 PC에서 해석 가능한 주소를 반환하는지도 확인해야 합니다. Kubernetes 안에서 Consumer를 배포하면 내부 서비스 DNS를 사용할 수 있습니다.

`auto_offset_reset=latest`는 새 consumer group에서 **접속 이후** 기록된 이벤트만 소비합니다. 과거 기록까지 가져오려면 재고 초기화와 재처리 전략을 먼저 마련하세요.

### 기존 order-simulator와 데이터 중복 차감 주의

본 실습의 `products.on_hand`는 **교육용 이벤트 기반 투영 재고**입니다. 원본 ERP 재고 DB와 동기화된 값이 아닙니다. Kafka에서 받은 `orders` INSERT에 대해 단순히 `on_hand -= 주문수량` 처리합니다. 실제 업무에서는 주문 취소, 부분 출고, 반품, 입고, 예약, 재고 조정 이벤트를 모두 처리해야 합니다. 기존 시뮬레이터가 별도의 재고 테이블도 변경하고 있다면 이 실습 상태를 ERP와 합산하거나 다시 차감하지 마세요. `order_id`에 유니크 제약을 두어 같은 주문 CDC가 재전달되어도 재고가 중복 차감되지 않도록 했습니다.

## 3. 재고 정책과 수치

- `inventory_position = on_hand + on_order - allocated`
- `reorder_point = ceil(daily_demand * lead_days + safety_stock)`
- `policy_action = PURCHASE if inventory_position <= reorder_point else WATCH`
- `target = ceil(daily_demand * (lead_days + review_days) + safety_stock)`
- `suggested_qty = max(MOQ, ceil((target - position)/pack_size)*pack_size)` (PURCHASE 시)

**P001 초기 예:** 현재고 190 / 확정발주 0 / 예약 15 / 일평균 수요 20 / 리드타임 5일 / 안전재고 50 / 재주문점 150 / 재고위치 175 → WATCH. 30개 주문 이벤트를 받으면 현재고 160 / 재고위치 145 → PURCHASE. 발주 목표 = 250, 제안량 = 110 (포장단위 10, 최소수량 50 적용).

중요: `on_order`는 **이미 확정되어 공급업체가 수용한 입고 예정 발주 잔량**입니다. `po_drafts`는 승인 전 제안이므로 `on_order`에 합치지 않습니다. 미승인 초안이 존재하면 같은 품목의 제안이 중복 생성되지 않도록 제한합니다. 일평균 수요, 안전재고, 리드타임, 초기 재고는 **실제 크레텍 수치가 아닌 교육용 예시**입니다.

## 4. Agent가 정말 하는 일

`agent.py`: Ollama `/api/chat` + function tools (`get_inventory`, `get_recent_orders`, `get_policy`). LLM은 도구 실행 순서와 추가 조사를 선택하고, `PURCHASE` 또는 `WATCH` 및 설명 JSON을 반환합니다. 숫자 계산과 ERP 쓰기는 LLM의 권한 밖입니다.

`store.complete`: 현재 상태에서 규칙을 **재계산**하고 LLM 결론을 대조합니다. 일치하면 `일치`, 다르면 `정책 불일치·교정`, LLM 실패 시 `LLM 실패·정책 엔진 대체`라고 표시하고 **규칙 엔진의 결과만** 발주 제안 생성에 사용합니다. 이것은 정답이 절대적으로 정확하다는 뜻이 아니라 **설정된 재고 정책과의 일치 여부**만 나타냅니다.

LLM은 반드시 `get_inventory`를 직접 호출해야 완료됩니다. 도구 호출을 하지 않거나 JSON 형식이 틀리면 오류가 기록됩니다. 실습용으로 Agent 추론과 정책 엔진을 명확히 분리했습니다.

## 5. Kubernetes

```powershell
# Docker Hub 사용자명, 네임스페이스, Service DNS는 실제 배포 환경으로 수정
# Docker build/push
# docker build -t YOUR_DOCKERHUB_USER/cretec-inventory-agent:1.0 .
# docker push YOUR_DOCKERHUB_USER/cretec-inventory-agent:1.0
kubectl apply -f k8s/agent.yaml
kubectl -n stud12 port-forward svc/inventory-agent 18080:8080
```

웹 대시보드: `http://localhost:18080`. 교육용 앱은 SQLite + 단일 파드(`replicas: 1`)입니다. PVC를 반드시 붙이세요. **다중 복제·운영 배포는 권장하지 않습니다.** 운영에서는 PostgreSQL/MySQL 트랜잭션 상태 저장, 별도 Kafka worker, outbox, 오프셋 관리, 주문 취소/반품, 승인 워크플로, RBAC, 모니터링이 필요합니다.

## 6. 테스트

```powershell
py -m pytest -q
```

## 7. 교육 확장

실제 재고 위치와 구매발주를 ERP DB/API로 연결하고, 입고 예정/주문 취소 이벤트를 추가합니다. 구매 정책 문서는 LightRAG의 검색 도구로 바꾸어 `get_policy`를 구현할 수 있습니다. 지금 버전은 현장 실행을 쉽게 하기 위해 정책을 텍스트 상수로 제공합니다. 대시보드의 차트는 Chart.js CDN을 사용하므로 브라우저 인터넷 접속이 필요합니다.
