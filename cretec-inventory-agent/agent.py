"""Ollama tool calling; read-only tools, deterministic procurement guardrail in store.complete."""
import json,os,re
import requests
from store import product,recent
from engine import inventory_metrics
URL=os.getenv('OLLAMA_URL','http://127.0.0.1:15000').rstrip('/')
MODEL=os.getenv('OLLAMA_MODEL','qwen3:8b')
TIMEOUT=int(os.getenv('OLLAMA_TIMEOUT','120'))
POLICY_TEXT='''교육용 구매 정책: 재고위치=현재고+확정 발주잔량-예약수량. 재주문점=일평균 수요×조달 리드타임+안전재고.
재고위치가 재주문점 이하이면 PURCHASE(발주 승인 요청 초안), 초과하면 WATCH(관찰).
목표 재고위치=일평균수요×(리드타임+검토주기)+안전재고. 부족분은 MOQ 및 포장단위에 맞춰 올림.
승인 전에는 ERP 구매발주를 실제로 전송하지 않는다. 진행 중인 발주 초안이 있으면 중복 생성하지 않는다.
예시 학습 데이터이며 업체의 실제 정책이나 실적이 아니다.'''

def get_inventory(product_id):
    p=product(product_id)
    if not p:return {'error':'not found'}
    keys=('product_id','name','on_hand','on_order','allocated','daily_demand','lead_days','safety_stock','review_days','moq','pack_size')
    return {'product':{k:p[k] for k in keys},'computed':inventory_metrics(p)}

def get_recent_orders(product_id):
    d=recent(product_id,60)
    return {'minutes':60,'qty':d['qty'],'number_of_events':d['events'], 'recent_orders':d['history'][-12:],
            'note':'최근 60분은 Kafka에서 실제로 수신한 이벤트에 대해서만 집계함. 일평균 수요는 교육용 마스터 설정 값.'}

def get_policy(product_id):
    p=product(product_id)
    return {'product_id':product_id,'policy':POLICY_TEXT,'lead_days':p['lead_days'] if p else None}

TOOLMAP={'get_inventory':get_inventory,'get_recent_orders':get_recent_orders,'get_policy':get_policy}
TOOLS=[{'type':'function','function':{'name':name,'description':desc,
        'parameters':{'type':'object','properties':{'product_id':{'type':'string'}},'required':['product_id']}}}
       for name,desc in [('get_inventory','상품별 현재고, 입고확정잔량, 미출고 예약, 안전재고, 재주문점과 발주량을 조회'),
                       ('get_recent_orders','Kafka CDC로 수신한 최근 주문 이벤트를 조회'),
                       ('get_policy','안전재고와 재주문점 및 승인 정책을 조회')]]
SYS='''당신은 실시간 재고 의사결정 AI Agent이다. 반드시 도구를 직접 호출하여 근거를 수집하라.
상품의 재고와 주문 추이를 조회하고 필요한 경우 정책도 조회한다. 데이터에 없는 내용은 만들어내지 마라.
사용 가능한 조치는 PURCHASE(구매발주 초안 생성, 관리자 승인 대기)와 WATCH(관찰)뿐이다.
절대로 실제 ERP 발주 API를 직접 호출하지 마라. 구매 수량은 도구의 computed.suggested_qty만 인용한다.
응답 마지막에는 한 개의 JSON 오브젝트로만 답하라: {"action":"PURCHASE 또는 WATCH", "reason":"한국어 근거 및 한계 2~4문장"}.
근거에 현재고, 재고위치, ROP, 재고 부족 위험, 추가 확인 사항을 포함하라.'''

def chat(messages,with_tools=True):
    body={'model':MODEL,'messages':messages,'stream':False,'options':{'temperature':0}}
    if with_tools:body['tools']=TOOLS
    else:body['format']='json'
    r=requests.post(URL+'/api/chat',json=body,timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()['message']

def parse_final(raw):
    raw=(raw or '').strip()
    try:d=json.loads(raw)
    except json.JSONDecodeError:
        match=re.search(r'\{[\s\S]*\}',raw)
        if not match:raise ValueError('LLM did not return JSON')
        d=json.loads(match.group(0))
    action=str(d.get('action','')).strip().upper()
    if action not in ('PURCHASE','WATCH'):raise ValueError('invalid LLM action')
    return action,str(d.get('reason','설명 없음'))[:3000]

def run_agent(product_id):
    messages=[{'role':'system','content':SYS},{'role':'user','content':f'{product_id} 상품의 새 주문이 발생했습니다. 상황을 조사하고 발주 초안을 생성할지 관찰할지 판단하세요.'}]
    trace=[]
    for _ in range(5):
        res=chat(messages,True)
        messages.append(res)
        calls=res.get('tool_calls') or []
        if not calls:break
        for call in calls[:5]:
            fn=call.get('function',{})
            name=fn.get('name','')
            args=fn.get('arguments') or {}
            if isinstance(args,str):
                try:args=json.loads(args)
                except:args={}
            if name not in TOOLMAP or str(args.get('product_id'))!=product_id:
                output={'error':'unrecognized tool or product scope mismatch'}
            else:
                output=TOOLMAP[name](product_id)
            trace.append({'tool':name,'arguments':args,'result':output})
            messages.append({'role':'tool','tool_name':name,'content':json.dumps(output,ensure_ascii=False)})
    # Critical evidence requirement: LLM cannot bypass checking the inventory state.
    if 'get_inventory' not in [t['tool'] for t in trace]:
        raise ValueError('Model did not call mandatory inventory tool')
    res=chat(messages+[{'role':'user','content':'지금까지 확인한 근거만으로 최종 결론 JSON을 반환하세요.'}],False)
    action,reason=parse_final(res.get('content',''))
    return action,reason,trace
