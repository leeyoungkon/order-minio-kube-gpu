import json, logging, os, queue, threading, time, uuid
from pathlib import Path
from fastapi import FastAPI,HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel,Field
from store import init,ingest,dashboard,mark_running,evaluation,complete,fail,pending,product
from agent import run_agent
from engine import parse_cdc
logging.basicConfig(level=logging.INFO)
log=logging.getLogger('inventory-agent')
app=FastAPI(title='Cretec Safety Stock & Reorder Agent',version='2.0')
jobq=queue.Queue(maxsize=200)
MODE=os.getenv('RUN_MODE','demo').lower()

class Sample(BaseModel):
    product_id:str='P001'
    quantity:int=Field(ge=1,le=500,default=20)


def submit(order,source):
    result=ingest(order,source)
    eid=result.get('evaluation_id')
    if eid:
        try:jobq.put_nowait(eid)
        except queue.Full:log.error('queue full for evaluation %s; remains pending',eid)
    return result

def worker():
    while True:
        eid=jobq.get()
        try:
            if not mark_running(eid):continue
            ev=evaluation(eid)
            try:
                action,reason,trace=run_agent(ev['product_id'])
                complete(eid,action,reason,trace)
            except Exception as exc:
                log.exception('AI analysis failed for %s',eid)
                fail(eid,f'{type(exc).__name__}: {exc}')
        finally:jobq.task_done()

def kafka_loop():
    from kafka import KafkaConsumer
    topic=os.getenv('KAFKA_TOPIC','orderscdc.orderdb.orders')
    brokers=os.getenv('KAFKA_BOOTSTRAP','kafka:9092').split(',')
    # auto_offset_reset=latest: seed the lab's inventory independently before starting real consumption.
    while True:
        try:
            consumer=KafkaConsumer(topic,bootstrap_servers=brokers,
                group_id=os.getenv('KAFKA_GROUP','cretec-inventory-agent-v2'),
                enable_auto_commit=False,auto_offset_reset=os.getenv('KAFKA_OFFSET_RESET','latest'),
                value_deserializer=lambda b: json.loads(b.decode('utf-8')) if b else None,
                consumer_timeout_ms=1000)
            log.info('Kafka connected topic=%s',topic)
            while True:
                for item in consumer:
                    if item.value is None:consumer.commit();continue # Debezium tombstone
                    data=parse_cdc(item.value)
                    if data:
                        outcome=submit(data,'kafka')
                        if 'error' in outcome:log.warning('ignored product not configured: %s',outcome)
                    consumer.commit() # DB projection committed first; order_id unique makes retries idempotent
        except Exception:
            log.exception('Kafka connection/processing error, retry in 5s')
            time.sleep(5)

@app.on_event('startup')
def startup():
    init()
    threading.Thread(target=worker,daemon=True).start()
    for eid in pending():jobq.put(eid)
    if MODE=='kafka':threading.Thread(target=kafka_loop,daemon=True).start()

@app.get('/')
def homepage():return FileResponse(Path(__file__).parent/'static'/'index.html')
@app.get('/api/dashboard')
def api_dashboard():
    result=dashboard();result['mode']=MODE;return result
@app.post('/api/demo/order')
def demo_order(item:Sample):
    if MODE!='demo':raise HTTPException(403,'demo orders disabled in kafka mode')
    if not product(item.product_id):raise HTTPException(404,'unknown product')
    return submit({'order_id':'DEMO-'+str(uuid.uuid4()),'product_id':item.product_id,'qty':item.quantity},'demo')
@app.get('/healthz')
def health():return {'status':'ok','mode':MODE}
