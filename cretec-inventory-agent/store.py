"""SQLite is intentionally a single-replica educational materialized view, not the ERP stock ledger."""
import json, os, sqlite3, time
from contextlib import contextmanager
from pathlib import Path
from engine import PRODUCTS, inventory_metrics
PATH=os.getenv('STATE_DB','./inventory_agent.db')

@contextmanager
def db():
    Path(PATH).parent.mkdir(parents=True,exist_ok=True)
    c=sqlite3.connect(PATH,timeout=20)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA busy_timeout=20000')
    try:
        yield c
        c.commit()
    except:
        c.rollback();raise
    finally:c.close()

def init():
    with db() as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript('''
        CREATE TABLE IF NOT EXISTS products (
          product_id TEXT PRIMARY KEY, name TEXT, on_hand INTEGER NOT NULL,
          on_order INTEGER NOT NULL, allocated INTEGER NOT NULL, daily_demand REAL NOT NULL,
          lead_days REAL NOT NULL, safety_stock INTEGER NOT NULL, review_days REAL NOT NULL,
          moq INTEGER NOT NULL, pack_size INTEGER NOT NULL, updated_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS event_log (
          order_id TEXT PRIMARY KEY, product_id TEXT NOT NULL, qty INTEGER NOT NULL,
          event_time REAL NOT NULL, received_at REAL NOT NULL, source TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT, product_id TEXT NOT NULL, ts REAL NOT NULL,
          on_hand INTEGER, position INTEGER, reorder_point INTEGER, order_qty INTEGER,
          action TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS evaluations (
          id INTEGER PRIMARY KEY AUTOINCREMENT, product_id TEXT NOT NULL,
          created_at REAL NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          policy_action TEXT NOT NULL, agent_action TEXT, effective_action TEXT,
          verdict TEXT, explanation TEXT, trace TEXT DEFAULT '[]',
          policy_qty INTEGER, actual_qty INTEGER DEFAULT 0, error TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS po_drafts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, product_id TEXT NOT NULL,
          evaluation_id INTEGER NOT NULL, qty INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING_APPROVAL',
          created_at REAL NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS one_active_draft ON po_drafts(product_id) WHERE status='PENDING_APPROVAL';
        ''')
        now=time.time()
        for p in PRODUCTS:
            cols=list(p.keys())+['updated_at']
            c.execute(f"INSERT OR IGNORE INTO products ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",[p[k] for k in p]+[now])
        for p in c.execute('SELECT * FROM products').fetchall():
            if not c.execute('SELECT 1 FROM snapshots WHERE product_id=?',(p['product_id'],)).fetchone():
                m=inventory_metrics(dict(p))
                c.execute('INSERT INTO snapshots(product_id,ts,on_hand,position,reorder_point,order_qty,action) VALUES(?,?,?,?,?,?,?)',
                          (p['product_id'],now,p['on_hand'],m['inventory_position'],m['reorder_point'],0,m['policy_action']))

def products():
    with db() as c:
        result=[]
        for r in c.execute('SELECT * FROM products ORDER BY product_id').fetchall():
            p=dict(r);p.update(inventory_metrics(p))
            p['active_draft']=c.execute("SELECT qty FROM po_drafts WHERE product_id=? AND status='PENDING_APPROVAL'",(p['product_id'],)).fetchone()
            p['active_draft']=int(p['active_draft']['qty']) if p['active_draft'] else 0
            e=c.execute('SELECT * FROM evaluations WHERE product_id=? ORDER BY id DESC LIMIT 1',(p['product_id'],)).fetchone()
            p['latest_evaluation']=dict(e) if e else None
            result.append(p)
        return result

def product(pid):
    with db() as c:
        x=c.execute('SELECT * FROM products WHERE product_id=?',(pid,)).fetchone()
        return dict(x) if x else None

def recent(pid,minutes=60):
    with db() as c:
        now=time.time()
        row=c.execute('SELECT COUNT(*) AS n,COALESCE(SUM(qty),0) AS qty FROM event_log WHERE product_id=? AND received_at>=?',(pid,now-minutes*60)).fetchone()
        history=[dict(x) for x in c.execute('SELECT received_at AS ts, qty FROM event_log WHERE product_id=? ORDER BY received_at DESC LIMIT 100',(pid,)).fetchall()]
        return {'minutes':minutes,'events':row['n'],'qty':row['qty'],'history':list(reversed(history))}

def ingest(order,source):
    pid=order['product_id'];now=time.time()
    with db() as c:
        p=c.execute('SELECT * FROM products WHERE product_id=?',(pid,)).fetchone()
        if not p:return {'error':'unknown_product','product_id':pid}
        cur=c.execute('INSERT OR IGNORE INTO event_log(order_id,product_id,qty,event_time,received_at,source) VALUES(?,?,?,?,?,?)',
                (order['order_id'],pid,order['qty'],order.get('event_time') or now,now,source))
        if not cur.rowcount:return {'duplicate':True}
        # Educational projection assumes simulator writes orders ONLY, not the authoritative stock ledger.
        c.execute('UPDATE products SET on_hand=MAX(0,on_hand-?),updated_at=? WHERE product_id=?',(order['qty'],now,pid))
        current=dict(c.execute('SELECT * FROM products WHERE product_id=?',(pid,)).fetchone());m=inventory_metrics(current)
        c.execute('INSERT INTO snapshots(product_id,ts,on_hand,position,reorder_point,order_qty,action) VALUES(?,?,?,?,?,?,?)',
                  (pid,now,current['on_hand'],m['inventory_position'],m['reorder_point'],order['qty'],m['policy_action']))
        latest=c.execute('SELECT created_at,policy_action FROM evaluations WHERE product_id=? ORDER BY id DESC LIMIT 1',(pid,)).fetchone()
        interval=int(os.getenv('EVALUATION_COOLDOWN_SECONDS','30'))
        evaluate = latest is None or now-latest['created_at']>=interval or latest['policy_action']!=m['policy_action']
        eid=None
        if evaluate:
            cur=c.execute('INSERT INTO evaluations(product_id,created_at,status,policy_action,policy_qty) VALUES(?,?,?,?,?)',
                          (pid,now,'pending',m['policy_action'],m['suggested_qty']))
            eid=cur.lastrowid
        return {'duplicate':False,'evaluation_id':eid,'metrics':m}

def pending():
    with db() as c:return [int(r['id']) for r in c.execute("SELECT id FROM evaluations WHERE status='pending' ORDER BY id").fetchall()]

def evaluation(eid):
    with db() as c:
        x=c.execute('SELECT * FROM evaluations WHERE id=?',(eid,)).fetchone()
        return dict(x) if x else None

def mark_running(eid):
    with db() as c:
        cur=c.execute("UPDATE evaluations SET status='running' WHERE id=? AND status='pending'",(eid,))
        return cur.rowcount>0

def complete(eid, agent_action, explanation, trace, error=''):
    with db() as c:
        e=c.execute('SELECT * FROM evaluations WHERE id=?',(eid,)).fetchone()
        if not e:return
        p=dict(c.execute('SELECT * FROM products WHERE product_id=?',(e['product_id'],)).fetchone())
        m=inventory_metrics(p)
        # Guardrail: enforce CURRENT recomputed facts, not stale initial snapshot or free-form LLM arithmetic.
        expected=m['policy_action']
        valid=agent_action in ('PURCHASE','WATCH')
        agreement=valid and agent_action==expected
        effective=expected
        verdict='일치' if agreement else ('모델 오류/미응답' if not valid else '정책 불일치·교정')
        if error:verdict='LLM 실패·정책 엔진 대체'
        c.execute('UPDATE evaluations SET status=?,agent_action=?,effective_action=?,verdict=?,explanation=?,trace=?,policy_qty=?,error=? WHERE id=?',
                  ('error' if error else 'done',agent_action,effective,verdict,explanation,json.dumps(trace,ensure_ascii=False),m['suggested_qty'],error,eid))
        if effective=='PURCHASE':
            exists=c.execute("SELECT id FROM po_drafts WHERE product_id=? AND status='PENDING_APPROVAL'",(e['product_id'],)).fetchone()
            if not exists:
                c.execute('INSERT INTO po_drafts(product_id,evaluation_id,qty,created_at) VALUES(?,?,?,?)',
                          (e['product_id'],eid,m['suggested_qty'],time.time()))

def fail(eid, reason):
    complete(eid,None,'LLM 분석 실패: '+reason+'. 정책 엔진에 따른 검토 상태를 표시합니다.',[],reason)

def dashboard():
    with db() as c:
        return {'products':products(),
                'evaluations':[dict(r) for r in c.execute('SELECT * FROM evaluations ORDER BY id DESC LIMIT 30')],
                'po_drafts':[dict(r) for r in c.execute('SELECT * FROM po_drafts ORDER BY id DESC LIMIT 30')],
                'history':{p['product_id']:[dict(r) for r in c.execute('SELECT ts,on_hand,position,reorder_point,order_qty,action FROM snapshots WHERE product_id=? ORDER BY id DESC LIMIT 100',(p['product_id'],)).fetchall()][::-1] for p in PRODUCTS}}
