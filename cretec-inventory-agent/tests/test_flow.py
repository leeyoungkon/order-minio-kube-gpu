import store

def test_ingest_dedup_and_guard(tmp_path,monkeypatch):
    monkeypatch.setattr(store,'PATH',str(tmp_path/'test.db'))
    store.init()
    assert store.product('P001')['on_hand']==190
    event={'order_id':'one','product_id':'P001','qty':30}
    result=store.ingest(event,'demo')
    assert result['metrics']['policy_action']=='PURCHASE'
    assert result['metrics']['suggested_qty']==110
    assert store.ingest(event,'demo')['duplicate'] is True
    assert store.product('P001')['on_hand']==160
    eid=result['evaluation_id']
    store.complete(eid,'WATCH','잘못된 모의 LLM 판단',[{'tool':'get_inventory'}])
    ev=store.evaluation(eid)
    assert ev['verdict']=='정책 불일치·교정' and ev['effective_action']=='PURCHASE'
    assert len(store.dashboard()['po_drafts'])==1
    event2={'order_id':'two','product_id':'P001','qty':20}
    r=store.ingest(event2,'demo')
    assert r['evaluation_id'] is None # cooldown, no duplicate PO
    assert len(store.dashboard()['po_drafts'])==1
