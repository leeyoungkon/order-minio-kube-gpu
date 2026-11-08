from engine import inventory_metrics,parse_cdc

def base(**k):
    p=dict(on_hand=120,on_order=0,allocated=0,daily_demand=20,lead_days=5,safety_stock=50,review_days=5,moq=50,pack_size=10)
    p.update(k)
    return p

def test_buy():
    m=inventory_metrics(base())
    assert (m['reorder_point'],m['inventory_position'],m['policy_action'],m['suggested_qty'])==(150,120,'PURCHASE',130)

def test_watch():
    m=inventory_metrics(base(on_hand=170))
    assert m['policy_action']=='WATCH' and m['suggested_qty']==0

def test_allocated_and_on_order():
    m=inventory_metrics(base(on_hand=120,on_order=100,allocated=20))
    assert m['inventory_position']==200 and m['policy_action']=='WATCH'

def test_debezium():
    assert parse_cdc({'payload':{'op':'c','after':{'id':44,'product_id':'P001','quantity':12}}})['qty']==12
    assert parse_cdc({'payload':{'op':'u','after':{'id':44,'product_id':'P001','quantity':12}}}) is None
    assert parse_cdc(None) is None
