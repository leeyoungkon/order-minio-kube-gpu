"""Deterministic inventory math; LLM does not do arithmetic or write ERP orders."""
import math

PRODUCTS = [
    {"product_id":"P001","name":"산업용 절삭공구","on_hand":190,"on_order":0,"allocated":15,"daily_demand":20,"lead_days":5,"safety_stock":50,"review_days":5,"moq":50,"pack_size":10},
    {"product_id":"P002","name":"작업용 장갑","on_hand":500,"on_order":100,"allocated":40,"daily_demand":30,"lead_days":4,"safety_stock":70,"review_days":5,"moq":100,"pack_size":50},
    {"product_id":"P003","name":"산업용 베어링","on_hand":70,"on_order":0,"allocated":5,"daily_demand":8,"lead_days":8,"safety_stock":35,"review_days":7,"moq":20,"pack_size":10},
    {"product_id":"P004","name":"전동공구 부속품","on_hand":350,"on_order":0,"allocated":20,"daily_demand":12,"lead_days":6,"safety_stock":50,"review_days":7,"moq":30,"pack_size":10},
]

def inventory_metrics(p: dict) -> dict:
    position = int(p['on_hand']) + int(p['on_order']) - int(p['allocated'])
    reorder = math.ceil(float(p['daily_demand']) * float(p['lead_days']) + int(p['safety_stock']))
    target = math.ceil(float(p['daily_demand']) * (float(p['lead_days'])+float(p['review_days'])) + int(p['safety_stock']))
    deficit = max(0, target-position)
    rounded = max(int(p['moq']), math.ceil(deficit / int(p['pack_size'])) * int(p['pack_size'])) if deficit else 0
    # Inventory position is the trigger basis, on_hand is deliberately distinct.
    action = 'PURCHASE' if position <= reorder else 'WATCH'
    return {"inventory_position":position, "reorder_point":reorder, "order_up_to":target,
            "suggested_qty":rounded if action == 'PURCHASE' else 0,
            "policy_action":action,
            "days_of_supply":round(max(0, int(p['on_hand'])-int(p['allocated']))/float(p['daily_demand']),2),
            "safety_stock":int(p['safety_stock'])}


def parse_cdc(payload: dict) -> dict | None:
    """Debezium JSON envelopes, INSERT only. Strict on required product/qty/order id."""
    if not isinstance(payload, dict): return None
    data=payload.get('payload',payload)
    if not isinstance(data,dict) or data.get('op')!='c': return None
    after=data.get('after')
    if not isinstance(after,dict): return None
    pid=after.get('product_id', after.get('product_code'))
    oid=after.get('order_id',after.get('id'))
    qty=after.get('quantity',after.get('qty'))
    if pid is None or oid is None or qty is None: raise ValueError('Expected id/order_id, product_id and quantity/qty in orders row')
    qty=int(qty)
    if qty<=0: raise ValueError('quantity must be >0')
    return {'order_id':str(oid),'product_id':str(pid),'qty':qty,
            'event_time':float((data.get('source') or {}).get('ts_ms') or data.get('ts_ms') or 0)/1000 or None}
