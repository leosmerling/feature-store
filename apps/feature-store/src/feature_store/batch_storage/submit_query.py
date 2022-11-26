from datetime import datetime
from typing import Dict, List
from hopeit.app.context import EventContext
from hopeit.dataobjects import dataclass, dataobject

__steps__ = ["dummy"]

@dataobject
@dataclass
class QueryEntity:
    entity_type: str
    feature_names: List[str]

@dataobject
@dataclass
class Query:
    fields: List[QueryEntity]
    source: str

# @dataobject
# @dataclass
# class QueryItem:
#     row_id: str
#     timestamp: datetime
#     entity_keys: Dict[str, str] # entity_type -> entity_key

##
## row1 2022-11-24 supplier.supplier1 product.product1 supplier.supplier_delay supplier_volume product_size product_weight
## row2 2022-11-24 supplier.supplier2 product.product1 supplier.supplier_delay supplier_volume product_size product_weight

## -->

## timestamp    supplier    product     delay   volume  size    weight
## 2022-11-24   supplier1   product1    10      100     "XL"    5
## 2022-11-24   supplier2   product1    5       500     "XL"    5

def dummy(payload: None, context: EventContext) -> None:
    pass
