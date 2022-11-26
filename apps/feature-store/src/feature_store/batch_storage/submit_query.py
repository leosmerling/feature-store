from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4
from pathlib import Path
from glob import iglob

import pandas as pd

from hopeit.app.context import EventContext
from hopeit.dataobjects import dataclass, dataobject
from hopeit.app.events import Spawn, SHUFFLE
from hopeit.app.api import event_api

from feature_store.datamodel import Job, JobItem, Query, QueryEntity, SeedData


__api__ = event_api(
    summary="Feature Store: Submit Batch Query",
    payload=(Query, "Query specification"),
    responses={
        200: (Job, 'Generated job information'),
    }
)


__steps__ = [
    "submit_query",
    SHUFFLE,  # <- Return response here
    "spawn_items",
    SHUFFLE,
    "process_item",
]


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


async def submit_query(payload: Query, context: EventContext) -> Job:
    job_id = str(uuid4())
    return Job(
        id = job_id,
        results_location=f"_data/{job_id}",
        query=payload,
    )


async def spawn_items(job: Job, context: EventContext) -> Spawn[JobItem]:
    for seed_row in job.query.seed_data:
        for field in job.query.fields:
            entity_key = seed_row.entity_keys[field.entity_type]
            for feature_name in field.feature_names:
                yield JobItem(
                    index=seed_row.index,
                    ts=seed_row.ts,
                    entity_key=entity_key,
                    feature_name=feature_name
                )


async def process_item(item: JobItem, context: EventContext) -> JobItem:
    # Find folders for feature_name/entity_key
    print()
    print("*** process_item", item)
    
    base_path = Path("_data/0.1/feature_store.0x1.batch_storage.save.path")
    path = f"{item.feature_name}/{item.entity_key}/????/??/??/??/*.parquet"
    locations = reversed(sorted(iglob(path, root_dir=base_path)))

    dt_match_max = item.ts.strftime("%Y%m%d%H")
    dt_max = item.ts.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    for location in locations:
        dt_match = "".join(location.split("/")[2:5])
        print("*** dt_max", dt_max, "dt_match", dt_match, "location", location)

        if dt_match_max >= dt_match:
            df = pd.read_parquet(Path(base_path) / location).set_index("ts").sort_index(ascending=False)
            df = df.loc[df.index <= dt_max].head(1)
            print("*** FOUND!", df)
            break

    print()
    return item
