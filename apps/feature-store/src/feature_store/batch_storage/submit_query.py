from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from pathlib import Path
from glob import glob

import pandas as pd

from hopeit.app.context import EventContext
from hopeit.dataobjects import dataclass, dataobject
from hopeit.app.events import Spawn, SHUFFLE
from hopeit.app.api import event_api

from feature_store.datamodel import Job, JobPartition, Query, QueryEntity, SeedData


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
    "spawn_partitions",
    SHUFFLE,
    "process_partition",
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


async def spawn_partitions(job: Job, context: EventContext) -> Spawn[JobPartition]:
    sorted_data = sorted(job.query.seed_data, key=lambda x: x.ts)

    for field in job.query.fields:
    
        for feature_name in field.feature_names:
            seed_row = sorted_data[0]
            current_partition_key = seed_row.ts.strftime("%Y/%m/%d/%H")
            current_entity_key = seed_row.entity_keys[field.entity_type]
            timestamps = defaultdict(list)
            timestamps[current_entity_key].append((seed_row.index, seed_row.ts))
    
            for seed_row in sorted_data[1:]:
                partition_key = seed_row.ts.strftime("%Y/%m/%d/%H")
    
                if current_partition_key != partition_key:
    
                    for k, vs in timestamps.items():
                        yield JobPartition(
                            partition_key=current_partition_key,
                            index_timestamps=vs,
                            entity_key=k,
                            feature_name=feature_name,
                        )
    
                    current_partition_key = partition_key
                    timestamps.clear()
    
                current_entity_key = seed_row.entity_keys[field.entity_type]
    
                timestamps[current_entity_key].append((seed_row.index, seed_row.ts))
        
            if len(timestamps):
    
                for k, vs in timestamps.items():
                    yield JobPartition(
                        partition_key=current_partition_key,
                        index_timestamps=vs,
                        entity_key=k,
                        feature_name=feature_name,
                    )
            

async def iter_lookback_partitions(partition_key: str, max_lookback_hours: int):
    yield partition_key
    current_partition = partition_key
    dt = datetime.strptime(partition_key, "%Y/%m/%d/%H")
    delta = timedelta(hours=1)
    for _ in range(max_lookback_hours):
        dt = dt - delta
        yield dt.strftime("%Y/%m/%d/%H")


async def extract_values(df: pd.DataFrame, index_timestamps: List[Tuple[int, datetime]]) -> pd.DataFrame:
    df.set_index("ts", inplace=True)
    df.sort_index(ascending=False)

    for index, ts in index_timestamps:
        aux_df = df.loc[df.index <= ts.isoformat()].head(1)
        print("*** aux_df", index, ts, aux_df)


async def process_partition(partition: JobPartition, context: EventContext) -> JobPartition:
    # Find folders for feature_name/entity_key
    print()
    print("*** process_item", partition)
    
    base_path = Path("_data/0.1/feature_store.0x1.batch_storage.save.path")

    async for partition_key in iter_lookback_partitions(partition.partition_key, max_lookback_hours=24):
        path = f"{partition.feature_name}/{partition.entity_key}/{partition_key}/*.parquet"
        files = glob(path, root_dir=base_path)
        if len(files):
            df = pd.concat((
                pd.read_parquet(Path(base_path) / file)
                for file in files
            ))
        # df = pd.read_parquet(Path(base_path) / path).set_index("ts").sort_index(ascending=False)
            print("*** FOUND!", partition, partition_key)
            print(df.head())

            # TODO: Extract latest value x timestamp
            await extract_values(df, partition.index_timestamps)

            break

    print()
    return partition

