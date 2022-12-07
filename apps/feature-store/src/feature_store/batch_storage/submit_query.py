from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Optional, Tuple
from uuid import uuid4
from pathlib import Path
from glob import glob
import os
import aiofiles

import pandas as pd

from hopeit.app.context import EventContext
from hopeit.dataobjects import dataclass, dataobject
from hopeit.app.events import Spawn, SHUFFLE
from hopeit.app.api import event_api

from feature_store.datamodel import Job, JobPartition, Query, QueryEntity, SeedData, SubmitQuerySettings


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


async def submit_query(payload: Query, context: EventContext) -> Job:
    job_id = str(uuid4())
    return Job(
        job_id=job_id,
        query=payload,
    )


async def read_seed_data(path: Path) -> Iterator[SeedData]:
    async with aiofiles.open(path) as f:
        header = None
        async for line in f:
            if header is None:
                header = line.strip('\n').split(',')
                continue

            tokens = line.strip('\n').split(',')
            entity_keys = {
                k.split('.')[-1]: v
                for k, v in zip(header, tokens)
                if k[0:7]=="entity."
            }
            print("**** entity_keys", tokens, entity_keys)
            yield SeedData(
                index=int(tokens[0]),
                ts=datetime.fromisoformat(tokens[1]),
                entity_keys=entity_keys
            )



async def spawn_partitions(job: Job, context: EventContext) -> Spawn[JobPartition]:
    settings: SubmitQuerySettings = context.settings(datatype=SubmitQuerySettings)

    seed_data_path = Path(settings.seed_data_path) / job.query.seed_data_file

    for field in job.query.fields:
    
        for feature_name in field.feature_names:

            init = True
            async for seed_row in read_seed_data(seed_data_path):
                if init:
                    current_partition_key = seed_row.ts.strftime("%Y/%m/%d/%H")
                    current_entity_key = seed_row.entity_keys[field.entity_type]
                    timestamps = defaultdict(list)
                    timestamps[current_entity_key].append((seed_row.index, seed_row.ts))
                    init = False
                    continue

                partition_key = seed_row.ts.strftime("%Y/%m/%d/%H")
    
                if current_partition_key != partition_key:
    
                    for k, vs in timestamps.items():
                        yield JobPartition(
                            job_id=job.job_id,
                            partition_key=current_partition_key,
                            entity_type=field.entity_type,
                            entity_key=k,
                            feature_name=feature_name,
                            index_timestamps=vs,
                        )
    
                    current_partition_key = partition_key
                    timestamps.clear()
    
                current_entity_key = seed_row.entity_keys[field.entity_type]
    
                timestamps[current_entity_key].append((seed_row.index, seed_row.ts))
        
            if len(timestamps):
    
                for k, vs in timestamps.items():
                    yield JobPartition(
                        job_id=job.job_id,
                        partition_key=current_partition_key,
                        entity_type=field.entity_type,
                        entity_key=k,
                        feature_name=feature_name,
                        index_timestamps=vs,
                    )
            

async def iter_lookback_partitions(partition_key: str, max_lookback_hours: int):
    yield partition_key
    # current_partition = partition_key
    dt = datetime.strptime(partition_key, "%Y/%m/%d/%H")
    delta = timedelta(hours=1)
    for _ in range(max_lookback_hours):
        dt -= delta
        yield dt.strftime("%Y/%m/%d/%H")


async def extract_values(df: pd.DataFrame, partition: JobPartition) -> pd.DataFrame:
    df.set_index("ts", inplace=True)
    df.sort_index(ascending=False)
    return pd.concat(
        get_feature_df(df, index, ts, partition.entity_type, partition.feature_name)
        for index, ts in partition.index_timestamps
    )


def get_feature_df(df: pd.DataFrame, index:int, timestamp: datetime, entity_type: str, feature_name: str) -> pd.DataFrame:
    feature_df = df.loc[df.index <= timestamp.isoformat()].head(1)
    feature_df["seed.index"] = index
    feature_df["seed.ts"] = timestamp
    print("\n\n*** feature_df_temp\n\n", feature_df)
    feature_df[f"feature.{feature_name}.ts"] = feature_df.index
    feature_df[f"feature.{feature_name}.c"] = feature_df["c"] if "c" in feature_df.columns else ""
    feature_df[f"feature.{feature_name}.n"] = feature_df["n"] if "n" in feature_df.columns else pd.NA
    feature_df[f"entity.{entity_type}"] = feature_df["entity_key"]
    return feature_df.set_index("seed.index")[[
        "seed.ts",
        f"entity.{entity_type}",
        f"feature.{feature_name}.c",
        f"feature.{feature_name}.n",
        f"feature.{feature_name}.ts",
    ]]


def save_partition_result(df: pd.DataFrame, partition: JobPartition):
    save_path = Path("_data/0.1/feature_store.0x1.submit_query.save.path") / partition.job_id
    os.makedirs(save_path, exist_ok=True)
    df.to_csv(save_path / f"{uuid4()}.csv")


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
            print("*** FOUND!", partition, partition_key)
            feature_df = await extract_values(df, partition)
            print("*** feature_df", feature_df)
            save_partition_result(feature_df, partition)
            break

    print()
    return partition

