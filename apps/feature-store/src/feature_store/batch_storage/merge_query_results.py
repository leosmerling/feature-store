from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from pathlib import Path
from glob import glob
import os

import pandas as pd

from hopeit.app.context import EventContext
from hopeit.dataobjects import dataclass, dataobject
from hopeit.app.events import Spawn, SHUFFLE
from hopeit.app.api import event_api

from feature_store.datamodel import Job, JobPartition, QueryResults, QueryEntity, SeedData


__api__ = event_api(
    summary="Feature Store: Merge Query Results",
    payload=(QueryResults, "Job id"),
    responses={
        200: (str, 'Saved path'),
    }
)


__steps__ = [
    "merge",
]


async def merge(payload: QueryResults, context: EventContext) -> QueryResults:
    data_path = Path("_data/0.1/feature_store.0x1.submit_query.save.path") / payload.job_id

    df = None
    for file in glob("*.csv", root_dir=data_path):
        part_df = pd.read_csv(data_path / file).set_index("seed.index")
        if df is None:
            df = part_df
        else:
            df = df.merge(part_df, left_index=True, right_index=True, how='outer', suffixes=(None, "_"))

            new_cols = (col for col in df.columns if col[-1]=='_')
            for col in new_cols:
                df[col[:-1]] = df[col[:-1]].fillna(df[col])

            keep_cols = [col for col in df.columns if col[-1]!='_']
            df = df[
                sorted(col for col in keep_cols if col[:4]=="seed")
                + sorted(col for col in keep_cols if col[:6]=="entity")
                + sorted(col for col in keep_cols if col[:7]=="feature")
            ]

    
    print("\n*** MERGED\n", df, "\n")

    save_path = Path("_data/0.1/feature_store.0x1.merge_query_results.save.path") / payload.job_id
    os.makedirs(save_path, exist_ok=True)
    df.to_csv(save_path / f"{payload.job_id}.csv")

    return payload
