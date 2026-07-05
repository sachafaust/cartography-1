"""Benchmark 3: legacy per-item run_write_query loops (N+1) vs a single batched UNWIND.

Reproduces the exact pattern from cartography/intel/aws/s3.py load_s3_buckets (one
run_write_query per bucket) vs the equivalent UNWIND batch write.
"""

import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from common import get_session, wipe, ACCOUNT_ID

from cartography.client.core.tx import run_write_query
from cartography.client.core.tx import execute_write_with_retry, write_list_of_dicts_tx

N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

# This is the actual query from s3.py:1005 (ingest_bucket), lightly trimmed
PER_ITEM_QUERY = """
MERGE (bucket:S3Bucket{id:$BucketName})
ON CREATE SET bucket.firstseen = timestamp(), bucket.creationdate = $CreationDate
SET bucket.name = $BucketName, bucket.region = $BucketRegion, bucket.arn = $Arn,
    bucket.lastupdated = $aws_update_tag
WITH bucket
MATCH (owner:AWSAccount{id: $AWS_ACCOUNT_ID})
MERGE (owner)-[r:RESOURCE]->(bucket)
ON CREATE SET r.firstseen = timestamp()
SET r.lastupdated = $aws_update_tag
"""

BATCHED_QUERY = """
UNWIND $Buckets AS bucket_data
MERGE (bucket:S3Bucket{id:bucket_data.BucketName})
ON CREATE SET bucket.firstseen = timestamp(), bucket.creationdate = bucket_data.CreationDate
SET bucket.name = bucket_data.BucketName, bucket.region = bucket_data.BucketRegion,
    bucket.arn = bucket_data.Arn, bucket.lastupdated = $aws_update_tag
WITH bucket
MATCH (owner:AWSAccount{id: $AWS_ACCOUNT_ID})
MERGE (owner)-[r:RESOURCE]->(bucket)
ON CREATE SET r.firstseen = timestamp()
SET r.lastupdated = $aws_update_tag
"""

session = get_session()
wipe(session)
session.run("MERGE (a:AWSAccount{id: $id})", id=ACCOUNT_ID)
session.run("CREATE INDEX IF NOT EXISTS FOR (n:S3Bucket) ON (n.id)")
session.run("CREATE INDEX IF NOT EXISTS FOR (n:AWSAccount) ON (n.id)")

buckets = [
    {
        "BucketName": f"bucket-{i}",
        "CreationDate": "2024-01-01",
        "BucketRegion": "us-east-1",
        "Arn": f"arn:aws:s3:::bucket-{i}",
    }
    for i in range(N)
]

# Pattern A: per-item run_write_query (current s3.py / elbv2 / tgw / redshift pattern)
t0 = time.perf_counter()
for b in buckets:
    run_write_query(
        session,
        PER_ITEM_QUERY,
        BucketName=b["BucketName"],
        CreationDate=b["CreationDate"],
        BucketRegion=b["BucketRegion"],
        Arn=b["Arn"],
        AWS_ACCOUNT_ID=ACCOUNT_ID,
        aws_update_tag=1000,
    )
per_item_dt = time.perf_counter() - t0
print(f"per-item run_write_query x {N}: {per_item_dt:.2f}s ({N/per_item_dt:.0f} items/s)")

# Reset bucket nodes so MERGE work is comparable
session.run("MATCH (n:S3Bucket) DETACH DELETE n")

# Pattern B: single UNWIND batch (same data, one transaction)
t0 = time.perf_counter()
execute_write_with_retry(
    session,
    write_list_of_dicts_tx,
    BATCHED_QUERY,
    Buckets=buckets,
    AWS_ACCOUNT_ID=ACCOUNT_ID,
    aws_update_tag=1000,
)
batched_dt = time.perf_counter() - t0
print(f"batched UNWIND x {N}:          {batched_dt:.2f}s ({N/batched_dt:.0f} items/s)")
print(f"speedup: {per_item_dt/batched_dt:.1f}x (localhost RTT ~0.2ms; remote/Aura RTT 10-50ms would widen this)")
session.close()
