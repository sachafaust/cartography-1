"""Benchmark 1: overhead of ensure_indexes() being called on every load().

Measures:
  a) # of CREATE INDEX statements issued per load() for a real schema (EC2InstanceSchema)
  b) latency of ensure_indexes() when indexes already exist (the steady-state case)
  c) load() of a small list WITH the ensure_indexes call vs bypassing it (load_graph_data directly)
"""

import statistics
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from common import get_session, wipe, seed_account, make_ec2_instances, ACCOUNT_ID

from cartography.client.core.tx import ensure_indexes, load, load_graph_data
from cartography.graph.querybuilder import build_create_index_queries, build_ingestion_query
from cartography.models.aws.ec2.instances import EC2InstanceSchema

UPDATE_TAG = 1000

session = get_session()
wipe(session)
seed_account(session, UPDATE_TAG)

schema = EC2InstanceSchema()
queries = build_create_index_queries(schema)
print(f"CREATE INDEX statements per load() call for EC2InstanceSchema: {len(queries)}")

# Warm up: create the indexes once
ensure_indexes(session, schema)

# b) steady-state ensure_indexes latency (indexes already exist)
samples = []
for _ in range(50):
    t0 = time.perf_counter()
    ensure_indexes(session, schema)
    samples.append(time.perf_counter() - t0)
print(
    f"ensure_indexes() steady-state latency: median={statistics.median(samples)*1000:.1f}ms "
    f"mean={statistics.mean(samples)*1000:.1f}ms p95={sorted(samples)[int(len(samples)*0.95)]*1000:.1f}ms"
)
per_q = statistics.median(samples) / len(queries) * 1000
print(f"  -> per index statement: {per_q:.2f}ms (each is a separate autocommit round trip)")

# c) small-batch load with vs without ensure_indexes (typical intel module loads are small)
small = make_ec2_instances(25)
kwargs = dict(Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=UPDATE_TAG)

with_samples = []
for _ in range(30):
    t0 = time.perf_counter()
    load(session, schema, small, **kwargs)
    with_samples.append(time.perf_counter() - t0)

q = build_ingestion_query(schema)
without_samples = []
for _ in range(30):
    t0 = time.perf_counter()
    load_graph_data(session, q, small, **kwargs)
    without_samples.append(time.perf_counter() - t0)

wm = statistics.median(with_samples) * 1000
wom = statistics.median(without_samples) * 1000
print(f"load() of 25 items WITH per-call ensure_indexes:    median {wm:.1f}ms")
print(f"load() of 25 items WITHOUT per-call ensure_indexes: median {wom:.1f}ms")
print(f"  -> overhead per load call: {wm-wom:.1f}ms ({(wm-wom)/wm*100:.0f}% of a small load)")
session.close()
