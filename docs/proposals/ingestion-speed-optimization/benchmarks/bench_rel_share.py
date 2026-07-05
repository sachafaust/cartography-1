"""Benchmark 6: what fraction of load() time is the relationship-attachment CALL subquery?"""

import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from common import get_session, wipe, seed_account, make_ec2_instances, ACCOUNT_ID

from cartography.client.core.tx import ensure_indexes, load_graph_data
from cartography.graph.querybuilder import build_ingestion_query
from cartography.models.aws.ec2.instances import EC2InstanceSchema

N = 100000
TAG = 1000
schema = EC2InstanceSchema()
session = get_session()

full_q = build_ingestion_query(schema)
node_only_q = build_ingestion_query(schema, selected_relationships=set())
subres_only_q = build_ingestion_query(schema, {schema.sub_resource_relationship})

kwargs = dict(Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=TAG)

for label, q in [("node-only (no rels)", node_only_q),
                 ("node + sub-resource rel", subres_only_q),
                 ("full (sub-resource + 3 other rels)", full_q)]:
    wipe(session)
    seed_account(session, TAG)
    ensure_indexes(session, schema)
    data = make_ec2_instances(N)
    t0 = time.perf_counter()
    load_graph_data(session, q, data, **kwargs)
    create_dt = time.perf_counter() - t0
    t0 = time.perf_counter()
    load_graph_data(session, q, data, **kwargs)
    update_dt = time.perf_counter() - t0
    print(f"{label:38s} create: {create_dt:6.2f}s ({N/create_dt:6.0f}/s)  "
          f"update: {update_dt:6.2f}s ({N/update_dt:6.0f}/s)")
session.close()
