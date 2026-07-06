"""Benchmark 5: cleanup query anchoring on a DENSE account node.

Real deployments have one AWSAccount node with rels to *every* resource in the account.
The generated cleanup query MATCH (n:Type)<-[s:RESOURCE]-(:AWSAccount{id})
plans as: NodeIndexSeek(account) -> Expand(All) over ALL account rels -> Filter by label+lastupdated.
So each cleanup statement's cost is O(total account degree), not O(stale nodes of that type).

We simulate: 200k "OtherResource" nodes + 10k fresh EC2Instance attached to one account.
Then compare the current generated query vs a label-anchored rewrite, both no-op and with stale rows.
"""

import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from common import get_session, wipe, seed_account, make_ec2_instances, ACCOUNT_ID

from cartography.client.core.tx import load
from cartography.graph.cleanupbuilder import build_cleanup_queries
from cartography.models.aws.ec2.instances import EC2InstanceSchema

NEW_TAG = 2000
OLD_TAG = 1000

session = get_session()
wipe(session)
seed_account(session, NEW_TAG)

# Dense account: 200k other resources (in 10k chunks to stay under tx memory)
print("Seeding dense account (200k OtherResource nodes)...")
for start in range(0, 200000, 10000):
    session.run(
        """
        MATCH (a:AWSAccount{id: $id})
        UNWIND range($start, $end) AS x
        CREATE (n:OtherResource{id: 'other-' + toString(x), lastupdated: $tag})
        CREATE (a)-[:RESOURCE{lastupdated: $tag}]->(n)
        """,
        id=ACCOUNT_ID, start=start, end=start + 9999, tag=NEW_TAG,
    ).consume()

# 10k fresh EC2 instances via the real load path (also creates indexes)
schema = EC2InstanceSchema()
fresh = make_ec2_instances(10000)
load(session, schema, fresh, Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=NEW_TAG)

queries = build_cleanup_queries(schema)
node_cleanup_query = queries[0].replace(";", "")  # the DETACH DELETE n statement
print("Current generated node-cleanup query:")
print(node_cleanup_query)

LABEL_ANCHORED = """
MATCH (n:EC2Instance)
WHERE n.lastupdated <> $UPDATE_TAG
MATCH (n)<-[s:RESOURCE]-(:AWSAccount{id: $AWS_ID})
WITH n LIMIT $LIMIT_SIZE
DETACH DELETE n
"""

params = {"UPDATE_TAG": NEW_TAG, "AWS_ID": ACCOUNT_ID, "LIMIT_SIZE": 100}


def run_iterative(query, params):
    """Mimic GraphStatement._run_iterative."""
    iterations = 0
    while True:
        summary = session.run(query, params).consume()
        iterations += 1
        if not summary.counters.contains_updates:
            break
    return iterations


def timed_noop(label, query):
    # single no-op execution (what every sync pays when nothing is stale)
    ts = []
    for _ in range(5):
        t0 = time.perf_counter()
        session.run(query, params).consume()
        ts.append(time.perf_counter() - t0)
    print(f"  no-op single iteration [{label}]: min {min(ts)*1000:.0f}ms")


print(f"\nDense account degree: 210k rels. EC2Instance count: 10k (all fresh).")
timed_noop("current (account-anchored)", node_cleanup_query)
timed_noop("label-anchored rewrite   ", LABEL_ANCHORED)

# Now with stale nodes: mark 5k EC2 instances stale and delete them via both, iterationsize=100
for label, query in [("current", node_cleanup_query), ("label-anchored", LABEL_ANCHORED)]:
    session.run(
        "MATCH (n:EC2Instance) WITH n LIMIT 5000 SET n.lastupdated = $old",
        old=OLD_TAG,
    ).consume()
    t0 = time.perf_counter()
    iters = run_iterative(query, params)
    dt = time.perf_counter() - t0
    print(f"  delete 5000 stale (iterationsize=100) [{label}]: {dt:.2f}s over {iters} iterations")
    # restore
    fresh_reload = make_ec2_instances(10000)
    load(session, schema, fresh_reload, Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=NEW_TAG)

session.close()
