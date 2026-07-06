"""Benchmark 2: cleanup job iterationsize (default 100) vs larger batches.

Uses the REAL code path: GraphJob.from_node_schema(EC2InstanceSchema(), ...)
Seeds N stale EC2Instance nodes attached to an AWSAccount, then times the cleanup job.
"""

import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from common import get_session, wipe, seed_account, make_ec2_instances, ACCOUNT_ID

from cartography.client.core.tx import load
from cartography.graph.job import GraphJob
from cartography.models.aws.ec2.instances import EC2InstanceSchema

OLD_TAG = 1000
NEW_TAG = 2000
N = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
SIZES = [100, 1000, 10000]

session = get_session()
schema = EC2InstanceSchema()

results = {}
for iterationsize in SIZES:
    wipe(session)
    seed_account(session, NEW_TAG)
    # Seed N nodes with OLD tag (stale), plus 1000 fresh ones with NEW tag
    data = make_ec2_instances(N)
    load(session, schema, data, Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=OLD_TAG)
    fresh = make_ec2_instances(1000)
    for d in fresh:
        d["InstanceId"] = "fresh-" + d["InstanceId"]
    load(session, schema, fresh, Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=NEW_TAG)

    params = {"UPDATE_TAG": NEW_TAG, "AWS_ID": ACCOUNT_ID}
    job = GraphJob.from_node_schema(schema, params, iterationsize=iterationsize)
    t0 = time.perf_counter()
    job.run(session)
    dt = time.perf_counter() - t0
    remaining = session.run("MATCH (n:EC2Instance) RETURN count(n) AS c").single()["c"]
    print(f"iterationsize={iterationsize:>6}: cleanup of {N} stale nodes took {dt:.2f}s "
          f"({N/dt:.0f} deletes/s); remaining fresh nodes: {remaining}")
    results[iterationsize] = dt

base = results[SIZES[0]]
for s in SIZES[1:]:
    print(f"speedup {SIZES[0]} -> {s}: {base/results[s]:.1f}x")

# Also measure the fixed cost of a no-op cleanup job (nothing stale) since every sync
# runs hundreds of these.
wipe(session)
seed_account(session, NEW_TAG)
fresh = make_ec2_instances(5000)
load(session, schema, fresh, Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=NEW_TAG)
params = {"UPDATE_TAG": NEW_TAG, "AWS_ID": ACCOUNT_ID}
job = GraphJob.from_node_schema(schema, params, iterationsize=100)
t0 = time.perf_counter()
job.run(session)
print(f"no-op cleanup job (0 stale, 5k fresh): {time.perf_counter()-t0:.3f}s")
session.close()
