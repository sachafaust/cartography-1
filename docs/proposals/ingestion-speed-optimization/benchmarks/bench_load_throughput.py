"""Benchmark 4: end-to-end load() throughput with the real generated ingestion query,
plus batch_size sensitivity, plus PROFILE checks on the cleanup MATCH and CONTAINS matcher.
"""

import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from common import get_session, wipe, seed_account, make_ec2_instances, ACCOUNT_ID

from cartography.client.core.tx import load
from cartography.models.aws.ec2.instances import EC2InstanceSchema

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
UPDATE_TAG = 1000
schema = EC2InstanceSchema()
session = get_session()

for batch_size in [1000, 10000, 25000]:
    wipe(session)
    seed_account(session, UPDATE_TAG)
    data = make_ec2_instances(N)
    t0 = time.perf_counter()
    load(session, schema, data, batch_size=batch_size,
         Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=UPDATE_TAG)
    dt = time.perf_counter() - t0
    print(f"load() {N} EC2 instances (create), batch_size={batch_size:>6}: "
          f"{dt:.2f}s ({N/dt:.0f} nodes/s)")
    # re-load same data = pure update path (typical steady-state sync)
    t0 = time.perf_counter()
    load(session, schema, data, batch_size=batch_size,
         Region="us-east-1", AWS_ID=ACCOUNT_ID, lastupdated=UPDATE_TAG + 1)
    dt = time.perf_counter() - t0
    print(f"                          re-load (update),  batch_size={batch_size:>6}: "
          f"{dt:.2f}s ({N/dt:.0f} nodes/s)")

# PROFILE: does the cleanup MATCH use an index for `lastupdated <> $tag`?
print("\n--- PROFILE cleanup MATCH (lastupdated <> tag), LIMIT 100 ---")
result = session.run(
    "PROFILE MATCH (n:EC2Instance)<-[s:RESOURCE]-(:AWSAccount{id: $AWS_ID}) "
    "WHERE n.lastupdated <> $UPDATE_TAG WITH n LIMIT 100 RETURN count(n)",
    AWS_ID=ACCOUNT_ID, UPDATE_TAG=UPDATE_TAG + 1,
)
result.consume()
summary = session.run(
    "PROFILE MATCH (n:EC2Instance)<-[s:RESOURCE]-(:AWSAccount{id: $AWS_ID}) "
    "WHERE n.lastupdated <> $UPDATE_TAG WITH n LIMIT 100 RETURN count(n)",
    AWS_ID=ACCOUNT_ID, UPDATE_TAG=UPDATE_TAG + 1,
).consume()


def walk(plan, depth=0):
    rows = plan.get("args", {}).get("Rows", "")
    dbhits = plan.get("args", {}).get("DbHits", "")
    print("  " * depth + f"{plan['operatorType']} rows={rows} dbHits={dbhits}")
    for child in plan.get("children", []):
        walk(child, depth + 1)


walk(summary.profile)
session.close()
