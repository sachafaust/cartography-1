"""Benchmark 7: cost of the fuzzy CONTAINS matcher (AWSPermissionSet -> AWSRole).

The generated attach clause is:
  OPTIONAL MATCH (n0:AWSRole) WHERE toLower(n0.arn) CONTAINS toLower(item.RoleHint)
which cannot use an index -> per-UNWIND-item scan of ALL AWSRole nodes.
"""

import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from common import get_session, wipe

session = get_session()

for n_roles, n_items in [(10000, 500), (50000, 1000)]:
    wipe(session)
    session.run("CREATE INDEX IF NOT EXISTS FOR (n:AWSRole) ON (n.arn)").consume()
    for start in range(0, n_roles, 10000):
        session.run(
            "UNWIND range($s, $e) AS x CREATE (:AWSRole{arn: 'arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_role' + toString(x)})",
            s=start, e=min(start + 9999, n_roles - 1),
        ).consume()
    items = [{"RoleHint": f"AWSReservedSSO_role{i}"} for i in range(n_items)]

    t0 = time.perf_counter()
    session.run(
        """
        UNWIND $items AS item
        OPTIONAL MATCH (n0:AWSRole)
        WHERE toLower(n0.arn) CONTAINS toLower(item.RoleHint)
        RETURN count(n0)
        """,
        items=items,
    ).consume()
    contains_dt = time.perf_counter() - t0

    # Index-backed alternative: exact match with ENDS WITH on a normalized property or STARTS WITH;
    # simplest fair comparison: exact equality on arn (index seek)
    exact_items = [
        {"Arn": f"arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_role{i}"}
        for i in range(n_items)
    ]
    t0 = time.perf_counter()
    session.run(
        """
        UNWIND $items AS item
        OPTIONAL MATCH (n0:AWSRole)
        WHERE n0.arn = item.Arn
        RETURN count(n0)
        """,
        items=exact_items,
    ).consume()
    exact_dt = time.perf_counter() - t0
    print(f"{n_roles} AWSRoles x {n_items} permission-set items: "
          f"CONTAINS scan {contains_dt:.2f}s vs index-backed exact {exact_dt:.3f}s "
          f"({contains_dt/exact_dt:.0f}x)")
session.close()
