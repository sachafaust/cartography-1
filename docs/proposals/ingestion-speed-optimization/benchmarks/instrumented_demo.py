"""Run the repo's demo sync (22 real modules) with instrumentation counting:
- load() calls and their sizes
- ensure_indexes() invocations and index statements issued
- cleanup-statement iterations
This grounds the per-load ensure_indexes overhead estimate with a REAL sync run.
"""

import sys
import time
from collections import Counter

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[4]))

import cartography.client.core.tx as tx
import cartography.graph.statement as stmt_mod

counters = Counter()
load_sizes = []

_orig_ensure = tx.ensure_indexes
_orig_index_query = tx._run_index_query_with_retry
_orig_load = tx.load
_orig_run_noniter = stmt_mod.GraphStatement._run_noniterative

def ensure_indexes_counted(session, schema):
    counters["ensure_indexes_calls"] += 1
    return _orig_ensure(session, schema)

def index_query_counted(session, query):
    counters["index_statements"] += 1
    t0 = time.perf_counter()
    r = _orig_index_query(session, query)
    counters["index_statement_seconds_x1000"] += int((time.perf_counter() - t0) * 1000)
    return r

def load_counted(session, schema, dict_list, **kw):
    counters["load_calls"] += 1
    load_sizes.append(len(dict_list))
    return _orig_load(session, schema, dict_list, **kw)

def run_noniter_counted(self, tx_):
    counters["graph_statement_txns"] += 1
    return _orig_run_noniter(self, tx_)

tx.ensure_indexes = ensure_indexes_counted
tx._run_index_query_with_retry = index_query_counted
tx.load = load_counted
stmt_mod.GraphStatement._run_noniterative = run_noniter_counted

# Also patch already-imported references in the tx module itself (load calls ensure_indexes by module global)
# and every intel module that did `from cartography.client.core.tx import load`.
import importlib, pkgutil
import cartography.intel

def repatch(module):
    for name in dir(module):
        try:
            val = getattr(module, name)
        except Exception:
            continue
        if val is _orig_load:
            setattr(module, name, load_counted)
        elif val is _orig_ensure:
            setattr(module, name, ensure_indexes_counted)

for finder, modname, ispkg in pkgutil.walk_packages(
    cartography.__path__, prefix="cartography."
):
    try:
        m = importlib.import_module(modname)
    except Exception:
        continue
    repatch(m)
import demo.seeds
for finder, modname, ispkg in pkgutil.walk_packages(demo.seeds.__path__, prefix="demo.seeds."):
    try:
        m = importlib.import_module(modname)
        repatch(m)
    except Exception:
        continue

from demo.__main__ import main

t0 = time.perf_counter()
main(force_flag=True, analysis_job_directory="cartography/data/jobs/analysis")
total = time.perf_counter() - t0

print("\n===== INSTRUMENTATION RESULTS =====")
print(f"total demo sync wall time: {total:.1f}s")
for k, v in sorted(counters.items()):
    print(f"{k}: {v}")
if load_sizes:
    import statistics
    print(f"load() sizes: n={len(load_sizes)} median={statistics.median(load_sizes)} "
          f"mean={statistics.mean(load_sizes):.0f} max={max(load_sizes)} "
          f"small(<100 items)={sum(1 for s in load_sizes if s < 100)}")
