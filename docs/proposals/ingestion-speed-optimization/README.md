# Proposal: Top 5 ingestion-speed optimizations

**Status:** Findings 2, 3, and 4 are implemented on this branch (the three lowest-risk items):
- Finding 2 — `ensure_indexes()` is now memoized per schema class per process (`cartography/client/core/tx.py`); `clear_ensure_indexes_cache()` resets it.
- Finding 3 — cleanup `iterationsize` default raised from 100 to 1000 (`cartography/graph/job.py` and all `cartography/data/jobs/cleanup/*.json`).
- Finding 4 — the Identity Center role matcher now uses an index-backed `STARTS WITH` on `AWSRole.name` plus an exact match on `AWSRole.path` via the new `PropertyRef(starts_with=True)` option, replacing the non-indexable `CONTAINS` on `arn`. Verified plan: `NodeIndexSeekByRange`; measured 0.04 s vs 27 s for 50k roles x 1k permission sets on the exact generated clause shape (~670x).

Findings 1 and 5 remain proposals.
**Scope:** Code-level optimizations only — no architectural changes to the sync model, data model, or query-generation design.
**Grounding:** Every claim below was validated against a live Neo4j 5.15 community instance (the same image as `docker-compose.yml`) using the benchmark harness in [`benchmarks/`](benchmarks/), which exercises the *real* cartography code paths (`load()`, `GraphJob.from_node_schema()`, `run_write_query()`, the generated ingestion/cleanup Cypher) with the real `EC2InstanceSchema` data model. A full instrumented run of the repo's own `demo` sync (22 intel modules) was used to measure how the ingestion machinery behaves in an end-to-end sync.

## How to reproduce every number in this document

```bash
# 1. Start Neo4j (same image/memory settings as the repo's docker-compose)
docker run -d --name neo4j-bench -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=none \
  -e NEO4J_server_memory_pagecache_size=1G \
  -e NEO4J_server_memory_heap_initial__size=1G \
  -e NEO4J_server_memory_heap_max__size=1G \
  neo4j:5.15.0-community

# 2. Run the benchmarks (each prints its own results; all destructive to the local DB)
uv run python docs/proposals/ingestion-speed-optimization/benchmarks/bench_ensure_indexes.py
uv run python docs/proposals/ingestion-speed-optimization/benchmarks/bench_cleanup.py 50000
uv run python docs/proposals/ingestion-speed-optimization/benchmarks/bench_n_plus_1.py 2000
uv run python docs/proposals/ingestion-speed-optimization/benchmarks/bench_load_throughput.py 100000
uv run python docs/proposals/ingestion-speed-optimization/benchmarks/bench_contains.py
uv run python docs/proposals/ingestion-speed-optimization/benchmarks/bench_rel_share.py

# 3. Instrumented end-to-end sync using the repo's demo data (22 modules)
uv run python docs/proposals/ingestion-speed-optimization/benchmarks/instrumented_demo.py
```

All measurements below were taken on localhost Bolt (~0.2 ms RTT). **Every finding that eliminates round trips scales up with network latency** — a managed Neo4j (Aura/remote) at 10–50 ms RTT amplifies these wins by 1–2 orders of magnitude, which is the common production topology.

---

## Baseline: what is already good (measured, not assumed)

Before the findings, three things the current design gets right — these were validated so we know where *not* to spend effort:

| Aspect | Measurement | Verdict |
|---|---|---|
| Schema-driven `load()` write throughput | 4.5–5.6k nodes/s create, 8–9.6k nodes/s update (100k EC2 instances, 24 properties, 4 relationships) | Healthy; UNWIND+MERGE batching works |
| `batch_size=10000` default | 1k / 10k / 25k batch sizes all within ±5% of each other | No change needed |
| Relationship-attachment `CALL {}` subqueries | Only ~20% of total load time (17.7 s node-only vs 22.1 s with all 4 rels per 100k) | Not a bottleneck; leave the generated query design alone |

The problems are **not** in the core generated-query design. They are in (1) legacy modules that bypass it, (2) fixed per-call overheads repeated thousands of times, (3) cleanup batching, (4) one pathological matcher, and (5) serial API fetching.

---

## Finding 1 — Legacy per-item write loops (N+1): batch them into UNWIND writes

**Measured: 35.5x** on the affected write paths (219 items/s → 7,750 items/s, `bench_n_plus_1.py`, using the actual `ingest_bucket` query from `s3.py`).

### The problem

A set of older modules never migrated to the `load()` API and issue **one managed write transaction per resource** via `run_write_query()` inside `for` loops. Each call is a full `session.execute_write()` — begin, run, commit — so cost is dominated by per-transaction overhead and round trips, not by the data:

- `cartography/intel/aws/s3.py:1038-1049` — one transaction **per bucket** in `load_s3_buckets`
- `cartography/intel/aws/ec2/load_balancer_v2s.py:108,143,188,226` — per LB, per security group, per subnet, **per target instance** (nested loops)
- `cartography/intel/aws/ec2/tgw.py:124,166,208,277` — per TGW / attachment / subnet
- `cartography/intel/aws/redshift.py:92,133,156` — per cluster / VPC SG / IAM role
- `cartography/intel/aws/config.py:84,125,173` — per recorder / channel / rule
- Same pattern family in parts of `okta`, `pagerduty`, `github/repos.py`, `elasticsearch.py`

### The fix (mechanical, per module)

Collect the loop's parameters into a list of dicts and issue **one** `UNWIND $Items AS item ...` write per batch — the exact pattern the same files already use elsewhere (e.g. `s3.py`'s `_load_s3_acls`). No behavior change: same MERGE semantics, same properties. Where convenient, migrate fully to the `load()` data-model API (which `AGENTS.md` already mandates for new code), but the minimal UNWIND conversion captures the win with near-zero risk.

### Forecast

For an account with 5,000 buckets + 2,000 ELBv2s (plus listeners/targets): roughly 20–30k per-item transactions today. Localhost: ~2 min → ~4 s. At 20 ms RTT: **~25–40 min → seconds, per account**. This is the single largest validated win for S3/ELB-heavy deployments.

**Effort:** Small-Medium (mechanical rewrite, module by module; existing integration tests cover the outcomes). **Risk:** Low.

---

## Finding 2 — `ensure_indexes()` runs on every `load()` call: memoize it

**Measured:** 9 `CREATE INDEX IF NOT EXISTS` statements per `load()` call for `EC2InstanceSchema`, each a separate autocommit round trip; **23–26 ms steady-state per call**, which is **73% of the wall time of a typical small load** (38.8 ms with vs 10.3 ms without, for a 25-item load — `bench_ensure_indexes.py`).

**Measured in a real sync:** the instrumented demo run (22 modules) issued **688 index statements across 143 `load()` calls, consuming 3.6 s of a 60 s sync (~6%)** — and critically, *every one of the 143 loads carried fewer than 100 items* (median: 2). Cartography syncs are dominated by many small loads, so fixed per-load overhead matters more than bulk throughput.

### The problem

`tx.load()` calls `ensure_indexes()` unconditionally (`cartography/client/core/tx.py:580`). The indexes are created on the *first* call and every subsequent call re-issues the same idempotent DDL statements. `load()` is invoked per node-type **per account and per region** — a 20-account AWS sync easily reaches 2,500+ `load()` calls (~131 call sites in `cartography/intel/aws` alone, times accounts). The design intent (module authors can never forget indexes — see the docstring at `tx.py:524`) is good and worth keeping; re-executing the DDL is not.

### The fix (~10 lines, one file)

Process-level memoization in `tx.py`: a module-level `set` keyed by node-schema class (or by the generated index-query strings). First `load()` for a schema runs the DDL exactly as today; subsequent calls skip it. The robustness guarantee is preserved — every schema still gets its indexes before its first write of the process. (Optional complement: after the `create-indexes` stage, run `CALL db.awaitIndexes()` once, as `demo/__main__.py:74-82` already does, so first loads don't race index population.)

### Forecast

Localhost: ~25 ms × (calls − distinct schemas). A 20-account sync: ~2,500 calls × 25 ms ≈ **~60 s saved locally; at 20 ms RTT ≈ 9 statements × 21 ms × 2,500 ≈ ~8 min saved**, plus removal of schema-lock churn that currently competes with concurrent syncs (the code already carries a workaround for exactly that race: `tx.py:201-211`).

**Effort:** Trivial. **Risk:** Very low (an externally dropped index mid-run would no longer be auto-recreated until restart — acceptable; the `create-indexes` stage still runs every sync).

---

## Finding 3 — Cleanup jobs delete 100 rows per transaction: raise to 1000

**Measured: 2.2x** on cleanup throughput (50k stale nodes: 11.2 s at `iterationsize=100` → 5.1 s at 1000; 10,000 shows no further gain — `bench_cleanup.py`, using the real `GraphJob.from_node_schema(EC2InstanceSchema(), ...)` path).

### The problem

Every auto-generated cleanup job defaults to `iterationsize=100` (`cartography/graph/job.py:144`), and 202 of the 203 handwritten cleanup statements in `cartography/data/jobs/cleanup/*.json` hardcode `"iterationsize": 100`. Deleting N stale entities costs N/100 full transactions, each re-running the MATCH from scratch. Deleting 50k stale nodes = 500 transactions; at 20 ms RTT that's ~10–20 s of pure round-trip time before any delete work happens.

### Why 100 was chosen, and why 1000 is safe

The small batch bounds transaction memory and lock hold-time for `DETACH DELETE` (a node with many rels amplifies each row). That trade-off is real — but measured headroom is large: at the repo's own 1 GB-heap docker-compose settings, batches of 1000 (and even 10000 for these node shapes) complete without approaching the memory pool limit, while 1000 already captures the entire throughput win. Recommendation: **change the default and the JSON files to 1000, keep the `iterationsize` parameter** so operators with extremely dense nodes can lower it.

### Forecast

Cleanup phase ~2x faster locally, and transaction count drops 10x (dominant on remote DBs). For churny environments (ephemeral EC2/containers, where 10–30% of assets go stale per sync) cleanup is a top-3 phase cost; this halves it for a two-line default change plus a `sed` across the JSON jobs.

**Effort:** Trivial. **Risk:** Low (memory headroom validated; parameter remains available as an escape hatch).

---

## Finding 4 — Identity Center's `CONTAINS` role matcher is quadratic: use an index-backed prefix match

**Measured: 187x–1,379x, and growing quadratically** (`bench_contains.py`):

| Graph size | `toLower() CONTAINS toLower()` (current) | Index-backed exact/prefix | Speedup |
|---|---|---|---|
| 10k AWSRoles × 500 permission sets | 6.6 s | 0.035 s | 187x |
| 50k AWSRoles × 1,000 permission sets | 46.5 s | 0.034 s | 1,379x |

### The problem

The single `fuzzy_and_ignore_case=True` matcher in the codebase (`cartography/models/aws/identitycenter/awspermissionset.py:55`) generates `WHERE toLower(n0.arn) CONTAINS toLower(item.RoleHint)` — which cannot use the `AWSRole.arn` index, so **every permission-set row scans every AWSRole node** and runs two string lowercases per pair. For a large org (50k+ roles, thousands of permission sets) this one attach clause costs minutes per sync and grows with the product of both sides.

### The fix (contained to one module + optional querybuilder support)

The `RoleHint` built in `transform_permission_sets` (`identitycenter.py:115-134`) is a *true prefix* of the SSO role ARN once the account prefix is added: SSO roles are always `arn:aws:iam::{account}:role/aws-reserved/sso.amazonaws.com/[{region}/]AWSReservedSSO_{Name}_{suffix}`. Since `AWS_ID` is available at load time, build the full prefix and match with `STARTS WITH` — which **is** index-backed in Neo4j (range index prefix seek) and preserves the existing semantics (both strings come from the same AWS API, so casing is consistent). Options, in increasing generality:
1. Match in the module: resolve role ARNs in Python (both node sets are already tiny to enumerate per account) or via a scoped matchlink query using `STARTS WITH`.
2. Add a `starts_with` option to `PropertyRef` beside `fuzzy_and_ignore_case` and emit `n.arn STARTS WITH item.RoleHintFull` in `querybuilder._build_where_clause_for_rel_match` — future modules get the fast path too.

**Effort:** Small. **Risk:** Low (semantics preserved; integration tests for identity center exist).

---

## Finding 5 — Serial per-region API fetching: fan out the `get` phase with the existing async utility

This is the only finding not benchmarkable without live cloud credentials, so it is stated as a forecast grounded in code precedent rather than a measurement — and it is deliberately scoped to avoid architectural change.

### The problem

Accounts, services, and regions all iterate sequentially (`cartography/intel/aws/__init__.py:74,199`; per-service `for region in regions:` loops, e.g. `ec2/instances.py:443`). For API-bound services, wall time ≈ Σ(regions) × Σ(pages) × API latency. In multi-region deployments the AWS API fetch phase — not Neo4j — typically dominates total sync time.

### The fix pattern already exists in this repo

`cartography/util.py:454` (`to_asynchronous`/`to_synchronous`, thread-pool backed) is already used to fan out per-bucket S3 detail calls (7 concurrent calls per bucket, `s3.py:94-116`) and ECR image layers. The proposal is to apply the same utility one level up in the highest-volume modules: **fetch all regions' data concurrently, then transform and `load()` on the main thread as today.** Neo4j session usage stays single-threaded (sessions are not thread-safe — this constraint is why the *write* phase must stay serial, and it can, because writes are <30% of module wall time for API-bound services). Start with the top modules by API volume: `ec2/instances`, `ec2/network_interfaces`, `rds`, `elasticache`, `lambda_function`.

### Forecast

For a deployment syncing R active regions, the API phase of a converted module approaches R× faster, bounded by a small concurrency cap (4–8) to respect AWS throttling; boto3 clients are thread-safe per-client, and per-region clients are independent. A 10-region account whose EC2 fetch takes 10 min serially drops to ~1.5–3 min. Rate-limit risk is managed the same way `get_s3_bucket_details` already does (bounded gather + existing botocore retry config).

**Effort:** Medium (per module, but formulaic; the util and precedent exist). **Risk:** Medium-low (throttling; mitigated by cap + existing retries). Recommended after findings 1–4, which are pure wins.

---

## Summary table

| # | Optimization | Measured evidence | Forecast (realistic deployment, remote Neo4j) | Effort | Risk |
|---|---|---|---|---|---|
| 1 | Batch N+1 write loops (s3, elbv2, tgw, redshift, config, …) | **35.5x** on affected paths | Minutes → seconds per account in affected modules | S–M | Low |
| 2 | Memoize `ensure_indexes()` per process | 23–26 ms/load = **73% of a small load**; 6% of demo sync | ~minutes saved per multi-account sync; removes schema-lock churn | XS | Very low |
| 3 | Cleanup `iterationsize` 100 → 1000 | **2.2x** cleanup throughput; 10x fewer transactions | Cleanup phase ~halved; bigger on remote | XS | Low |
| 4 | Replace Identity Center `CONTAINS` with index-backed prefix match | **187–1,379x**, quadratic growth removed | Minutes → milliseconds for large orgs | S | Low |
| 5 | Parallelize per-region API `get` (existing `to_asynchronous` util) | Precedent in-repo (`s3.py`, ECR); not directly benchmarkable | Up to ~Rx on API-bound phase per module | M | Med-low |

## Explicitly rejected (architectural or not worth it — with evidence)

- **Changing `batch_size`:** measured flat from 1k–25k; the 10k default is fine.
- **Restructuring the generated ingestion query (splitting node/rel loads, removing `CALL {}` subqueries):** rel attachment is only ~20% of load time; not worth touching core query generation.
- **Re-anchoring cleanup queries on the node label instead of the account node:** measured only 2.6x on the no-op iteration (8 ms → 3 ms); planner statistics already handle this reasonably. Revisit only if profiling a very dense production account shows otherwise.
- **Parallel top-level sync stages / multi-session writes / async driver:** architectural; excluded by design constraint.
- **Uniqueness constraints instead of plain indexes:** a correctness/data-integrity improvement, not a speed win; out of scope.

## Validation plan for implementation

1. Each finding lands as its own PR with a before/after run of the corresponding benchmark script in the PR description.
2. Findings 1–4 are covered by existing integration tests (`make test_integration` against `neo4j:4.4` and `neo4j:5`, as CI already does); finding 1 conversions must keep tests asserting *outcomes* (nodes/rels present) green with zero test changes.
3. The instrumented demo (`benchmarks/instrumented_demo.py`) provides an end-to-end regression check: total wall time, `load()` call count, index-statement count, and cleanup-transaction count before vs after.
4. For finding 5, gate each module conversion behind measured module wall time from the existing `@timeit`/statsd instrumentation in a staging sync before/after.
