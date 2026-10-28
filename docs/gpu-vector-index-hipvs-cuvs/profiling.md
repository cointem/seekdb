# AMD vector-index profiling

This workflow is for a **source-built observer and bridge**. Set
`SEEKDB_VECTOR_PROFILE=1` and
`SEEKDB_VECTOR_PROFILE_LOG=/absolute/path/profile.jsonl` before starting the observer.
The dedicated file is required for seekdb: it redirects stderr to an internal
log whose independent writes can overwrite diagnostic records. No trace-enabled build is
required. Normal execution emits no profile records and adds no profile GPU
synchronization when the switch is absent.

## Contest provenance

The contest page checked on 2026-10-28 explicitly freezes
`amd-cagra-contest-baseline-v1`, commit
`08c76418a8e1fef3c1aae83010658ff56a4faa57`.
Source: https://oceanbase.yuque.com/org-wiki-obtech-vh7w9r/hc3pio/seebnbaoxot1b0tb
The newer upstream v5 (`634888d9b037153fa143d2c913ba4b25503e1d62`)
is a descendant containing additional GPU correctness/persistence fixes; it is
part of this candidate's history, **not the frozen performance comparator**.

Verify the candidate before submission:

```bash
git merge-base --is-ancestor 08c76418a8e1fef3c1aae83010658ff56a4faa57 HEAD
```

This candidate also contains experimental GPU parameter and staging changes
that predate profiling. Do not label it an untouched baseline. Use the exact
frozen commit for performance comparisons. Diagnostic instrumentation adds
synchronization; turn it off for performance results and use identical data,
SQL parameters, and alternating cold runs. The convenience workload below
uses fresh indexes within a warm process and is not the official evaluator.

## Build and run

From the source checkout on the GPU machine:

```bash
mkdir -p /workspace/vector-profile/bridge
tools/hipvs_bridge/build.sh /workspace/vector-profile/bridge/libseekdb_cuvs_bridge.so
./build.sh release \
  -DOB_BUILD_CUVS=ON \
  -DOB_BUILD_CUVS_TRACE=OFF \
  -DCUVS_BRIDGE_LIB=/workspace/vector-profile/bridge/libseekdb_cuvs_bridge.so \
  --make -j8
```

Point `OBSERVER` at the binary produced by this build, not the preinstalled
`/seekdb_workspace/bin/seekdb`. Use an unused port and a new data directory.
The following launches a separate diagnostic instance on port 2981:

```bash
OBSERVER=/absolute/path/to/source-built/seekdb
RUN=/workspace/vector-profile/run-001
mkdir -p "$RUN/base" "$RUN/store"
SEEKDB_VECTOR_PROFILE=1 \
SEEKDB_VECTOR_PROFILE_LOG="$RUN/profile.log" \
LD_LIBRARY_PATH=/workspace/vector-profile/bridge:/opt/hipvs/lib:/opt/rocm/lib \
"$OBSERVER" --nodaemon --port 2981 \
  --base-dir "$RUN/base" --data-dir "$RUN/store" \
  --parameter local_ip=127.0.0.1 \
  --parameter memory_budget=8G \
  --parameter datafile_size=2G \
  --parameter datafile_maxsize=8G \
  --parameter log_disk_size=2G \
  > "$RUN/observer.out" 2>&1 &
echo $! > "$RUN/observer.pid"
```

Wait until SQL responds, then configure vector memory for this dedicated
instance. The workload itself does not change global parameters:

```bash
mysql -h127.0.0.1 -P2981 -uroot -e \
  'alter system set ob_vector_memory_limit_percentage=30; select version();'
```

Run the repeatable workload (Python 3, numpy and mysql CLI are required):

```bash
python3 tools/hipvs_bridge/profile_workload.py \
  --port 2981 --label candidate-001 \
  --commit "$(git rev-parse HEAD)" \
  --base /seekdb_workspace/datasets/base.f32 \
  --queries /seekdb_workspace/datasets/query.f32 \
  --dim 128 --n 10000 --nq 100 --rounds 3 \
  --output "$RUN/workload"
```

The workload creates a uniquely named database and keeps its tables for
inspection. It never drops existing tables. Every round imports identical FP32
vectors without an index, then times CREATE VECTOR INDEX through the first
legal query. CPU/GPU order alternates. It measures subsequent Top-10/Top-100
latency, client QPS, exact Recall and ID validity. GPU batch calls are measured
separately and their output cardinality, ID validity and Recall are checked.
Import and ground-truth generation are outside build timing. Input files,
index parameters and server version are recorded.

These are **fresh-index, warm-process** rounds: the workload does not restart
the observer or flush OS/ROCm caches. mysql CLI process/connection overhead is
included in client latency/QPS. A batch call currently includes its own rebuild;
do not interpret it as pure search throughput. The fixed official test program
remains the source of competition scores.

Summarize the dedicated profile file:

```bash
python3 tools/hipvs_bridge/summarize_profile.py "$RUN/profile.log" \
  --label candidate-001 --commit "$(git rev-parse HEAD)" \
  --dataset n10000-d128 \
  --bridge /workspace/vector-profile/bridge/libseekdb_cuvs_bridge.so \
  --workload "$RUN/workload/workload.json" \
  --output "$RUN/report"
```

Return `report/summary.json`, `report/phases.csv`, and
`workload/workload.json` for analysis. `events.jsonl` retains individual events
for timeline analysis. Use a new RUN/output directory for each iteration; do
not concatenate baseline/candidate runs into one summary. Stop the dedicated
observer gracefully when finished with `kill -TERM "$(cat "$RUN/observer.pid")"`.

## What is measured

| Stage | Meaning |
| --- | --- |
| `sql.table_scan_rows` / `sql.table_scan_batches` | SQL table-scan operator row/batch retrieval, including DAS work and scan processing; accumulated per operator (all SQL scans, not just DDL) |
| `ddl.append_pipeline` | HNSW append pipeline, including temporary-file consumption and append work |
| `ddl.read_tmp_batches` | Temporary-vector-batch reads, accumulated per row file; not the initial SQL table scan |
| `ddl.decode_and_add_rows` | Decode/LOB access, adapter acquisition and CPU/GPU buffering, accumulated per row file |
| `adaptor.buffer_vectors` | Copy FP32 vectors and IDs into the GPU adapter's host buffer |
| `cpu.vsag_add` | CPU incremental VSAG builds, accumulated per index handler |
| `cpu.vsag_build` | VSAG bulk Build calls |
| `ddl.serialize_index` | Snapshot/index serialization and its transaction/LOB work |
| `ddl.build_and_serialize` | Enclosing DDL build operator and transaction completion |
| `ddl.write_macros` | Persist serialized vector-index macro blocks |
| `adaptor.registry_lock_wait` | Time waiting for the registry mutex before ordinary GPU search |
| `gpu.dataset_alloc` / `gpu.pinned_alloc` | Device dataset allocation / pinned staging-buffer and stream creation |
| `gpu.host_pack` / `gpu.h2d_base` | Host-to-pinned copy / completed host-to-device base upload |
| `gpu.resources` | cuVS resource, graph-parameter and index-handle creation |
| `gpu.cagra_build` | CAGRA build plus completion synchronization; host-input construction includes implicit upload and CPU refinement |
| `gpu.bridge_build_total` | Entire bridge build, including allocation, upload, graph build and any retry |
| `gpu.query_alloc` / `gpu.query_free` | Query/result GPU-buffer allocation / release |
| `gpu.h2d_queries` / `gpu.cagra_search` / `gpu.d2h_results` | Query upload / completed CAGRA search / result download |
| `gpu.bridge_search_total` | Entire bridge search call |
| `adaptor.knn_total` / `cpu.vsag_search` | Ordinary dense ANN wrapper / CPU search path |
| `route.gpu_served` / `route.cpu_fallback` / `route.cpu_selected` | Ordinary dense ANN route indicators; use counts, not their tiny durations |
| `route.host_input_refinement` | Intentionally selected host-input CAGRA path for supported FP32 refinement; not a failed build or VSAG fallback |
| `route.gpu_build_retry` | Failed automatic IVF-PQ attempt followed by NN-Descent GPU retry; recorded in the dedicated profile file |
| `route.host_input_retry` | Retry CAGRA with host input after staging was unavailable/failed; this is still GPU building, not VSAG fallback |
| `sql.scan_base` / `sql.scan_probes` | SQL batch operator's read, decode and vector collection |
| `sql.decode_base` / `sql.decode_probes` | Decode/LOB/vector collection subset of those reads |
| `sql.gpu_batch` / `adaptor.raw_batch_total` | Batch build+search wrapper, including worker-thread overhead |
| `sql.write_results` / `sql.batch_total` | Result-table clearing/writing / enclosing batch procedure |

Records include PID, thread, handle, monotonic start time, duration, row/dimension/
query/Top-k metadata and return code. Process CPU user/system times and process
peak RSS snapshots are included as cumulative process-wide counters, not GPU
memory or per-stage exclusive resource use.

## Interpretation

- Enclosing spans contain child spans. Never sum all stage times into a total.
  Concurrent CPU work is accumulated CPU wall time, not end-to-end elapsed time.
- Per-row hot paths emit cumulative snapshots at powers of two and on
  flush/destruction. The summarizer retains one maximum snapshot per counter
  lifetime. It does not sum repeated snapshots. An abrupt kill before the last
  flush can leave an incomplete tail; shut down gracefully if final totals matter.
- Profile mode synchronizes GPU work to attribute completed work to a phase.
  This changes overlap and scheduling. Use it for bottleneck diagnosis, then run
  the same workload with `SEEKDB_VECTOR_PROFILE` absent for performance validation.
- `workload.json` provides client wall time and Recall; source spans provide
  attribution. The summarizer reports GPU stages observed in each case's build
  and query time windows. Missing GPU records mean **unobserved**, not zero GPU
  cost or proof of successful GPU execution.
- All logs must come from the instrumented observer and its newly built bridge.
  A preinstalled binary, old running process or old `.so` will not gain these
  spans. Record commit, working-tree changes and bridge fingerprint every run.
- SQL table-scan counters cover operator row/batch retrieval during initial scans as
  well as subsequent queries. Batch row counts are returned vector slots and can
  include skipped slots. Use the workload time windows to identify build scans.
  DDL scheduling, PX communication outside retrieval, sorting and some transaction
  work are not fully split. Unattributed client time must not be renamed "scan time".

## Host-side checks

```bash
PYTHONDONTWRITEBYTECODE=1 python3 unittest/oblib/vector/test_vector_profile.py -v
```

Tests compile the timing header, exercise opt-in/multithreaded counters, check
collector deduplication, and exercise bridge ownership/error paths using fake
GPU APIs with AddressSanitizer. They do not validate real GPU speed/Recall or
replace a complete Linux/ROCm observer build.


## Optimized construction and reproducible comparisons

The default bridge now constructs CAGRA from IVF-PQ candidates followed by
FP32 refinement when `n >= 8192`, `dim >= 16`, and `dim` is divisible by four.
It uses 128 coarse partitions, 64 probes, PQ subspaces of four coordinates,
refinement rate 2, and internal query batches of 128. On hipVS 26.03 this uses
the supported host-input CAGRA path: IVF-PQ training/candidate search execute on
the GPU, FP32 candidate refinement executes on the CPU, and the resulting CAGRA
index and ANN search reside on the GPU. Device-input refinement rejects the
256-entry intermediate graph; choosing host input before building avoids a
failed device attempt and duplicate training. This is not a CPU VSAG index.
All refinement, transfers, and GPU completion stay within the build timer.
The stored dataset and
query distance computation remain FP32. Graph degree 128, intermediate degree
256, and search candidate floor 512 are unchanged. This is approximate graph
construction and must still pass Recall checks on the evaluation data.

Smaller or unsupported shapes retain upstream-v5 NN-Descent (100 iterations),
so the new fast path does not reduce their construction quality. If the automatic
IVF-PQ build fails, the bridge destroys the incomplete index and retries
NN-Descent on the GPU. Both attempts remain inside the build timer; no previous
index is reused. Explicit algorithm overrides fail visibly instead of retrying,
so failed development configurations cannot masquerade as successful tests.

`SK_CUVS_BUILD_ALGO=nn_descent` and `SK_CUVS_NN_NITER=100` select the upstream-v5
algorithmic development comparator. This is **not** the frozen-v1 comparator.
`SK_CUVS_BUILD_ALGO=auto` selects the submission default. Other development knobs
are `SK_CUVS_IVF_LISTS`, `SK_CUVS_IVF_PROBES`, `SK_CUVS_PQ_DIM`,
`SK_CUVS_REFINE`, and `SK_CUVS_IVF_BATCH`. Do not set them in a submission runner
unless the chosen configuration is intentionally part of that submission.

For an alternating isolated-process development sweep:

```bash
LD_LIBRARY_PATH=/opt/hipvs/lib:/opt/rocm/lib python3 tools/hipvs_bridge/benchmark_bridge.py \
  --bridge /absolute/path/libseekdb_cuvs_bridge.so \
  --base /absolute/path/base.f32 --queries /absolute/path/query.f32 \
  --n 100000 --dim 256 --rounds 3 \
  --variants tools/hipvs_bridge/experiments/pq-development-256d.json \
  --output /workspace/bridge-comparison-new
```

That checked-in variant file is specifically a **256-dimensional development
experiment**, not an adaptive production configuration. Without `--variants`,
the benchmark sweeps NN-Descent iterations; an explicit external
`SK_CUVS_BUILD_ALGO` can select another algorithm. Every result records the
library hash, input hashes, overrides and exact float64 ground-truth Recall.

`compare_sql.py` restarts a source-built database for every run, alternates the
order, creates fresh storage directories and includes CREATE INDEX plus the
first legal query. Use `--baseline-kind frozen` only with the exact frozen
source. For algorithm development, set `--baseline-kind development`, pass
`--baseline-env` a JSON object containing
`{"SK_CUVS_BUILD_ALGO":"nn_descent"}`, and set `--baseline-niter 100`.
Leave candidate overrides absent to exercise the production default. Explicit
`--base`, `--queries`, `--n`, `--dim` and `--nq` permit larger SQL comparisons.
Its reports include binary/library hashes and the effective configuration.

The standalone `experimental_fp16.cpp` is a rejected development experiment:
FP16 graph generation with FP32 search did not materially improve construction
on the tested GPU. It is not linked by `build.sh` or used by the submission.


## Measured results (2026-10-28)

These results are development evidence, **not an official contest score**.
The performance comparator is NN-Descent with 100 iterations and upstream-v5
128/256 graph degrees, search floor 512 and width 4. Both variants run the same
source-built database and bridge, with profiling disabled. The candidate uses
its default host-input IVF-PQ construction, including CPU FP32 refinement and
all transfers. No cached graph from another build is reused.

### Three alternating cold-process SQL rounds, 100k x 256

Data: NumPy 2.4.6 `default_rng(20261028)`, first
`standard_normal((100000,256), dtype=float32)` for base, then
`standard_normal((100,256), dtype=float32)` for probes. This is independently
generated development data. Input hashes, binary/bridge hashes, parameters,
individual query latencies and all rounds are retained in
[sql-comparison.json](results/20261028/sql-comparison.json).

| Measurement | NN-Descent comparator | Default candidate | Ratio |
| --- | ---: | ---: | ---: |
| CREATE INDEX + first query, median | 30,984.18 ms | 12,188.32 ms | 2.542x faster |
| Ordinary Top-10 client throughput, ratio of medians | 1.000 | 1.023 | 1.023x |
| Ordinary Top-100 client throughput, ratio of medians | 1.000 | 1.032 | 1.032x |
| SQL batch Top-10 throughput, ratio of medians | 1.000 | 2.963 | 2.963x |
| SQL batch Top-100 throughput, ratio of medians | 1.000 | 2.915 | 2.915x |

Ordinary-query candidate Recall@10 is 0.988–0.990 and Recall@100 is
0.9708–0.9738. Across ordinary and batch queries in all three pairs, the worst
Recall decrease is 0.003 (allowed relative loss: 0.005). Every observed
throughput ratio exceeds 0.90. There are zero GPU algorithm retries.
[Per-round gate calculations](results/20261028/gates.json) preserve all values.
Batch timing includes its fresh graph build and SQL result writes; it is not
pure CAGRA search speed. CLI startup/connection overhead is included in ordinary
query latency. The OS page cache is not flushed. CPU VSAG was separately checked
on the same 100k data before the GPU-only performance series; these six runs
intentionally omit that unchanged control.

The measured bridge code is commit `4616592e7ac4f1524bf1e51720e1887f38d7aa33`.
Later final code restores 100 NN-Descent iterations for small/unsupported shapes
and build-error retries; that does not change the measured IVF-PQ path. Do not
substitute a different dataset or compare a diagnostic profile run with these
profile-disabled timings.

### Additional validation

- A different seed (`20261029`), 200k x 128 development dataset was tested in
  three alternating fresh-process bridge rounds. Median build time was
  38,321.94 ms versus 11,124.90 ms (3.445x). See
  [bridge-200k128.json](results/20261028/bridge-200k128.json). This is a bridge
  check, not a SQL score. All six builds completed without backend build errors
  or GPU algorithm retries; per-pair Recall loss stays within 0.005.
- The official 10k x 128 demonstration data passed three CPU/GPU diagnostic
  rounds. GPU Recall@10 and Recall@100 were 1.0 in every round. CPU VSAG remains
  operational with unchanged development Recall values 0.64/0.6524 at
  `ef_search=64`.
- Source instrumentation observed three GPU DDL builds and all 603 ordinary
  GPU queries, with zero CPU fallback. There were no malformed records.
  Process peak RSS was 2,332,164 KiB; this is host process memory, not GPU VRAM.
  See [profile summary](results/20261028/profile-summary.json) and
  [phase timings](results/20261028/phases.csv). These profile-enabled times are
  diagnostic and are excluded from the performance comparison above.
- Separate-process serialization/reload returned the expected Top-1 ID 0 at
  distance 0; wrong row count and dimension were correctly rejected.
- Host tests cover opt-in diagnostics, multithreaded counters, trace collection,
  fake-MySQL workload execution, and bridge ownership/error paths under ASan,
  including automatic IVF-PQ failure followed by GPU NN-Descent retry.
- Final source rebuilt successfully. The 4,096-row default NN-Descent path
  returned Recall@10/@100 of 1.0 with no build errors or retries; final-library
  loading of a previously saved optimized index also passed.
- Smaller NN iteration counts and the standalone FP16 graph experiment were
  not adopted. Query width 8 showed a modest gain on one graph, but width 4 is
  retained because the complete SQL validation above used that setting.

### Frozen-baseline limitation

The exact frozen-v1 source was built and tested separately. Its SQL batch
Top-100 call failed with SQL error 1235 and bridge search return code 15 on this
instance. The retained [failed comparison](results/20261028/frozen-v1-failure.json)
is evidence of that failure, **not a speedup result**; its candidate was an
earlier development revision, not this final candidate. Consequently these
results cannot establish the official speedup against frozen v1. Absolute
Recall floors were not specified in the inspected task text. They remain
unevaluated here; no official score or full-score claim is made.

The final branch remains a descendant of the required frozen commit, in the
same `cointem/seekdb` repository as the user's other task branches. Validate with
`git merge-base --is-ancestor 08c76418a8e1fef3c1aae83010658ff56a4faa57 HEAD`.

To recompute the observed relative gates:

```bash
python3 tools/hipvs_bridge/summarize_comparison.py \
  docs/gpu-vector-index-hipvs-cuvs/results/20261028/sql-comparison.json
```
