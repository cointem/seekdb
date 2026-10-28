#!/usr/bin/env bash
# Run from a Git checkout on the official AMD Linux instance.
set -Eeuo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo 'Usage: bash run_on_gpu.sh'
  echo 'Optional environment: JOBS=8 PORT=2981 N=10000 DIM=128 NQ=100 ROUNDS=3 LABEL=candidate-001'
  echo 'INIT_DEPS=0 reuses already initialized dependencies (default: 1)'
  echo 'Optional: BASE_FILE=/path/base.f32 QUERY_FILE=/path/query.f32 RUN_ROOT=/workspace/amd-profile-runs'
  exit 0
fi
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR=$PWD
INIT_DEPS=${INIT_DEPS:-1}
[[ "$INIT_DEPS" == 0 || "$INIT_DEPS" == 1 ]] || { echo 'INIT_DEPS must be 0 or 1'; exit 2; }
JOBS=${JOBS:-8}
PORT=${PORT:-2981}
N=${N:-10000}
DIM=${DIM:-128}
NQ=${NQ:-100}
ROUNDS=${ROUNDS:-3}
LABEL=${LABEL:-candidate-001}
BASE_FILE=${BASE_FILE:-/seekdb_workspace/datasets/base.f32}
QUERY_FILE=${QUERY_FILE:-/seekdb_workspace/datasets/query.f32}
RUN_ROOT=${RUN_ROOT:-/workspace/amd-profile-runs}
[[ $(uname -s) == Linux ]] || { echo 'Run this script in the cloud Jupyter terminal (Linux), not on the Mac.' >&2; exit 2; }
for value in "$JOBS" "$PORT" "$N" "$DIM" "$NQ" "$ROUNDS"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo 'Numeric settings must be positive integers.' >&2; exit 2; }
done
for tool in python3 mysql tar; do command -v "$tool" >/dev/null || { echo "Missing command: $tool" >&2; exit 2; }; done
python3 -c 'import numpy' || { echo 'Use the Python environment provided with the official notebook (numpy required).' >&2; exit 2; }
[[ -f "$BASE_FILE" && -f "$QUERY_FILE" ]] || { echo 'Dataset files not found. Set BASE_FILE and QUERY_FILE.' >&2; exit 2; }
[[ -x /opt/rocm/bin/hipcc && -f /opt/hipvs/include/cuvs/neighbors/cagra.h ]] || { echo 'ROCm/hipVS development files not found under /opt/rocm and /opt/hipvs.' >&2; exit 2; }
python3 - "$PORT" <<'PY'
import socket,sys
port=int(sys.argv[1])
if not 1<=port<=65535:raise SystemExit('Invalid port')
with socket.socket() as s:
    s.bind(('127.0.0.1',port))
PY
mkdir -p "$RUN_ROOT"
RUN=$(mktemp -d "$RUN_ROOT/run-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
exec > >(tee "$RUN/driver.log") 2>&1
OBSERVER_PID=''
on_exit() {
  local status=$1
  trap - EXIT
  set +e
  if [[ -n "$OBSERVER_PID" ]] && kill -0 "$OBSERVER_PID" 2>/dev/null; then
    kill -TERM "$OBSERVER_PID" 2>/dev/null
    for ((i=0;i<30;i++)); do
      kill -0 "$OBSERVER_PID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$OBSERVER_PID" 2>/dev/null; then
      echo "Observer still shutting down: PID $OBSERVER_PID. Data directory: $RUN/base"
    else
      wait "$OBSERVER_PID" 2>/dev/null
    fi
  fi
  local -a files=()
  # Exclude driver.log while tee may still be appending to it.
  for name in bridge-build.log build.log observer.out source_snapshot.json workload report; do
    [[ -e "$RUN/$name" ]] && files+=("$name")
  done
  tar -czf "$RUN-results.tar.gz" -C "$RUN" "${files[@]}"
  echo "Run directory: $RUN"
  echo "Result bundle: $RUN-results.tar.gz"
  if ((status != 0)); then
    echo "Run stopped with exit status $status. Keep the result bundle and driver.log for diagnosis."
  else
    echo 'Finished. Download the result bundle and send it back for analysis.'
  fi
  exit "$status"
}
trap 'on_exit "$?"' EXIT
COMMIT=$(git rev-parse HEAD)
python3 - "$RUN/source_snapshot.json" <<'PYMETA'
import json, subprocess, sys
json.dump({"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
           "status": subprocess.check_output(["git", "status", "--porcelain"], text=True)},
          open(sys.argv[1], "w"), indent=2)
PYMETA
echo "Run directory: $RUN"
echo '[1/5] Build the GPU bridge'
mkdir -p "$RUN/bridge"
bash tools/hipvs_bridge/build.sh "$RUN/bridge/libseekdb_cuvs_bridge.so" 2>&1 | tee "$RUN/bridge-build.log"
echo '[2/5] Initialize dependencies and compile seekdb (first run can take considerable time)'
init_args=()
[[ "$INIT_DEPS" == 0 ]] || init_args+=(--init)
bash build.sh release "${init_args[@]}" -DOB_BUILD_CUVS=ON -DOB_BUILD_CUVS_TRACE=OFF \
  "-DCUVS_BRIDGE_LIB=$RUN/bridge/libseekdb_cuvs_bridge.so" --make "-j$JOBS" 2>&1 | tee "$RUN/build.log"
OBSERVER="$SOURCE_DIR/build_release/src/observer/seekdb"
[[ -x "$OBSERVER" ]] || { echo "Source-built binary not found: $OBSERVER"; exit 2; }
echo '[3/5] Start an isolated instrumented observer'
mkdir -p "$RUN/base" "$RUN/store"
export SEEKDB_VECTOR_PROFILE=1
export LD_LIBRARY_PATH="$RUN/bridge:/opt/hipvs/lib:/opt/rocm/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
"$OBSERVER" --nodaemon --port "$PORT" --base-dir "$RUN/base" --data-dir "$RUN/store" \
  --parameter local_ip=127.0.0.1 --parameter memory_budget=8G \
  --parameter datafile_size=2G --parameter datafile_maxsize=8G --parameter log_disk_size=2G \
  > "$RUN/observer.out" 2>&1 &
OBSERVER_PID=$!
echo "$OBSERVER_PID" > "$RUN/observer.pid"
ready=0
for ((i=0;i<180;i++)); do
  kill -0 "$OBSERVER_PID" 2>/dev/null || { tail -40 "$RUN/observer.out"; exit 2; }
  if mysql --connect-timeout=2 -h127.0.0.1 "-P$PORT" -uroot -e 'select 1' >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
((ready == 1)) || { echo 'SQL startup timed out; inspect observer.out'; exit 2; }
mysql -h127.0.0.1 "-P$PORT" -uroot -e 'alter system set ob_vector_memory_limit_percentage=30;'
echo '[4/5] Run the fixed CPU/GPU workload'
python3 tools/hipvs_bridge/profile_workload.py --port "$PORT" --label "$LABEL" --commit "$COMMIT" \
  --base "$BASE_FILE" --queries "$QUERY_FILE" --dim "$DIM" --n "$N" --nq "$NQ" --rounds "$ROUNDS" \
  --output "$RUN/workload"
echo '[5/5] Summarize source-level phase timings'
python3 tools/hipvs_bridge/summarize_profile.py "$RUN/observer.out" \
  --label "$LABEL" --commit "$COMMIT" --dataset "n$N-d$DIM-q$NQ" \
  --bridge "$RUN/bridge/libseekdb_cuvs_bridge.so" --workload "$RUN/workload/workload.json" \
  --output "$RUN/report"
