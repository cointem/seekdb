#!/usr/bin/env bash
# Execute from the source checkout. No GPU or device access is required here.
set -Eeuo pipefail
mkdir -p ci-logs dist
exec > >(tee ci-logs/build.log) 2>&1
: "${SDK_URL:?}" "${SDK_SHA256:?}"
JOBS=${JOBS:-2}
[[ "$JOBS" =~ ^[1-9][0-9]*$ ]] || exit 2
FROZEN=08c76418a8e1fef3c1aae83010658ff56a4faa57
git merge-base --is-ancestor "$FROZEN" HEAD
COMMIT=$(git rev-parse HEAD)
SDK="$PWD/ci-sdk"
mkdir -p "$SDK"
curl --fail --location --retry 3 "$SDK_URL" -o "$SDK/sdk.tar.gz"
printf '%s  %s\n' "$SDK_SHA256" "$SDK/sdk.tar.gz" | sha256sum -c -
tar -xzf "$SDK/sdk.tar.gz" -C "$SDK"
ln -sf libamdhip64.so.7.2.70204 "$SDK/rocm/lib/libamdhip64.so"
ln -sf libamdhip64.so.7.2.70204 "$SDK/rocm/lib/libamdhip64.so.7"
# The bridge is C, without device kernels. Use the genuine matching headers
# and libraries, not a stub. ROCm's HIP header accepts a normal C compiler.
mkdir -p "$SDK/bridge"
cc -shared -fPIC -O2 -D__HIP_PLATFORM_AMD__ \
  -I"$SDK/hipvs/include" -I"$SDK/rocm/include" \
  tools/hipvs_bridge/seekdb_cuvs_bridge.c \
  -L"$SDK/hipvs/lib" -L"$SDK/rocm/lib" \
  -Wl,-soname,libseekdb_cuvs_bridge.so -lcuvs_c -lamdhip64 \
  -o "$SDK/bridge/libseekdb_cuvs_bridge.so"
readelf -d "$SDK/bridge/libseekdb_cuvs_bridge.so" > ci-logs/bridge-dynamic.txt
if [[ "${DEPS_CACHE_HIT:-false}" != true ]]; then
  # Management clients/obshell are not linked into seekdb. The upstream
  # obshell download currently returns 404; exclude only non-build sections.
  profile=deps/init/oceanbase.el9.x86_64.deps
  cp "$profile" ci-logs/deps-profile.original
  trap 'cp ci-logs/deps-profile.original deps/init/oceanbase.el9.x86_64.deps' EXIT
  python3 - "$profile" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1]); out=[]; skip=False
for line in p.read_text().splitlines(keepends=True):
    if line.startswith('['): skip=line.strip() in ('[tools-deps]','[test-utils]')
    if not skip: out.append(line)
p.write_text(''.join(out))
PY
  (cd deps/init && bash dep_create.sh)
  cp ci-logs/deps-profile.original "$profile"
  trap - EXIT
fi
# Allow only the bridge's transitive GPU-library symbols to remain unresolved
# on this CPU build host. References from seekdb's own objects must resolve.
# Those GPU libraries already exist on the target; validate them on deployment.
bash build.sh release -DOB_BUILD_CUVS=ON -DOB_BUILD_CUVS_TRACE=OFF \
  "-DCUVS_BRIDGE_LIB=$SDK/bridge/libseekdb_cuvs_bridge.so" \
  '-DCMAKE_EXE_LINKER_FLAGS=-Wl,--unresolved-symbols=ignore-in-shared-libs' \
  "-DLTO_JOBS=$JOBS" --make "-j$JOBS"
PACKAGE="seekdb-amd-${COMMIT:0:12}"
OUT="dist/$PACKAGE"
mkdir -p "$OUT/bin" "$OUT/lib" "$OUT/tools"
cp build_release/src/observer/seekdb "$OUT/bin/seekdb"
# Debug symbols are not necessary for cloud performance experiments.
strip --strip-debug "$OUT/bin/seekdb"
cp "$SDK/bridge/libseekdb_cuvs_bridge.so" "$OUT/lib/"
cp -L deps/3rd/usr/local/oceanbase/devtools/lib64/libstdc++.so.6 "$OUT/lib/"
patchelf --set-rpath '$ORIGIN/../lib:/opt/hipvs/lib:/opt/rocm/lib' "$OUT/bin/seekdb"
patchelf --set-rpath '$ORIGIN:/opt/hipvs/lib:/opt/rocm/lib' "$OUT/lib/libseekdb_cuvs_bridge.so"
cp tools/hipvs_bridge/{profile_workload.py,summarize_profile.py} "$OUT/tools/"
cp ../ci/.github/scripts/amd-build/{launch.sh,verify.py} "$OUT/"
chmod +x "$OUT/launch.sh"
export COMMIT FROZEN OUT
python3 - <<'PY'
import hashlib,json,os,pathlib,platform,subprocess
root=pathlib.Path(os.environ['OUT'])
manifest=dict(commit=os.environ['COMMIT'], frozen_baseline=os.environ['FROZEN'],
 sdk_sha256=os.environ['SDK_SHA256'], target='Linux x86_64 / Ubuntu 24.04 / ROCm 7.2.4 / gfx1100',
 kind='development artifact; official evaluation recompiles source',
 build_os=platform.platform(),rustc=subprocess.check_output(['rustc','--version'],cwd='rust',text=True).strip(),
 workflow_commit=os.environ.get('GITHUB_SHA'),run_url='https://github.com/'+os.environ['GITHUB_REPOSITORY']+'/actions/runs/'+os.environ['GITHUB_RUN_ID'],
 files={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob('*') if p.is_file()})
(root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
PY
readelf -d "$OUT/bin/seekdb" > ci-logs/seekdb-dynamic.txt
tar -C dist -czf "dist/$PACKAGE.tar.gz" "$PACKAGE"
(cd dist && sha256sum "$PACKAGE.tar.gz" > "$PACKAGE.tar.gz.sha256")
rm -r "$OUT"
printf 'Built source commit: `%s`\n\nArtifact: `%s.tar.gz`\n' "$COMMIT" "$PACKAGE" >> "$GITHUB_STEP_SUMMARY"
