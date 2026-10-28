# GitHub Actions development binaries

Workflow: `.github/workflows/amd-cloud-build.yml`.
This is a development build for the current AMD cloud image (Ubuntu 24.04,
x86_64, ROCm 7.2.4, hipVS from the contest image, gfx1100). The official
contest evaluator still builds submitted source itself.

The workflow lives on `codex/amd-build-actions`; pushing that branch triggers
it once. After the initial run is registered, dispatch a build of any source
ref in the same repository:

```bash
gh workflow run amd-cloud-build.yml --repo cointem/seekdb \
  --ref codex/amd-build-actions \
  -f source_ref=codex/amd-cagra-v5-profile
```

The GitHub UI may not show the manual Run button until the workflow is also
present on the repository's default branch. The default branch is deliberately
not modified by this setup. CLI dispatch can be used after the push run.

Download the successful run's `amd-linux-x86_64-<run-id>` artifact:

```bash
gh run list --repo cointem/seekdb --workflow amd-cloud-build.yml
gh run download <run-id> --repo cointem/seekdb \
  --name amd-linux-x86_64-<run-id> --dir ./amd-artifact
cd amd-artifact
sha256sum -c *.sha256
tar -xzf seekdb-amd-*.tar.gz
```

On the matching cloud image, launch from the extracted directory. Use a free
port and fresh data directories, as in `profiling.md`:

```bash
mkdir -p /workspace/ci-run/base /workspace/ci-run/store
SEEKDB_VECTOR_PROFILE=1 ./launch.sh --nodaemon --port 2982 \
  --base-dir /workspace/ci-run/base --data-dir /workspace/ci-run/store \
  --parameter local_ip=127.0.0.1 --parameter memory_budget=8G \
  --parameter datafile_size=2G --parameter log_disk_size=2G
```

`manifest.json` records the exact source SHA, frozen baseline SHA, SDK digest,
workflow SHA, workflow URL, Rust version and all bundled file hashes.
The bundle uses Ubuntu 24.04 libstdc++, checked for GLIBCXX_3.4.32;
the build toolchain's GCC-12 libstdc++ is too old for the contest HIP/hipVS. Keep it
with benchmark results. The startup verifier checks those hashes and resolves
the real bridge's complete dynamic dependency graph before running seekdb.
This does not prove actual GPU execution or Recall; run the profiling workload
and inspect route/build events on the GPU machine.

## Matching SDK

Set the repository Actions secret `AMD_BUILD_SDK_URL` to a download URL for
the matching SDK archive. The workflow verifies the pinned SHA-256
`aad96f94c7cafaa6ac9afcd409164bc029f0890f8b295ab26b30b784cfb79074`.
The SDK snapshot is available locally at
`validation/github-actions/amd-build-sdk.tar.gz` in the oceanbase workspace.
The owner-authorized SDK is published at
https://github.com/cointem/seekdb/releases/tag/amd-build-sdk-rocm-7.2.4
and `AMD_BUILD_SDK_URL` has been configured for that asset. A private download
URL can also be used. Keep any signed or authenticated URL in the secret, not
in source.
Without this secret the build fails early with a missing-SDK message.

The fixed SDK archive contains only the contest image's `/opt/hipvs`
headers and C shared library, ROCm headers, and the HIP runtime link library.
It contains no keys, notebook state, data, GPU graphs or user files. The fixed
SHA-256 is checked before extraction. The bridge is plain C and needs no
GPU compiler/code generation; actual GPU kernels reside in target hipVS.
Runtime GPU libraries are supplied by the cloud image, not bundled wholesale.

The host linker allows unresolved *transitive shared-library* references,
because the CPU runner has no GPU math libraries. Direct seekdb references
still must resolve. `launch.sh` loads the real bridge with `RTLD_NOW` before
startup, so a missing or incompatible target runtime fails immediately.

For CI dependency initialization only, `[tools-deps]` and `[test-utils]` are
excluded from a temporary copy of the dependency profile. These sections
contain obshell and client/test tools and are not linked into seekdb. The
source file is restored afterward. This avoids the upstream obshell RPM URL
that returned 404 during environment setup. Compilation uses the same source
and build libraries, with two compile/LTO workers to fit hosted-runner memory.

Artifacts expire after 14 days; logs after 7 days. GPU measurements, automatic
deployment, cloud SSH credentials and official submission are outside this
workflow. Existing cloud compilation and measurements are not interrupted.
