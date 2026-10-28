/* seekdb <-> hipVS bridge: plain-C CAGRA kNN; all cuVS/ROCm deps live here. */
#include "seekdb_cuvs_bridge.h"
#include "../../src/oblib/lib/vector/seekdb_vector_profile.h"
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#ifdef __HIP_PLATFORM_AMD__
#include <cuvs/cuda_runtime.h>
#include <hip/hip_runtime_api.h>
#else
#include <cuda_runtime.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Build-time graph quality. The baseline shipped NN_DESCENT with niter=100,
 * which is far past the point where NN-Descent still improves the graph; the
 * iteration count is the dominant cost of the build stage. Degrees stay at the
 * baseline values because they drive Recall, which is a hard gate. Override
 * with SK_CUVS_* only for sweeps: defaults must reproduce submitted numbers. */
#define SK_GRAPH_DEG           128
#define SK_INTERMEDIATE_DEG    256
#define SK_NN_DESCENT_NITER    32

/* Rows per host->device chunk. Small enough to pipeline with the SQL scan,
 * large enough that DMA launch overhead stays negligible. */
#define SK_STAGE_CHUNK_ROWS    32768
/* Double buffer so the DMA of chunk k overlaps filling chunk k+1. */
#define SK_STAGE_BUFS          2

/* Device-resident base dataset plus pinned staging buffers.
 *
 * cuVS keeps whatever device pointer it was handed at build time (a device
 * input is used in place rather than copied), so `dev` must outlive the index
 * built from it -- the owning sk_cuvs_index releases it in seekdb_cuvs_free. */
typedef struct {
  float        *dev;
  float        *pinned[SK_STAGE_BUFS];
  cudaStream_t  stream;
  long          dim;
  long          cap;
  long          rows;
  int           next_buf;
  double        upload_ms;
} sk_cuvs_stage;

typedef struct {
  cuvsResources_t        res;
  cuvsCagraIndexParams_t ip;
  cuvsCagraIndex_t       idx;
  long n, dim;
  sk_cuvs_stage         *stage;
} sk_cuvs_index;

static double sk_now_ms(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static int sk_timing_on(void)
{
  const char *v = getenv("SEEKDB_CUVS_TIMING");
  return v != NULL && v[0] == '1';
}

static int64_t sk_profile_begin(void)
{
  return seekdb_vprofile_enabled() ? seekdb_vprofile_now() : 0;
}

static int sk_profile_sync(void)
{
  if (!seekdb_vprofile_enabled()) { return 0; }
#ifdef __HIP_PLATFORM_AMD__
  return (int)hipDeviceSynchronize();
#else
  return (int)cudaDeviceSynchronize();
#endif
}

static long sk_env_long(const char *name, long dflt, long lo, long hi)
{
  const char *v = getenv(name);
  if (v == NULL || v[0] == '\0') { return dflt; }
  char *end = NULL;
  long r = strtol(v, &end, 10);
  if (end == v || r < lo || r > hi) { return dflt; }
  return r;
}

/* `shape` must be caller-owned storage living as long as the tensor: a DLTensor
 * points at its shape array instead of owning it. */
static void sk_fill_dltensor(DLManagedTensor *t, void *data, long rows, long cols,
                             int device_type, int dtype_code, int64_t *shape)
{
  shape[0] = rows; shape[1] = cols;
  memset(t, 0, sizeof(*t));
  t->dl_tensor.data = data;
  t->dl_tensor.device.device_type = device_type;
  t->dl_tensor.device.device_id = 0;
  t->dl_tensor.ndim = 2;
  t->dl_tensor.dtype.code = dtype_code;
  t->dl_tensor.dtype.bits = 32;
  t->dl_tensor.dtype.lanes = 1;
  t->dl_tensor.shape = shape;
  t->dl_tensor.strides = NULL;
  t->dl_tensor.byte_offset = 0;
}

/* Shared build core. `own` (may be NULL) is the device dataset this index will
 * keep alive; it is released if the build fails. */
static void *sk_cagra_build(DLManagedTensor *dset, long n, long dim, sk_cuvs_stage *own)
{
  const long gd    = sk_env_long("SK_CUVS_GRAPH_DEG", SK_GRAPH_DEG, 2, 4096);
  const long igd   = sk_env_long("SK_CUVS_INTERMEDIATE_DEG", SK_INTERMEDIATE_DEG, 2, 8192);
  const long niter = sk_env_long("SK_CUVS_NN_NITER", SK_NN_DESCENT_NITER, 1, 10000);

  const int64_t resource_start = sk_profile_begin();
  int64_t build_start = 0;
  double t0 = 0, t1 = 0;
  sk_cuvs_index *h = (sk_cuvs_index *)calloc(1, sizeof(*h));
  if (h == NULL) { return NULL; }
  if (cuvsResourcesCreate(&h->res) != CUVS_SUCCESS) { free(h); return NULL; }
  if (cuvsCagraIndexParamsCreate(&h->ip) != CUVS_SUCCESS) { goto fail_res; }
  h->ip->build_algo = NN_DESCENT;
  h->ip->graph_degree = gd;
  h->ip->intermediate_graph_degree = igd;
  h->ip->nn_descent_niter = niter;
  if (cuvsCagraIndexCreate(&h->idx) != CUVS_SUCCESS) { goto fail_ip; }

  seekdb_vprofile_span("gpu.resources", h, resource_start, n, dim, 0, 0, 0);
  build_start = sk_profile_begin();
  t0 = sk_now_ms();
  if (cuvsCagraBuild(h->res, h->ip, dset, h->idx) != CUVS_SUCCESS) {
    seekdb_vprofile_span("gpu.cagra_build", h, build_start, n, dim, 0, 0, 1);
    goto fail_idx;
  }
  if (sk_profile_sync() != 0) {
    seekdb_vprofile_span("gpu.cagra_build", h, build_start, n, dim, 0, 0, 1);
    goto fail_idx;
  }
  seekdb_vprofile_span("gpu.cagra_build", h, build_start, n, dim, 0, 0, 0);
  t1 = sk_now_ms();

  h->n = n; h->dim = dim;
  if (sk_timing_on()) {
    fprintf(stderr,
            "[seekdb_cuvs] build n=%ld dim=%ld gd=%ld igd=%ld niter=%ld device=%d "
            "upload_ms=%.3f build_ms=%.3f\n",
            n, dim, gd, igd, niter, dset->dl_tensor.device.device_type == kDLCUDA,
            own != NULL ? own->upload_ms : 0.0, t1 - t0);
  }
  if (own != NULL) {
    /* The stage struct itself is no longer needed; only its device buffer is,
     * and that stays owned by the index. Steal it into a bare holder. */
    sk_cuvs_stage *keep = (sk_cuvs_stage *)calloc(1, sizeof(*keep));
    if (keep != NULL) {
      keep->dev = own->dev; keep->dim = own->dim; keep->cap = own->cap; keep->rows = own->rows;
      own->dev = NULL;
      seekdb_cuvs_stage_free(own);
    } else {
      keep = own;
    }
    h->stage = keep;
  }
  return h;

fail_idx: cuvsCagraIndexDestroy(h->idx);
fail_ip:  cuvsCagraIndexParamsDestroy(h->ip);
fail_res: cuvsResourcesDestroy(h->res); free(h);
  fprintf(stderr, "[seekdb_cuvs] build failed: %s\n", cuvsGetLastErrorText());
  return NULL;
}

void *seekdb_cuvs_stage_create(long n, long dim)
{
  if (n <= 0 || dim <= 0) { return NULL; }
  sk_cuvs_stage *s = (sk_cuvs_stage *)calloc(1, sizeof(*s));
  if (s == NULL) { return NULL; }
  s->dim = dim; s->cap = n;
  const int64_t alloc_start = sk_profile_begin();
  if (cudaMalloc((void **)&s->dev, sizeof(float) * (size_t)n * (size_t)dim) != cudaSuccess) {
    fprintf(stderr, "[seekdb_cuvs] device dataset alloc failed for n=%ld dim=%ld: %s\n",
            n, dim, cudaGetErrorString(cudaGetLastError()));
    free(s);
    return NULL;
  }
  seekdb_vprofile_span("gpu.dataset_alloc", s, alloc_start, n, dim, 0, 0, 0);
  const int64_t pinned_start = sk_profile_begin();
  const size_t chunk_bytes = sizeof(float) * (size_t)SK_STAGE_CHUNK_ROWS * (size_t)dim;
  for (int i = 0; i < SK_STAGE_BUFS; ++i) {
    if (cudaHostAlloc((void **)&s->pinned[i], chunk_bytes, cudaHostAllocDefault) != cudaSuccess) {
      seekdb_cuvs_stage_free(s);
      return NULL;
    }
  }
  if (cudaStreamCreate(&s->stream) != cudaSuccess) {
    seekdb_cuvs_stage_free(s);
    return NULL;
  }
  seekdb_vprofile_span("gpu.pinned_alloc", s, pinned_start, n, dim, 0, 0, 0);
  return s;
}

int seekdb_cuvs_stage_put(void *handle, const float *host, long row_offset, long rows)
{
  sk_cuvs_stage *s = (sk_cuvs_stage *)handle;
  if (s == NULL || host == NULL || rows <= 0) { return 1; }
  if (row_offset != s->rows || row_offset + rows > s->cap) { return 2; }

  const double t0 = sk_now_ms();
  const int64_t upload_start = sk_profile_begin();
  int64_t pack_ns = 0, transfer_ns = 0;
  const size_t row_bytes = sizeof(float) * (size_t)s->dim;
  long done = 0;
  while (done < rows) {
    long take = rows - done;
    if (take > SK_STAGE_CHUNK_ROWS) { take = SK_STAGE_CHUNK_ROWS; }
    float *buf = s->pinned[s->next_buf];
    /* Reusing this pinned buffer requires its previous DMA to have landed. */
    if (cudaStreamSynchronize(s->stream) != cudaSuccess) { return 3; }
    const int64_t pack_start = sk_profile_begin();
    memcpy(buf, host + (size_t)(row_offset + done) * (size_t)s->dim, row_bytes * (size_t)take);
    if (pack_start != 0) { pack_ns += seekdb_vprofile_now() - pack_start; }
    const int64_t transfer_start = sk_profile_begin();
    if (cudaMemcpyAsync(s->dev + (size_t)(row_offset + done) * (size_t)s->dim, buf,
                        row_bytes * (size_t)take, cudaMemcpyHostToDevice,
                        s->stream) != cudaSuccess) { return 4; }
    if (transfer_start != 0) {
      if (cudaStreamSynchronize(s->stream) != cudaSuccess) { return 5; }
      transfer_ns += seekdb_vprofile_now() - transfer_start;
    }
    s->next_buf = (s->next_buf + 1) % SK_STAGE_BUFS;
    done += take;
  }
  s->rows += rows;
  s->upload_ms += sk_now_ms() - t0;
  if (upload_start != 0) {
    seekdb_vprofile_emit("gpu.host_pack", s, upload_start, pack_ns / 1e6,
        rows, s->dim, 0, 0, 0, "span", 1);
    seekdb_vprofile_emit("gpu.h2d_base", s, upload_start, transfer_ns / 1e6,
        rows, s->dim, 0, 0, 0, "span", 1);
  }
  return 0;
}

void *seekdb_cuvs_build_stage(void *handle)
{
  sk_cuvs_stage *s = (sk_cuvs_stage *)handle;
  if (s == NULL) { return NULL; }
  if (s->rows <= 0) { seekdb_cuvs_stage_free(s); return NULL; }
  if (cudaStreamSynchronize(s->stream) != cudaSuccess) {
    fprintf(stderr, "[seekdb_cuvs] stage sync failed\n");
    seekdb_cuvs_stage_free(s);
    return NULL;
  }
  DLManagedTensor dset;
  int64_t dset_shape[2];
  sk_fill_dltensor(&dset, s->dev, s->rows, s->dim, kDLCUDA, kDLFloat, dset_shape);
  void *h = sk_cagra_build(&dset, s->rows, s->dim, s);
  if (h == NULL) { seekdb_cuvs_stage_free(s); }
  return h;
}

void seekdb_cuvs_stage_free(void *handle)
{
  sk_cuvs_stage *s = (sk_cuvs_stage *)handle;
  if (s == NULL) { return; }
  if (s->stream != NULL) { cudaStreamDestroy(s->stream); }
  for (int i = 0; i < SK_STAGE_BUFS; ++i) {
    if (s->pinned[i] != NULL) { cudaFreeHost(s->pinned[i]); }
  }
  if (s->dev != NULL) { cudaFree(s->dev); }
  free(s);
}

/* Host-input build: the baseline path, kept as the fallback if device staging
 * is unavailable (no VRAM, or hipVS rejects a device tensor). */
static void *sk_build_host(const float *base, long n, long dim)
{
  DLManagedTensor dset;
  int64_t dset_shape[2];
  sk_fill_dltensor(&dset, (void *)base, n, dim, kDLCPU, kDLFloat, dset_shape);
  return sk_cagra_build(&dset, n, dim, NULL);
}

static void *sk_build_impl(const float *base, long n, long dim)
{
  if (base == NULL || n <= 0 || dim <= 0) { return NULL; }
  void *s = seekdb_cuvs_stage_create(n, dim);
  if (s != NULL) {
    if (seekdb_cuvs_stage_put(s, base, 0, n) == 0) {
      void *h = seekdb_cuvs_build_stage(s);
      if (h != NULL) { return h; }
    } else {
      seekdb_cuvs_stage_free(s);
    }
  }
  seekdb_vprofile_span("route.host_input_retry", base, sk_profile_begin(), n, dim, 0, 0, 0);
  return sk_build_host(base, n, dim);
}

void *seekdb_cuvs_build(const float *base, long n, long dim)
{
  const int64_t start = sk_profile_begin();
  void *handle = sk_build_impl(base, n, dim);
  seekdb_vprofile_span("gpu.bridge_build_total", handle, start, n, dim, 0, 0, handle != NULL ? 0 : 1);
  return handle;
}

int seekdb_cuvs_search(void *handle, const float *query, long nq, long topk,
                       unsigned int *out_ids, float *out_dist)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  if (!h) return 1;
  int rc = 0;
  float *qd = 0; unsigned *nd = 0; float *dd = 0; cuvsCagraSearchParams_t sp = 0;
  const double t0 = sk_now_ms();
  const int64_t profile_start = sk_profile_begin();
  int64_t phase_start = profile_start;
  /* CAGRA needs at least one entry point per search_width lane and itopk must
   * cover topk, otherwise results past Top-64 come back short. The baseline
   * floor of 512 was a safety margin; itopk scales with topk instead, which
   * keeps deep Top-k correct without paying 512-wide candidate lists for Top-10. */
  const long itopk_env = sk_env_long("SK_CUVS_ITOPK", 0, 0, 1 << 20);
  const long width_env = sk_env_long("SK_CUVS_SEARCH_WIDTH", 0, 0, 64);
  size_t itopk;
  long width;
  if (itopk_env > 0) {
    itopk = (size_t)itopk_env;
  } else {
    itopk = (size_t)topk * 2;
    if (itopk < 128) itopk = 128;
    /* itopk_size must be a power of two per the CAGRA contract. */
    size_t pow2 = 128;
    while (pow2 < itopk) pow2 <<= 1;
    itopk = pow2;
  }
  width = width_env > 0 ? width_env : 4;

  if (cuvsRMMAlloc(h->res, (void **)&qd, sizeof(float) * nq * h->dim)  != CUVS_SUCCESS) { rc = 10; goto done; }
  if (cuvsRMMAlloc(h->res, (void **)&nd, sizeof(unsigned) * nq * topk) != CUVS_SUCCESS) { rc = 11; goto done; }
  if (cuvsRMMAlloc(h->res, (void **)&dd, sizeof(float) * nq * topk)    != CUVS_SUCCESS) { rc = 12; goto done; }
  seekdb_vprofile_span("gpu.query_alloc", h, phase_start, h->n, h->dim, nq, topk, 0);
  phase_start = sk_profile_begin();
  if (cudaMemcpy(qd, query, sizeof(float) * nq * h->dim, cudaMemcpyDefault) != cudaSuccess) { rc = 13; goto done; }
  if (sk_profile_sync() != 0) { rc = 17; goto done; }
  seekdb_vprofile_span("gpu.h2d_queries", h, phase_start, h->n, h->dim, nq, topk, 0);
  {
    DLManagedTensor qt, nt, dt;
    int64_t qshape[2], nshape[2], dshape[2];
    sk_fill_dltensor(&qt, qd, nq, h->dim, kDLCUDA, kDLFloat, qshape);
    sk_fill_dltensor(&nt, nd, nq, topk, kDLCUDA, kDLUInt, nshape);
    sk_fill_dltensor(&dt, dd, nq, topk, kDLCUDA, kDLFloat, dshape);
    if (cuvsCagraSearchParamsCreate(&sp) != CUVS_SUCCESS) { rc = 14; goto done; }
    if (sp->itopk_size < itopk) sp->itopk_size = itopk;
    sp->search_width = width;
    cuvsFilter f; f.type = NO_FILTER; f.addr = (uintptr_t)0;
    phase_start = sk_profile_begin();
    if (cuvsCagraSearch(h->res, sp, h->idx, &qt, &nt, &dt, f) != CUVS_SUCCESS) { rc = 15; goto done; }
    if (sk_profile_sync() != 0) { rc = 18; goto done; }
    seekdb_vprofile_span("gpu.cagra_search", h, phase_start, h->n, h->dim, nq, topk, 0);
    phase_start = sk_profile_begin();
    if (cudaMemcpy(out_ids, nd, sizeof(unsigned) * nq * topk, cudaMemcpyDefault) != cudaSuccess) { rc = 16; goto done; }
    if (out_dist && cudaMemcpy(out_dist, dd, sizeof(float) * nq * topk, cudaMemcpyDefault) != cudaSuccess) {
      rc = 19; goto done;
    }
    if (sk_profile_sync() != 0) { rc = 20; goto done; }
    seekdb_vprofile_span("gpu.d2h_results", h, phase_start, h->n, h->dim, nq, topk, 0);
  }
done:
  phase_start = sk_profile_begin();
  if (sp) cuvsCagraSearchParamsDestroy(sp);
  if (dd) cuvsRMMFree(h->res, dd, sizeof(float) * nq * topk);
  if (nd) cuvsRMMFree(h->res, nd, sizeof(unsigned) * nq * topk);
  if (qd) cuvsRMMFree(h->res, qd, sizeof(float) * nq * h->dim);
  seekdb_vprofile_span("gpu.query_free", h, phase_start, h->n, h->dim, nq, topk, rc);
  seekdb_vprofile_span("gpu.bridge_search_total", h, profile_start, h->n, h->dim, nq, topk, rc);
  if (sk_timing_on()) {
    fprintf(stderr, "[seekdb_cuvs] search nq=%ld topk=%ld itopk=%zu width=%ld rc=%d ms=%.3f\n",
            nq, topk, itopk, width, rc, sk_now_ms() - t0);
  }
  if (rc) fprintf(stderr, "[seekdb_cuvs] search rc=%d: %s\n", rc, cuvsGetLastErrorText());
  return rc;
}

void seekdb_cuvs_free(void *handle)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  if (!h) return;
  cuvsCagraIndexDestroy(h->idx);
  cuvsCagraIndexParamsDestroy(h->ip);
  cuvsResourcesDestroy(h->res);
  seekdb_cuvs_stage_free(h->stage);
  free(h);
}

int seekdb_cuvs_serialize(void* handle, const char* filename)
{
  sk_cuvs_index* index = (sk_cuvs_index*)handle;
  if (!index || !filename) return 1;
  if (cuvsCagraSerialize(index->res, filename, index->idx, true) != CUVS_SUCCESS) {
    fprintf(stderr, "[seekdb_cuvs] serialize failed: %s\n", cuvsGetLastErrorText());
    return 1;
  }
  return 0;
}

void* seekdb_cuvs_deserialize(const char* filename, long n, long dim)
{
  if (!filename || n <= 0 || dim <= 0) return NULL;
  sk_cuvs_index* index = (sk_cuvs_index*)calloc(1, sizeof(*index));
  if (!index) return NULL;
  if (cuvsResourcesCreate(&index->res) != CUVS_SUCCESS) goto fail;
  if (cuvsCagraIndexParamsCreate(&index->ip) != CUVS_SUCCESS) goto fail_res;
  if (cuvsCagraIndexCreate(&index->idx) != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraDeserialize(index->res, filename, index->idx) != CUVS_SUCCESS) goto fail_idx;
  {
    int64_t actual_n = 0;
    int64_t actual_dim = 0;
    if (cuvsCagraIndexGetSize(index->idx, &actual_n) != CUVS_SUCCESS ||
        cuvsCagraIndexGetDims(index->idx, &actual_dim) != CUVS_SUCCESS ||
        actual_n != n || actual_dim != dim) {
      fprintf(stderr, "[seekdb_cuvs] snapshot shape mismatch: %ld x %ld, expected %ld x %ld\n",
              (long)actual_n, (long)actual_dim, n, dim);
      goto fail_idx;
    }
  }
  index->n = n;
  index->dim = dim;
  return index;
fail_idx:
  cuvsCagraIndexDestroy(index->idx);
fail_ip:
  cuvsCagraIndexParamsDestroy(index->ip);
fail_res:
  cuvsResourcesDestroy(index->res);
fail:
  fprintf(stderr, "[seekdb_cuvs] deserialize failed: %s\n", cuvsGetLastErrorText());
  free(index);
  return NULL;
}

int seekdb_cuvs_cagra_knn(const float* base, long n, long dim,
                          const float* query, long nq, long topk,
                          unsigned int* out_ids)
{
  void *h = seekdb_cuvs_build(base, n, dim);
  if (!h) return 10;
  int rc = seekdb_cuvs_search(h, query, nq, topk, out_ids, NULL);
  seekdb_cuvs_free(h);
  return rc;
}
