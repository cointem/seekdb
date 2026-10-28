#ifndef SEEKDB_CUVS_BRIDGE_H
#define SEEKDB_CUVS_BRIDGE_H
#ifdef __cplusplus
extern "C" {
#endif
/* One-shot: build a CAGRA index over base (n x dim, row-major f32) on the GPU and
 * search queries (nq x dim), writing top-k uint32 neighbor ids to out_ids
 * (length nq*topk). Returns 0 on success, non-zero on error. */
int seekdb_cuvs_cagra_knn(const float* base, long n, long dim,
                          const float* query, long nq, long topk,
                          unsigned int* out_ids);

/* Build-once / search-many (used by seekdb's ob_vsag_adaptor GPU path).
 *  - seekdb_cuvs_build: builds a CAGRA index on the GPU from base (n x dim,
 *    row-major f32) and returns an opaque handle (NULL on error). cuVS copies
 *    the dataset to the device, so `base` may be freed after this returns.
 *  - seekdb_cuvs_search: runs top-k search for nq queries (row-major f32, dim
 *    cols), writing uint32 row offsets to out_ids[nq*topk] and (if non-NULL) L2
 *    distances to out_dist[nq*topk]. Returns 0 on success.
 *  - seekdb_cuvs_free: releases the handle.
 * All ROCm/cuVS state lives inside the .so; seekdb only ever sees these C symbols. */
void* seekdb_cuvs_build(const float* base, long n, long dim);
int   seekdb_cuvs_search(void* handle, const float* query, long nq, long topk,
                         unsigned int* out_ids, float* out_dist);
void  seekdb_cuvs_free(void* handle);
int seekdb_cuvs_serialize(void* handle, const char* filename);
void* seekdb_cuvs_deserialize(const char* filename, long n, long dim);

/* Chunked host->device staging for the base dataset.
 *
 * seekdb's index build scans the table row by row and spends that time building
 * the CPU VSAG index. Uploading the vectors to the GPU while the scan runs takes
 * the transfer off the critical path instead of paying it inside the CAGRA build.
 *
 *  - seekdb_cuvs_stage_create: allocates an n x dim device dataset plus pinned
 *    staging buffers. Returns an opaque handle, NULL if the GPU cannot host it
 *    (callers then fall back to seekdb_cuvs_build).
 *  - seekdb_cuvs_stage_put: uploads `rows` vectors starting at `row_offset`.
 *    Calls must be contiguous and ascending from 0. Returns 0 on success.
 *  - seekdb_cuvs_build_stage: builds CAGRA over what has been uploaded and
 *    consumes the handle (whether it succeeds or not). Returns an index handle
 *    usable with seekdb_cuvs_search / seekdb_cuvs_free.
 *  - seekdb_cuvs_stage_free: releases a stage handle that was not built. */
void* seekdb_cuvs_stage_create(long n, long dim);
int   seekdb_cuvs_stage_put(void* stage, const float* host, long row_offset, long rows);
void* seekdb_cuvs_build_stage(void* stage);
void  seekdb_cuvs_stage_free(void* stage);
#ifdef __cplusplus
}
#endif
#endif
