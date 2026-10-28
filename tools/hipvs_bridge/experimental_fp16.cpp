// Development-only bridge experiment: FP16 graph construction, FP32 search.
// Not used by build.sh or the submission bridge. All GPU work is synchronized.
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <hip/hip_runtime.h>
#include <hip/hip_fp16.h>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <algorithm>
struct ExperimentIndex {
  cuvsResources_t res{};
  cuvsCagraIndexParams_t params{};
  cuvsCagraIndex_t graph{}, index{};
  float *base{};
  __half *half{};
  long n{}, dim{};
};
static DLManagedTensor tensor(void *data, int64_t *shape, int bits, int code=kDLFloat) {
  DLManagedTensor t{}; t.dl_tensor.data=data;
  t.dl_tensor.device={kDLCUDA,0}; t.dl_tensor.ndim=2;
  t.dl_tensor.dtype={static_cast<uint8_t>(code),static_cast<uint8_t>(bits),1};
  t.dl_tensor.shape=shape; return t;
}
__global__ void convert_half(const float *src, __half *dst, size_t n) {
  size_t i=blockIdx.x*blockDim.x+threadIdx.x;
  if(i<n) dst[i]=__float2half(src[i]);
}
extern "C" void seekdb_cuvs_free(void *p) {
  auto *h=static_cast<ExperimentIndex*>(p); if(!h)return;
  if(h->index)cuvsCagraIndexDestroy(h->index);
  if(h->graph)cuvsCagraIndexDestroy(h->graph);
  if(h->params)cuvsCagraIndexParamsDestroy(h->params);
  hipFree(h->half); hipFree(h->base);
  if(h->res)cuvsResourcesDestroy(h->res); delete h;
}
extern "C" void *seekdb_cuvs_build(const float *base,long n,long dim) {
  auto *h=new ExperimentIndex; h->n=n;h->dim=dim;
  int64_t shape[2]={n,dim}; DLManagedTensor graph{};
  auto fail=[&](){fprintf(stderr,"FP16 experiment failed: %s\n",cuvsGetLastErrorText());
    if(graph.deleter)graph.deleter(&graph);seekdb_cuvs_free(h);return static_cast<void*>(nullptr);};
  if(cuvsResourcesCreate(&h->res)!=CUVS_SUCCESS ||
     cuvsCagraIndexParamsCreate(&h->params)!=CUVS_SUCCESS ||
     cuvsCagraIndexCreate(&h->graph)!=CUVS_SUCCESS ||
     cuvsCagraIndexCreate(&h->index)!=CUVS_SUCCESS)return fail();
  const char *it=getenv("SK_CUVS_NN_NITER");
  h->params->build_algo=NN_DESCENT; h->params->graph_degree=128;
  h->params->intermediate_graph_degree=256;h->params->nn_descent_niter=it?atoi(it):100;
  size_t size=static_cast<size_t>(n)*dim;
  if(hipMalloc(&h->base,size*sizeof(float))!=hipSuccess ||
     hipMalloc(&h->half,size*sizeof(__half))!=hipSuccess ||
     hipMemcpy(h->base,base,size*sizeof(float),hipMemcpyHostToDevice)!=hipSuccess)return fail();
  hipLaunchKernelGGL(convert_half,dim3((size+255)/256),dim3(256),0,0,h->base,h->half,size);
  if(hipGetLastError()!=hipSuccess || hipDeviceSynchronize()!=hipSuccess)return fail();
  auto half=tensor(h->half,shape,16), full=tensor(h->base,shape,32);
  if(cuvsCagraBuild(h->res,h->params,&half,h->graph)!=CUVS_SUCCESS ||
     cuvsCagraIndexGetGraph(h->graph,&graph)!=CUVS_SUCCESS ||
     cuvsCagraIndexFromArgs(h->res,h->params->metric,&graph,&full,h->index)!=CUVS_SUCCESS ||
     hipDeviceSynchronize()!=hipSuccess)return fail();
  if(graph.deleter)graph.deleter(&graph);
  return h;
}
extern "C" int seekdb_cuvs_search(void *p,const float *query,long nq,long k,
                                 unsigned *ids,float *dist) {
  auto *h=static_cast<ExperimentIndex*>(p);
  float *q{},*d{};unsigned *i{}; cuvsCagraSearchParams_t params{};int rc=1;
  int64_t qs[2]={nq,h->dim},out[2]={nq,k};
  if(hipMalloc(&q,nq*h->dim*sizeof(float))==hipSuccess &&
     hipMalloc(&d,nq*k*sizeof(float))==hipSuccess &&
     hipMalloc(&i,nq*k*sizeof(unsigned))==hipSuccess &&
     hipMemcpy(q,query,nq*h->dim*sizeof(float),hipMemcpyHostToDevice)==hipSuccess &&
     cuvsCagraSearchParamsCreate(&params)==CUVS_SUCCESS) {
    params->itopk_size=512;while(params->itopk_size<size_t(k*4))params->itopk_size*=2;
    params->search_width=4;
    auto qt=tensor(q,qs,32),dt=tensor(d,out,32),it=tensor(i,out,32,kDLUInt);
    cuvsFilter filter{}; filter.type=NO_FILTER;
    if(cuvsCagraSearch(h->res,params,h->index,&qt,&it,&dt,filter)==CUVS_SUCCESS &&
       hipMemcpy(ids,i,nq*k*sizeof(unsigned),hipMemcpyDeviceToHost)==hipSuccess &&
       hipMemcpy(dist,d,nq*k*sizeof(float),hipMemcpyDeviceToHost)==hipSuccess &&
       hipDeviceSynchronize()==hipSuccess)rc=0;
  }
  if(rc)fprintf(stderr,"FP32 query failed: %s\n",cuvsGetLastErrorText());
  if(params)cuvsCagraSearchParamsDestroy(params);hipFree(i);hipFree(d);hipFree(q);return rc;
}
