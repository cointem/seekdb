#!/usr/bin/env python3
"""Host-side profile regression tests; fake GPU APIs do not validate GPU speed/Recall."""
import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HEADER = ROOT / 'src/oblib/lib/vector/seekdb_vector_profile.h'
COLLECTOR = ROOT / 'tools/hipvs_bridge/summarize_profile.py'
BRIDGE = ROOT / 'tools/hipvs_bridge/seekdb_cuvs_bridge.c'
spec = importlib.util.spec_from_file_location('profile_summary', COLLECTOR)
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)

FAKE_API = r'''
#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
typedef struct { void *data; struct { int device_type, device_id; } device;
 int ndim; struct { int code, bits, lanes; } dtype; int64_t *shape, *strides; uint64_t byte_offset; } DLTensor;
typedef struct { DLTensor dl_tensor; void *manager_ctx; void (*deleter)(void*); } DLManagedTensor;
enum { kDLCPU=1, kDLCUDA=2, kDLFloat=2, kDLUInt=1, CUVS_SUCCESS=0, NN_DESCENT=1,
 NO_FILTER=0, hipSuccess=0, hipHostAllocDefault=0, cudaSuccess=0, cudaHostAllocDefault=0, cudaMemcpyHostToDevice=1, cudaMemcpyDefault=4 };
typedef void *cuvsResources_t;
typedef struct Params { int build_algo, graph_degree, intermediate_graph_degree, nn_descent_niter; } *cuvsCagraIndexParams_t;
typedef struct Index { float *data; long rows, dim; } *cuvsCagraIndex_t;
typedef struct SearchParams { size_t itopk_size; long search_width; } *cuvsCagraSearchParams_t;
typedef struct { int type; uintptr_t addr; } cuvsFilter;
typedef void *cudaStream_t;
static int live_device=0,live_pinned=0,live_streams=0,sync_calls=0;
static int cuvsResourcesCreate(cuvsResources_t *p) { *p=malloc(1); return 0; }
static int cuvsResourcesDestroy(cuvsResources_t p) { free(p); return 0; }
static const char *cuvsGetLastErrorText(void) { return "injected test failure"; }
static int cuvsCagraIndexParamsCreate(cuvsCagraIndexParams_t *p) { *p=(cuvsCagraIndexParams_t)malloc(sizeof(**p));return 0; }
static int cuvsCagraIndexParamsDestroy(cuvsCagraIndexParams_t p) {free(p);return 0;}
static int cuvsCagraIndexCreate(cuvsCagraIndex_t *p) {*p=(cuvsCagraIndex_t)malloc(sizeof(**p));return 0;}
static int cuvsCagraIndexDestroy(cuvsCagraIndex_t p) {free(p);return 0;}
static int cuvsCagraBuild(cuvsResources_t r,cuvsCagraIndexParams_t p,DLManagedTensor *t,cuvsCagraIndex_t i) {
 (void)r;(void)p;i->data=(float*)t->dl_tensor.data;i->rows=t->dl_tensor.shape[0];i->dim=t->dl_tensor.shape[1];return 0; }
static int cuvsCagraSerialize(cuvsResources_t r,const char*f,cuvsCagraIndex_t i,bool data) {(void)r;(void)f;(void)i;(void)data;return 0;}
static int cuvsCagraDeserialize(cuvsResources_t r,const char*f,cuvsCagraIndex_t i) {(void)r;(void)f;(void)i;return 0;}
static int cuvsCagraIndexGetSize(cuvsCagraIndex_t i,int64_t*n) {*n=i->rows;return 0;}
static int cuvsCagraIndexGetDims(cuvsCagraIndex_t i,int64_t*n) {*n=i->dim;return 0;}
static int cuvsCagraSearchParamsCreate(cuvsCagraSearchParams_t *p) {*p=(cuvsCagraSearchParams_t)malloc(sizeof(**p));(*p)->itopk_size=0;return 0;}
static int cuvsCagraSearchParamsDestroy(cuvsCagraSearchParams_t p) {free(p);return 0;}
static int cuvsCagraSearch(cuvsResources_t r,cuvsCagraSearchParams_t p,cuvsCagraIndex_t idx,DLManagedTensor*q,DLManagedTensor*n,DLManagedTensor*d,cuvsFilter f) {
 (void)r;(void)p;(void)q;(void)f;volatile float check=idx->data[0];(void)check;
 for(long j=0;j<n->dl_tensor.shape[0]*n->dl_tensor.shape[1];++j) {((unsigned*)n->dl_tensor.data)[j]=(unsigned)(j%idx->rows);((float*)d->dl_tensor.data)[j]=0;}
 return 0; }
static int cuvsRMMAlloc(cuvsResources_t r,void **p,size_t n) {(void)r;*p=malloc(n);return 0;}
static int cuvsRMMFree(cuvsResources_t r,void*p,size_t n) {(void)r;(void)n;free(p);return 0;}
static int cudaMalloc(void **p,size_t n) {*p=malloc(n);++live_device;return 0;}
static int cudaFree(void*p) {free(p);--live_device;return 0;}
static int cudaHostAlloc(void **p,size_t n,int flags) {(void)flags;*p=malloc(n);++live_pinned;return 0;}
static int hipHostAlloc(void **p,size_t n,unsigned int flags) {return cudaHostAlloc(p,n,(int)flags);}
static int cudaFreeHost(void*p) {free(p);--live_pinned;return 0;}
static int cudaStreamCreate(cudaStream_t*p) {*p=malloc(1);++live_streams;return 0;}
static int cudaStreamDestroy(cudaStream_t p) {free(p);--live_streams;return 0;}
static int cudaStreamSynchronize(cudaStream_t p) {(void)p;++sync_calls;const char*f=getenv("PROFILE_FAULT");return f&&strcmp(f,"stage_sync")==0&&sync_calls==2?1:0;}
static int cudaMemcpy(void*d,const void*s,size_t n,int kind) {(void)kind;memcpy(d,s,n);return 0;}
static int cudaMemcpyAsync(void*d,const void*s,size_t n,int kind,cudaStream_t st) {(void)st;return cudaMemcpy(d,s,n,kind);}
static int hipDeviceSynchronize(void) {return 0;}
static int cudaGetLastError(void) {return 0;}
static const char*cudaGetErrorString(int e) {(void)e;return "test";}
'''


FAKE_MYSQL = r"""
import json,os,re,sys
from pathlib import Path
path=Path(os.environ['FAKE_MYSQL_STATE'])
tables=json.loads(path.read_text()) if path.exists() else {}
sql=sys.stdin.read()
if 'SELECT version()' in sql: print('fake-test-only')
match=re.search(r'INSERT INTO (\w+) VALUES',sql)
if match:
    table=tables.setdefault(match[1],{})
    for i,vec in re.findall(r"\((\d+),'(\[[^']+\])'\)",sql):table[i]=json.loads(vec)
match=re.search(r"SELECT id FROM (\w+) ORDER BY l2_distance\(v,'([^']+)'\) APPROXIMATE LIMIT (\d+)",sql)
def neighbors(table,query,k):
    return sorted(tables[table],key=lambda i:sum((a-b)**2 for a,b in zip(tables[table][i],query)))[:k]
if match:
    print('\n'.join(neighbors(match[1],json.loads(match[2]),int(match[3]))))
match=re.search(r"CALL dbms_vector.batch_knn\('(\w+)','probes',(\d+),'batch_out'\)",sql)
if match:
    tables['out']=[[int(probe),int(i)] for probe,q in sorted(tables['probes'].items(),key=lambda p:int(p[0])) for i in neighbors(match[1],q,int(match[2]))]
if 'SELECT probe_id,neighbor_id FROM batch_out' in sql:
    print('\n'.join(str(p)+'\t'+str(i) for p,i in tables['out']))
path.write_text(json.dumps(tables))
"""


class ProfileTests(unittest.TestCase):
    def test_counter_dedup_groups_and_malformed_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'observer.log'
            def event(kind, ms, calls, **extra):
                row = dict(version=1,pid=1,tid=2,key='0x1',stage='cpu.vsag_add',start_ns=1,
                           ms=ms,rows=calls,dim=0,nq=0,topk=0,rc=0,kind=kind,calls=calls)
                row.update(extra)
                return '[seekdb_profile] '+json.dumps(row)+'\n'
            path.write_text(event('counter',2,2)+event('counter',4,4)+event('counter',4,4)+
                            event('counter',1,1,start_ns=2)+event('span',3,1,stage='gpu.cagra_build')+
                            '[seekdb_profile] broken\n')
            rows, invalid, _ = summary.parse_records([path])
            self.assertEqual(len(rows),3)
            self.assertEqual(len(invalid),1)
            phases = {x['kind']:x for x in summary.summarize(rows)}
            self.assertEqual(phases['counter']['calls'],5)
            self.assertEqual(phases['counter']['total_ms'],5)
            self.assertEqual(phases['span']['total_ms'],3)

    @unittest.skipUnless(shutil.which('clang++'),'clang++ required')
    def test_multithreaded_counter_and_opt_in(self):
        code = '#include "'+str(HEADER)+'"\n'+r'''
#include <thread>
#include <vector>
int main() {
 int rc=0;VectorProfileScope scope("test.total",nullptr,rc);
 VectorProfileCounter counter("test.counter",nullptr);
 std::vector<std::thread> threads;
 for(int j=0;j<4;++j)threads.emplace_back([&](){for(int i=0;i<1000;++i){
 int64_t t=seekdb_vprofile_enabled()?seekdb_vprofile_now():0;counter.add(t,1,0);}});
 for(auto &t:threads)t.join();counter.flush();return 0;
}
'''
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'test.cpp';src.write_text(code);binary=Path(tmp)/'test'
            subprocess.run(['clang++','-std=c++17','-Wall','-Wextra','-Werror','-pthread',str(src),'-o',str(binary)],check=True,capture_output=True)
            for enabled in ('0','1'):
                env=dict(os.environ,SEEKDB_VECTOR_PROFILE=enabled)
                run=subprocess.run([str(binary)],capture_output=True,text=True,env=env,check=True)
                if enabled=='0': self.assertEqual(run.stderr,'');continue
                path=Path(tmp)/'run.log';path.write_text(run.stderr)
                rows,invalid,_=summary.parse_records([path]);self.assertFalse(invalid)
                counter=next(x for x in rows if x['kind']=='counter')
                self.assertEqual(counter['calls'],4000);self.assertEqual(counter['rows'],4000)

    @unittest.skipUnless(shutil.which('clang'),'clang required')
    def test_bridge_schema_and_failure_ownership_with_fake_gpu(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'fake.h').write_text(FAKE_API)
            for name in ['cuvs/core/c_api.h','cuvs/neighbors/cagra.h','cuvs/cuda_runtime.h','dlpack/dlpack.h','hip/hip_runtime_api.h']:
                p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('#include "fake.h"\n')
            code = r'''
#include <stdlib.h>
#include <string.h>
static int calloc_calls=0;
static void *profile_calloc(size_t n,size_t size) {
 ++calloc_calls;const char *f=getenv("PROFILE_FAULT");
 if(f&&strcmp(f,"keep_alloc")==0&&calloc_calls==3)return NULL;
 return calloc(n,size);
}
#define calloc profile_calloc
'''+'#include "'+str(BRIDGE)+'"\n'+r'''
#undef calloc
#include <assert.h>
int main() {
 float base[64]={1},query[2]={0},dist[10];unsigned ids[10];
 const char *f=getenv("PROFILE_FAULT");
 if(f&&strcmp(f,"empty_stage")==0) {
   void *s=seekdb_cuvs_stage_create(32,2);assert(s);
   assert(seekdb_cuvs_build_stage(s)==NULL);
 } else {
   void *h=seekdb_cuvs_build(base,32,2);assert(h);
   assert(seekdb_cuvs_search(h,query,1,10,ids,dist)==0);
   seekdb_cuvs_free(h);
 }
 assert(live_device==0&&live_pinned==0&&live_streams==0);return 0;
}
'''
            src=root/'test.c';src.write_text(code)
            binary=root/'test'
            subprocess.run(['clang','-std=c11','-D__HIP_PLATFORM_AMD__','-fsanitize=address','-g','-I'+str(root),str(src),'-o',str(binary)],check=True,capture_output=True)
            for enabled,fault in [('0',''),('1',''),('0','keep_alloc'),('0','stage_sync'),('1','empty_stage')]:
                env=dict(os.environ,SEEKDB_VECTOR_PROFILE=enabled,PROFILE_FAULT=fault,ASAN_OPTIONS='detect_leaks=0')
                run=subprocess.run([str(binary)],capture_output=True,text=True,env=env)
                self.assertEqual(run.returncode,0,run.stderr)
                if enabled=='1' and not fault:
                    path=root/'gpu.log';path.write_text(run.stderr)
                    rows,invalid,_=summary.parse_records([path]);self.assertFalse(invalid)
                    stages={x['stage'] for x in rows}
                    self.assertTrue({'gpu.cagra_build','gpu.h2d_base','gpu.cagra_search','gpu.bridge_build_total','gpu.bridge_search_total'}<=stages)
                elif enabled=='0': self.assertNotIn('[seekdb_profile]',run.stderr)
            # HIP builds may treat .c as C++; verify goto/declaration compatibility.
            subprocess.run(['clang++','-x','c++','-std=c++17','-D__HIP_PLATFORM_AMD__','-I'+str(root),'-fsyntax-only',str(src)],check=True,capture_output=True)


    def test_workload_end_to_end_with_fake_mysql(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest('numpy required for workload test')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            base=np.column_stack((np.arange(100,dtype=np.float32),np.zeros(100,dtype=np.float32)))
            probes=np.array([[.1,0],[50.1,0]],dtype=np.float32)
            base.tofile(root/'base.f32');probes.tofile(root/'query.f32')
            fake=root/'mysql'
            fake.write_text('#!'+__import__('sys').executable+'\n'+FAKE_MYSQL)
            fake.chmod(0o755)
            output=root/'results'
            env=dict(os.environ,PATH=str(root)+os.pathsep+os.environ.get('PATH',''),
                     FAKE_MYSQL_STATE=str(root/'state.json'),PYTHONDONTWRITEBYTECODE='1')
            run=subprocess.run([__import__('sys').executable,str(ROOT/'tools/hipvs_bridge/profile_workload.py'),
                '--base',str(root/'base.f32'),'--queries',str(root/'query.f32'),
                '--dim','2','--n','100','--nq','2','--rounds','2',
                '--label','test','--output',str(output)],capture_output=True,text=True,env=env)
            self.assertEqual(run.returncode,0,run.stderr)
            report=json.loads((output/'workload.json').read_text())
            self.assertEqual(report['status'],'ok')
            self.assertEqual([x['lib'] for x in report['cases']],['vsag','cuvs','cuvs','vsag'])
            for case in report['cases']:
                self.assertGreater(case['build_plus_first_ms'],case['ddl_ms'])
                for metric in case['queries'].values():self.assertEqual(metric['recall'],1)
                for metric in case.get('batch',{}).values():self.assertEqual(metric['recall'],1)


if __name__ == '__main__':
    unittest.main()
