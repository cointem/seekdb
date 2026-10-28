#!/usr/bin/env python3
"""Search-width/candidate-list comparison on one freshly built FP32 GPU index."""
import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import statistics
import time
import numpy as np
from profile_workload import exact_topk, quality, file_hash


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bridge',type=Path,required=True)
    p.add_argument('--base',type=Path,required=True)
    p.add_argument('--queries',type=Path,required=True)
    p.add_argument('--dim',type=int,required=True)
    p.add_argument('--n',type=int,required=True)
    p.add_argument('--nq',type=int,default=100)
    p.add_argument('--widths',type=int,nargs='+',default=[1,2,4,8])
    p.add_argument('--itopks',type=int,nargs='+',default=[512,1024])
    p.add_argument('--rounds',type=int,default=3)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if a.output.exists():p.error('choose a new output file')
    if min(a.n,a.dim,a.nq,a.rounds,*a.widths,*a.itopks)<=0 or a.n<100:p.error('invalid sizes')
    base=np.fromfile(a.base,dtype='<f4').reshape(-1,a.dim)[:a.n].copy()
    q=np.fromfile(a.queries,dtype='<f4').reshape(-1,a.dim)[:a.nq].copy()
    if len(base)!=a.n or len(q)!=a.nq:p.error('insufficient data')
    if not np.isfinite(base).all() or not np.isfinite(q).all():p.error('non-finite data')
    truth=exact_topk(base,q,100)
    os.environ['SEEKDB_VECTOR_PROFILE']='0'
    lib=C.CDLL(str(a.bridge.resolve()));ptr=C.c_void_p
    lib.seekdb_cuvs_build.argtypes=[ptr,C.c_long,C.c_long];lib.seekdb_cuvs_build.restype=ptr
    lib.seekdb_cuvs_search.argtypes=[ptr,ptr,C.c_long,C.c_long,ptr,ptr];lib.seekdb_cuvs_search.restype=C.c_int
    lib.seekdb_cuvs_free.argtypes=[ptr]
    hip=C.CDLL('/opt/rocm/lib/libamdhip64.so');hip.hipDeviceSynchronize.restype=C.c_int
    def sync():
        if hip.hipDeviceSynchronize()!=0:raise RuntimeError('HIP sync failed')
    report={'scope':'same newly built graph; query tuning only, not a build benchmark',
        'bridge_sha256':hashlib.sha256(a.bridge.read_bytes()).hexdigest(),
        'base_sha256':file_hash(a.base),'query_sha256':file_hash(a.queries),
        'n':a.n,'dim':a.dim,'nq':a.nq,'runs':[],
        'build_environment':{k:v for k,v in os.environ.items() if k.startswith('SK_CUVS_')}}
    def save():a.output.write_text(json.dumps(report,indent=2))
    start=time.perf_counter();h=lib.seekdb_cuvs_build(base.ctypes.data,a.n,a.dim)
    if not h:raise RuntimeError('build failed')
    try:
        sync();report['build_ms']=(time.perf_counter()-start)*1000
        configs=[(w,i) for w in a.widths for i in a.itopks]
        for round_no in range(a.rounds):
            for width,itopk in configs if round_no%2==0 else reversed(configs):
                os.environ['SK_CUVS_SEARCH_WIDTH']=str(width);os.environ['SK_CUVS_ITOPK']=str(itopk)
                result={'round':round_no,'width':width,'itopk':itopk,'queries':{}}
                for k in (10,100):
                    ids=np.empty((a.nq,k),dtype=np.uint32);dist=np.empty((a.nq,k),dtype=np.float32)
                    durations=[]
                    for repeat in range(4):
                        start=time.perf_counter()
                        rc=lib.seekdb_cuvs_search(h,q.ctypes.data,a.nq,k,ids.ctypes.data,dist.ctypes.data)
                        sync()
                        if rc:raise RuntimeError(f'query failed width={width} itopk={itopk}: {rc}')
                        if repeat:durations.append((time.perf_counter()-start)*1000)
                    recall=[]
                    for found,gt in zip(ids,truth):
                        valid,r=quality(found.tolist(),list(gt),a.n,k)
                        if not valid:raise RuntimeError('invalid IDs')
                        recall.append(r)
                    result['queries'][str(k)]={'recall':float(np.mean(recall)),
                        'batch_ms_median':statistics.median(durations),'batch_ms':durations}
                report['runs'].append(result);save();print(json.dumps(result),flush=True)
    finally:
        lib.seekdb_cuvs_free(h)


if __name__=='__main__':main()
