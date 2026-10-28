#!/usr/bin/env python3
"""Isolated-process CAGRA parameter sweep, not an end-to-end contest score."""
import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import numpy as np
from profile_workload import exact_topk, quality, file_hash


def worker(args):
    base=np.fromfile(args.base,dtype='<f4').reshape(-1,args.dim)[:args.n].copy()
    queries=np.fromfile(args.queries,dtype='<f4').reshape(-1,args.dim)[:args.nq].copy()
    truth=np.load(args.truth)
    lib=C.CDLL(str(args.bridge.resolve()))
    build_begin=time.perf_counter()
    pool_percent=int(os.environ.get('SK_CUVS_RMM_POOL_PERCENT','0'))
    if pool_percent:
        lib.cuvsRMMPoolMemoryResourceEnable.argtypes=[C.c_int,C.c_int,C.c_bool]
        lib.cuvsRMMPoolMemoryResourceEnable.restype=C.c_int
        if lib.cuvsRMMPoolMemoryResourceEnable(1,pool_percent,False)!=1:
            raise RuntimeError('RMM pool initialization failed')
    ptr=C.c_void_p
    lib.seekdb_cuvs_build.argtypes=[ptr,C.c_long,C.c_long];lib.seekdb_cuvs_build.restype=ptr
    lib.seekdb_cuvs_search.argtypes=[ptr,ptr,C.c_long,C.c_long,ptr,ptr];lib.seekdb_cuvs_search.restype=C.c_int
    lib.seekdb_cuvs_free.argtypes=[ptr]
    hip=C.CDLL('/opt/rocm/lib/libamdhip64.so');hip.hipDeviceSynchronize.restype=C.c_int
    def sync():
        if hip.hipDeviceSynchronize()!=0:raise RuntimeError('HIP synchronize failed')
    start=build_begin;handle=lib.seekdb_cuvs_build(base.ctypes.data,len(base),args.dim)
    if not handle:raise RuntimeError('GPU build failed')
    try:
        sync();build_ms=(time.perf_counter()-start)*1000
        result={'iterations':int(os.environ['SK_CUVS_NN_NITER']),'build_ms':build_ms,'queries':{}}
        for k in (10,100):
            ids=np.empty((len(queries),k),dtype=np.uint32);dist=np.empty_like(ids,dtype=np.float32)
            timings=[]
            for repeat in range(4):
                start=time.perf_counter()
                rc=lib.seekdb_cuvs_search(handle,queries.ctypes.data,len(queries),k,ids.ctypes.data,dist.ctypes.data)
                sync()
                if rc:raise RuntimeError('GPU search failed: '+str(rc))
                if repeat:timings.append((time.perf_counter()-start)*1000)
            recalls=[]
            for found,gt in zip(ids,truth):
                valid,recall=quality(found.tolist(),gt.tolist(),len(base),k)
                if not valid:raise RuntimeError('invalid IDs at k='+str(k))
                recalls.append(recall)
            result['queries'][str(k)]={'recall':float(np.mean(recalls)),
                'batch_ms_median':float(np.median(timings)),'batch_ms':timings}
        print(json.dumps(result),flush=True)
    finally:lib.seekdb_cuvs_free(handle)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bridge',type=Path,required=True)
    p.add_argument('--base',type=Path,default=Path('/seekdb_workspace/datasets/base.f32'))
    p.add_argument('--queries',type=Path,default=Path('/seekdb_workspace/datasets/query.f32'))
    p.add_argument('--dim',type=int,default=128);p.add_argument('--n',type=int,default=10000)
    p.add_argument('--nq',type=int,default=100);p.add_argument('--rounds',type=int,default=3)
    p.add_argument('--iterations',type=int,nargs='+',default=[100,64,32,16])
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--variants',type=Path,help='JSON list of named environment overrides; alternates order per round')
    p.add_argument('--worker',action='store_true',help=argparse.SUPPRESS)
    p.add_argument('--truth',type=Path,help=argparse.SUPPRESS)
    args=p.parse_args()
    if min(args.dim,args.n,args.nq,args.rounds,*args.iterations)<=0 or args.n<100:p.error('invalid dimensions or iterations')
    if args.worker:return worker(args)
    if args.output.exists():p.error('output must be a new directory')
    base=np.fromfile(args.base,dtype='<f4').reshape(-1,args.dim)[:args.n]
    queries=np.fromfile(args.queries,dtype='<f4').reshape(-1,args.dim)[:args.nq]
    if len(base)!=args.n or len(queries)!=args.nq:p.error('insufficient input rows')
    if not np.isfinite(base).all() or not np.isfinite(queries).all():p.error('non-finite input')
    args.output.mkdir(parents=True);truth_path=args.output/'truth.npy'
    np.save(truth_path,np.asarray(exact_topk(base,queries,100),dtype=np.int64))
    report={'scope':'bridge only, fresh process per build; not contest score',
        'bridge_sha256':hashlib.sha256(args.bridge.read_bytes()).hexdigest(),
        'n':args.n,'dim':args.dim,'nq':args.nq,'runs':[],
        'base_sha256':file_hash(args.base),'queries_sha256':file_hash(args.queries),
        'parameter_overrides':{k:v for k,v in os.environ.items() if k.startswith('SK_CUVS_')}}
    report_path=args.output/'results.json'
    variants=json.loads(args.variants.read_text()) if args.variants else [
        {'name':'iter-'+str(i),'environment':{'SK_CUVS_NN_NITER':str(i)}} for i in args.iterations]
    if not variants or len({v['name'] for v in variants})!=len(variants):p.error('empty or duplicate variants')
    for v in variants:
        if not all(c.isalnum() or c in '-_' for c in v['name']):p.error('invalid variant name')
        if any(not k.startswith('SK_CUVS_') for k in v['environment']):p.error('only SK_CUVS_ overrides allowed')
    report['variants']=variants
    for round_no in range(args.rounds):
        order=variants if round_no%2==0 else list(reversed(variants))
        for variant in order:
            env=dict(os.environ,SK_CUVS_NN_NITER='100',SEEKDB_VECTOR_PROFILE='0')
            env.update({k:str(v) for k,v in variant['environment'].items()})
            iterations=int(env['SK_CUVS_NN_NITER'])
            cmd=[sys.executable,str(Path(__file__).resolve()),'--worker','--bridge',str(args.bridge.resolve()),
                 '--base',str(args.base.resolve()),'--queries',str(args.queries.resolve()),'--dim',str(args.dim),
                 '--n',str(args.n),'--nq',str(args.nq),'--truth',str(truth_path.resolve()),'--output',str(args.output.resolve())]
            run=subprocess.run(cmd,env=env,text=True,capture_output=True,timeout=600)
            (args.output/f'round-{round_no}-{variant["name"]}.stderr').write_text(run.stderr)
            if run.returncode:
                report['failure']={'round':round_no,'variant':variant['name'],'iterations':iterations,'returncode':run.returncode,'stderr':run.stderr}
                report_path.write_text(json.dumps(report,indent=2));raise RuntimeError(str(report['failure']))
            result=json.loads(run.stdout);result['round']=round_no;result['variant']=variant['name'];report['runs'].append(result)
            report_path.write_text(json.dumps(report,indent=2));print(json.dumps(result),flush=True)


if __name__=='__main__':main()
