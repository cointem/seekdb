#!/usr/bin/env python3
"""Alternate frozen/candidate source-built servers with fresh data directories.

Uses the same SQL workload; disables phase profiling. No official score is
claimed: mysql client startup contributes to query latency and OS caches remain.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import time


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('baseline','candidate'):
        p.add_argument('--'+name+'-bin',type=Path,required=True)
        p.add_argument('--'+name+'-bridge',type=Path,required=True)
        p.add_argument('--'+name+'-commit',required=True)
        p.add_argument('--'+name+'-env',type=Path,help='JSON object of explicit SK_CUVS_ overrides')
    p.add_argument('--base',type=Path,default=Path('/seekdb_workspace/datasets/base.f32'))
    p.add_argument('--queries',type=Path,default=Path('/seekdb_workspace/datasets/query.f32'))
    p.add_argument('--dim',type=int,default=128)
    p.add_argument('--n',type=int,default=10000)
    p.add_argument('--nq',type=int,default=100)
    p.add_argument('--baseline-kind',choices=['frozen','development'],default='frozen')
    p.add_argument('--baseline-niter',type=int)
    p.add_argument('--candidate-niter',type=int)
    p.add_argument('--gpu-only',action='store_true',help='omit unchanged CPU control; record this scope explicitly')
    p.add_argument('--rounds',type=int,default=3);p.add_argument('--port',type=int,default=2982)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    if args.rounds<1:p.error('rounds must be positive')
    if args.output.exists():p.error('choose a new output directory')
    with socket.socket() as sock:sock.bind(('127.0.0.1',args.port))
    args.output.mkdir(parents=True)
    report={'scope':'alternating fresh-process SQL comparison; not official score','runs':[],
            'profiling':False,'gpu_only':args.gpu_only,'port':args.port,'binaries':{},'baseline_kind':args.baseline_kind,
            'iteration_overrides':{'baseline':args.baseline_niter,'candidate':args.candidate_niter}}
    for name in ('baseline','candidate'):
        env_path=getattr(args,name+'_env')
        overrides=json.loads(env_path.read_text()) if env_path else {}
        if not isinstance(overrides,dict) or any(not k.startswith('SK_CUVS_') for k in overrides):
            p.error('only SK_CUVS_ environment overrides allowed')
        setattr(args,name+'_overrides',{k:str(v) for k,v in overrides.items()})
        binary=getattr(args,name+'_bin').resolve();bridge=getattr(args,name+'_bridge').resolve()
        def digest(path):
            h=hashlib.sha256()
            with path.open('rb') as f:
                for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
            return h.hexdigest()
        report['binaries'][name]={'commit':getattr(args,name+'_commit'),'binary_sha256':digest(binary),'bridge_sha256':digest(bridge),'environment_overrides':getattr(args,name+'_overrides')}
    def save():(args.output/'comparison.json').write_text(json.dumps(report,indent=2))
    mysql=['mysql','--connect-timeout=2','-h127.0.0.1','-P'+str(args.port),'-uroot','-e']
    for round_no in range(args.rounds):
        order=['baseline','candidate'] if round_no%2==0 else ['candidate','baseline']
        for name in order:
            run_dir=args.output/f'{round_no}-{name}';run_dir.mkdir()
            base=run_dir/'base';store=run_dir/'store';base.mkdir();store.mkdir()
            env={k:v for k,v in os.environ.items() if not k.startswith(('SEEKDB_VECTOR_PROFILE','SK_CUVS_'))}
            env.update(getattr(args,name+'_overrides'))
            niter=getattr(args,name+'_niter')
            if niter is not None:env['SK_CUVS_NN_NITER']=str(niter)
            bridge=getattr(args,name+'_bridge').resolve()
            env['LD_LIBRARY_PATH']=str(bridge.parent)+':/opt/hipvs/lib:/opt/rocm/lib'
            binary=getattr(args,name+'_bin').resolve()
            cmd=[str(binary),'--nodaemon','--port',str(args.port),'--base-dir',str(base.resolve()),'--data-dir',str(store.resolve())]
            for param in ['local_ip=127.0.0.1','memory_budget=8G','datafile_size=2G','datafile_maxsize=8G','log_disk_size=2G']:
                cmd+=['--parameter',param]
            result={'round':round_no,'variant':name,'status':'running'};report['runs'].append(result);save()
            with (run_dir/'observer.out').open('w') as log:
                proc=subprocess.Popen(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
                try:
                    for _ in range(180):
                        if proc.poll() is not None:raise RuntimeError('server exited during startup')
                        ready=subprocess.run(mysql+['select 1'],capture_output=True,timeout=5)
                        if ready.returncode==0:break
                        time.sleep(1)
                    else:raise RuntimeError('server startup timeout')
                    subprocess.run(mysql+['alter system set ob_vector_memory_limit_percentage=30'],check=True,capture_output=True,timeout=30)
                    workload=[sys.executable,str(Path(__file__).with_name('profile_workload.py')),
                        '--port',str(args.port),'--rounds','1','--label',name,'--commit',getattr(args,name+'_commit'),
                        '--output',str(run_dir/'workload'),'--base',str(args.base),'--queries',str(args.queries),
                        '--dim',str(args.dim),'--n',str(args.n),'--nq',str(args.nq)]
                    if args.gpu_only:workload+=['--libraries','cuvs']
                    with (run_dir/'workload.log').open('w') as out:
                        run=subprocess.run(workload,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=900)
                    result['status']='ok' if run.returncode==0 else 'failed'
                    path=run_dir/'workload'/'workload.json'
                    if path.exists():result['workload']=json.loads(path.read_text())
                    result['returncode']=run.returncode
                    server_log=base/'log'/'seekdb.log'
                    result['gpu_algorithm_retries']=0
                    if server_log.exists():
                        with server_log.open(errors='replace') as source:
                            result['gpu_algorithm_retries']=sum('retrying GPU NN-Descent' in line for line in source)
                except Exception as error:
                    result['status']='failed';result['error']=str(error)
                finally:
                    if proc.poll() is None:
                        proc.terminate()
                        try:proc.wait(timeout=45)
                        except subprocess.TimeoutExpired:proc.kill();proc.wait();result['forced_stop']=True
                    save()
            print(json.dumps({'round':round_no,'variant':name,'status':result['status']}),flush=True)
        if any(r['status']!='ok' for r in report['runs']):break
    report['status']='ok' if all(r['status']=='ok' for r in report['runs']) else 'failed'
    if report['status']=='ok':
        medians={}
        for name in ('baseline','candidate'):
            times=[case['build_plus_first_ms'] for r in report['runs'] if r['variant']==name
                   for case in r['workload']['cases'] if case['lib']=='cuvs']
            medians[name]=statistics.median(times)
        report['gpu_build_plus_first_median_ms']=medians
        report['observed_build_speedup']=medians['baseline']/medians['candidate']
    save();print(args.output/'comparison.json',flush=True)
    if report['status']!='ok':raise SystemExit(1)


if __name__=='__main__':main()
