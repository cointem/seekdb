#!/usr/bin/env python3
"""Repeatable SQL workload for a source-built, already running seekdb.

Requires numpy and the mysql CLI. Uses a new database, never drops user tables.
Fresh indexes are built each round; the observer process is NOT restarted.
"""
import argparse
import hashlib
import json
import subprocess
import time
from pathlib import Path
np = None


def vector_literal(row):
    return '[' + ','.join(format(float(x), '.9g') for x in row) + ']'


def exact_topk(base, queries, k):
    import numpy as np
    truth = []
    for query in queries:
        distances = np.empty(len(base), dtype=np.float64)
        for offset in range(0, len(base), 4096):
            chunk = np.asarray(base[offset:offset + 4096], dtype=np.float64) - query
            distances[offset:offset + len(chunk)] = np.einsum('ij,ij->i', chunk, chunk)
        truth.append(np.argsort(distances, kind='stable')[:k].tolist())
    return truth


def quality(ids, truth, n, k):
    valid = len(ids) == k and len(set(ids)) == k and all(0 <= x < n for x in ids)
    return valid, len(set(ids) & set(truth[:k])) / k


def file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=2981)
    parser.add_argument('--user', default='root')
    parser.add_argument('--base', type=Path, default=Path('/seekdb_workspace/datasets/base.f32'))
    parser.add_argument('--queries', type=Path, default=Path('/seekdb_workspace/datasets/query.f32'))
    parser.add_argument('--dim', type=int, default=128)
    parser.add_argument('--n', type=int, default=10000)
    parser.add_argument('--nq', type=int, default=100)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--libraries',nargs='+',choices=['vsag','cuvs'],default=['vsag','cuvs'])
    parser.add_argument('--ef-search', type=int, default=64)
    parser.add_argument('--label', required=True)
    parser.add_argument('--commit', default='unknown')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    global np
    try:
        import numpy as np
    except ImportError:
        parser.error("numpy is required (available in the official GPU notebook environment)")
    if min(args.dim,args.n,args.nq,args.rounds,args.ef_search) <= 0 or args.n < 100:
        parser.error('positive sizes required; n must be at least 100 for Recall@100')
    if args.output.exists():
        parser.error('choose a new output directory for each run')
    def load(path, count):
        if path.stat().st_size % (args.dim * 4):
            parser.error(str(path) + ' size is not divisible by dim * sizeof(float32)')
        array = np.memmap(path,dtype='<f4',mode='r').reshape(-1,args.dim)
        if len(array) < count:
            parser.error(str(path) + ' has fewer rows than requested')
        selected = np.asarray(array[:count])
        if not np.isfinite(selected).all():
            parser.error(str(path) + ' contains NaN or infinity')
        return selected
    base, queries = load(args.base,args.n),load(args.queries,args.nq)
    truth = exact_topk(base,queries,100)
    args.output.mkdir(parents=True)
    database = 'vp_' + str(time.time_ns())
    report = dict(version=1,label=args.label,commit=args.commit,database=database,
                  n=args.n,dim=args.dim,nq=args.nq,rounds=args.rounds,libraries=args.libraries,
                  ef_search=args.ef_search,base_sha256=file_hash(args.base),queries_sha256=file_hash(args.queries),
                  mode='fresh_index_warm_process',cases=[],status='running',
                  caveats=['SQL latencies include mysql CLI startup and connections.',
                           'Rounds create new tables/indexes; observer/context/OS caches are not reset.',
                           'batch_knn rebuilds an index per call in the current implementation; its QPS includes this work.',
                           'GPU execution must be verified against source profile/trace records.',
                           'Profile-enabled timings are diagnostic, not official performance scores.'])
    def save():
        (args.output/'workload.json').write_text(json.dumps(report,indent=2)+'\n')
    def sql(statement):
        p=subprocess.run(['mysql','--connect-timeout=10','-h'+args.host,'-P'+str(args.port),'-u'+args.user,'-N','-B'],
                         input=statement,text=True,capture_output=True,timeout=600)
        if p.returncode:
            raise RuntimeError(p.stderr.strip())
        return p.stdout
    def query(table, literal, k):
        return f"USE {database}; SELECT id FROM {table} ORDER BY l2_distance(v,'{literal}') APPROXIMATE LIMIT {k};"
    def insert(table, ids, vectors):
        for offset in range(0,len(ids),250):
            rows=','.join(f"({int(i)},'{vector_literal(v)}')" for i,v in zip(ids[offset:offset+250],vectors[offset:offset+250]))
            sql(f'USE {database}; INSERT INTO {table} VALUES '+rows+';')
    try:
        report['server_version']=sql('SELECT version();').strip()
        # No global server parameters are changed. Configure vector memory at startup.
        sql(f'CREATE DATABASE {database}; USE {database}; CREATE TABLE probes(id BIGINT PRIMARY KEY,v VECTOR({args.dim})); CREATE TABLE batch_out(probe_id BIGINT,neighbor_id BIGINT,distance FLOAT,rk INT);')
        insert('probes',range(100000,100000+args.nq),queries)
        for round_no in range(args.rounds):
            for lib in (['vsag','cuvs'] if round_no%2==0 else ['cuvs','vsag']):
                if lib not in args.libraries:continue
                table=f't_{lib}_{round_no}'
                case=dict(round=round_no,lib=lib,table=table,status='running')
                report['cases'].append(case);save()
                sql(f'USE {database}; CREATE TABLE {table}(id BIGINT PRIMARY KEY,v VECTOR({args.dim}));')
                begin=time.monotonic_ns();insert(table,range(args.n),base)
                case['import_ms']=(time.monotonic_ns()-begin)/1e6
                case['build_start_ns']=time.monotonic_ns()
                sql(f'USE {database}; CREATE VECTOR INDEX idx ON {table}(v) WITH(distance=l2,type=hnsw,lib={lib},m=16,ef_construction=200,ef_search={args.ef_search});')
                ddl_end=time.monotonic_ns()
                first=sql(query(table,vector_literal(queries[0]),10))
                case['build_end_ns']=time.monotonic_ns()
                case['ddl_ms']=(ddl_end-case['build_start_ns'])/1e6
                case['first_query_ms']=(case['build_end_ns']-ddl_end)/1e6
                case['build_plus_first_ms']=(case['build_end_ns']-case['build_start_ns'])/1e6
                valid,_=quality([int(x) for x in first.split()],truth[0],args.n,10)
                if not valid:raise RuntimeError('invalid first query IDs')
                case['queries']={}
                case['query_start_ns']=time.monotonic_ns()
                for k in [10,100]:
                    latencies=[];recalls=[]
                    for q,gt in zip(queries,truth):
                        statement=query(table,vector_literal(q),k)
                        begin=time.monotonic_ns();out=sql(statement);elapsed=(time.monotonic_ns()-begin)/1e6
                        valid,recall=quality([int(x) for x in out.split()],gt,args.n,k)
                        if not valid:raise RuntimeError(f'invalid IDs for {table}, k={k}')
                        latencies.append(elapsed);recalls.append(recall)
                    case['queries'][str(k)]=dict(recall=float(np.mean(recalls)),latency_p50_ms=float(np.percentile(latencies,50)),
                        latency_p95_ms=float(np.percentile(latencies,95)),client_qps=1000*args.nq/sum(latencies),latencies_ms=latencies)
                case['query_end_ns']=time.monotonic_ns()
                if lib=='cuvs':
                    case['batch_start_ns']=time.monotonic_ns()
                    for k in [10,100]:
                        begin=time.monotonic_ns();sql(f"USE {database}; CALL dbms_vector.batch_knn('{table}','probes',{k},'batch_out');")
                        elapsed=(time.monotonic_ns()-begin)/1e6
                        out=sql(f'USE {database}; SELECT probe_id,neighbor_id FROM batch_out ORDER BY probe_id,rk;')
                        groups={100000+i:[] for i in range(args.nq)}
                        for line in out.splitlines():
                            probe,neighbor=map(int,line.split());groups[probe].append(neighbor)
                        recalls=[]
                        for i,gt in enumerate(truth):
                            valid,recall=quality(groups[100000+i],gt,args.n,k)
                            if not valid:raise RuntimeError(f'invalid batch IDs, k={k}')
                            recalls.append(recall)
                        case.setdefault('batch',{})[str(k)]=dict(wall_ms=elapsed,client_qps=1000*args.nq/elapsed,recall=float(np.mean(recalls)))
                    case['batch_end_ns']=time.monotonic_ns()
                case['status']='ok';save()
                print(json.dumps(case),flush=True)
        report['status']='ok'
    except Exception as error:
        report['status']='failed';report['error']=str(error)
        if report['cases']:report['cases'][-1]['status']='failed'
        save();raise
    save()
    print(args.output/'workload.json')


if __name__=='__main__':
    main()
