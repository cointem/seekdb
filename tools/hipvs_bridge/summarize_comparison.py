#!/usr/bin/env python3
"""Evaluate observed relative gates; never label a development run an official score."""
import argparse
import json
from pathlib import Path
import statistics


def summarize(report):
    result={'scope':report['scope'],'baseline_kind':report['baseline_kind'],
            'status':report.get('status','incomplete'),'official_score':None,
            'absolute_recall_floor':None,'rounds':[]}
    runs=report['runs']
    if report.get('status')!='ok':return result
    for round_no in sorted({r['round'] for r in runs}):
        selected={r['variant']:r for r in runs if r['round']==round_no}
        cases={name:next(c for c in r['workload']['cases'] if c['lib']=='cuvs')
               for name,r in selected.items()}
        b,c=cases['baseline'],cases['candidate']
        row={'round':round_no,'baseline_build_ms':b['build_plus_first_ms'],
             'candidate_build_ms':c['build_plus_first_ms'],
             'candidate_gpu_algorithm_retries':selected['candidate'].get('gpu_algorithm_retries'),
             'queries':{},'batch':{}}
        for mode in ('queries','batch'):
            for k in ('10','100'):
                qb,qc=b[mode][k],c[mode][k]
                loss=qb['recall']-qc['recall']
                ratio=qc['client_qps']/qb['client_qps']
                row[mode][k]={'baseline_recall':qb['recall'],'candidate_recall':qc['recall'],
                    'recall_loss':loss,'throughput_ratio':ratio,
                    'relative_recall_pass':loss<=0.005+1e-12,'throughput_pass':ratio>=0.90}
        result['rounds'].append(row)
    rows=result['rounds']
    result['baseline_build_median_ms']=statistics.median(r['baseline_build_ms'] for r in rows)
    result['candidate_build_median_ms']=statistics.median(r['candidate_build_ms'] for r in rows)
    result['build_speedup']=result['baseline_build_median_ms']/result['candidate_build_median_ms']
    result['observed_relative_gates_pass']=all(q['relative_recall_pass'] and q['throughput_pass']
        for r in rows for mode in ('queries','batch') for q in r[mode].values())
    result['caveats']=['Absolute Recall floors and official evaluator are not supplied by this tool.',
        'Client query timings include CLI process/connection overhead.',
        'SQL batch timings include a fresh graph build and result writes.',
        'Development comparators do not establish a frozen-baseline contest score.']
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('report',type=Path)
    p.add_argument('--output',type=Path);a=p.parse_args()
    result=summarize(json.loads(a.report.read_text()));text=json.dumps(result,indent=2)
    if a.output:
        if a.output.exists():p.error('choose a new output file')
        a.output.write_text(text+'\n')
    print(text)


if __name__=='__main__':main()
