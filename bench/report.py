#!/usr/bin/env python3
"""Compare and render ukiel macro-bench results (plan 30) — no LLM, no deps.

Reads the JSON reports the `bench` binary writes under bench/results/:
  hits-queries-<label>.json   (read suite)
  bsky-run-<label>.json       (write path)

Usage:
  report.py compare BASELINE CANDIDATE   # terminal delta tables (lower ms = better)
  report.py html [OUT]                   # self-contained HTML of every run + a
                                         #   comparison picker (default: bench/results/report.html)
  report.py list                         # list available run labels
"""
import glob
import json
import os
import sys

RESULTS = os.path.join(os.path.dirname(__file__), "results")


def load(kind, label):
    """kind is 'hits-queries' or 'bsky-run'."""
    path = os.path.join(RESULTS, f"{kind}-{label}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def all_runs():
    """{'hits-queries': [labels], 'bsky-run': [labels]} present on disk."""
    out = {"hits-queries": [], "bsky-run": []}
    for kind in out:
        for p in sorted(glob.glob(os.path.join(RESULTS, f"{kind}-*.json"))):
            label = os.path.basename(p)[len(kind) + 1 : -len(".json")]
            out[kind].append(label)
    return out


def pct(base, cand):
    if base in (0, None) or cand is None:
        return "—"
    return f"{(cand - base) / base * 100:+.1f}%"


# ---------------------------------------------------------------- terminal compare


def hits_scenarios(run):
    """{class: {scenario: ms}} from a hits-queries report."""
    out = {}
    for t in run.get("tenants", []):
        out[t["class"]] = {s["name"]: s["median_ms"] for s in t["scenarios"]}
    return out


def compare_hits(base, cand):
    print(f"\n=== hits queries: {base['label']} → {cand['label']}  (median ms, Δ% — lower is better) ===")
    b, c = hits_scenarios(base), hits_scenarios(cand)
    classes = [k for k in ("heavy", "median", "light") if k in b and k in c]
    scenarios = list(dict.fromkeys(s for cls in b.values() for s in cls))
    header = f"{'scenario':<16}" + "".join(f"{cls:>26}" for cls in classes)
    print(header)
    for sc in scenarios:
        row = f"{sc:<16}"
        for cls in classes:
            bv, cv = b[cls].get(sc), c[cls].get(sc)
            if bv is None or cv is None:
                row += f"{'—':>26}"
            else:
                row += f"{f'{bv:.1f} → {cv:.1f} ({pct(bv, cv)})':>26}"
        print(row)


def compare_bsky(base, cand):
    print(f"\n=== bluesky run: {base['label']} → {cand['label']}  (Δ%; latency lower better, throughput higher better) ===")
    metrics = [
        ("produce_rows_per_s", "produce rows/s"),
        ("ingest_catchup_secs", "ingest catch-up s"),
        ("compaction_secs", "compaction s"),
        ("finalization_secs", "finalization s"),
        ("stored_rows", "stored rows"),
        ("backpressure_deferrals", "deferrals"),
    ]
    print(f"{'metric':<22}{'baseline':>16}{'candidate':>16}{'Δ':>10}")
    for key, name in metrics:
        bv, cv = base.get(key), cand.get(key)
        if bv is None or cv is None:
            continue
        print(f"{name:<22}{bv:>16.1f}{cv:>16.1f}{pct(bv, cv):>10}"
              if isinstance(bv, float) or isinstance(cv, float)
              else f"{name:<22}{bv:>16}{cv:>16}{pct(bv, cv):>10}")
    # per-tenant scenarios
    def tq(run):
        return {t["id"]: {s[0]: s[1] for s in t["scenarios"]} for t in run.get("tenant_queries", [])}
    bt, ct = tq(base), tq(cand)
    shared = [i for i in bt if i in ct]
    if shared:
        print(f"\n  per-tenant reads (median ms, Δ%):")
        scen = list(dict.fromkeys(s for t in bt.values() for s in t))
        for tid in shared:
            print(f"  tenant {tid}: " + "  ".join(
                f"{s} {bt[tid][s]:.1f}→{ct[tid][s]:.1f} ({pct(bt[tid][s], ct[tid][s])})"
                for s in scen if s in bt[tid] and s in ct[tid]))


def cmd_compare(base_label, cand_label):
    found = False
    for kind, fn in (("hits-queries", compare_hits), ("bsky-run", compare_bsky)):
        base, cand = load(kind, base_label), load(kind, cand_label)
        if base and cand:
            fn(base, cand)
            found = True
        elif base or cand:
            have = base_label if base else cand_label
            miss = cand_label if base else base_label
            print(f"\n[{kind}] only '{have}' present ('{miss}' missing) — skipped")
    if not found:
        print(f"no comparable runs for labels '{base_label}' and '{cand_label}'", file=sys.stderr)
        sys.exit(1)
    print()


# ---------------------------------------------------------------- html


def cmd_html(out_path):
    data = {}
    for kind in ("hits-queries", "bsky-run"):
        for label in all_runs()[kind]:
            run = load(kind, label)
            if kind == "bsky-run":
                run.pop("metrics_dump", None)  # huge; not needed in the page
            data[f"{kind}-{label}"] = run
    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(data))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        f.write(html)
    print(f"wrote {out_path} ({len(data)} runs)")


HTML_TEMPLATE = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>ukiel macro bench</title>
<style>
 body{font:14px/1.5 system-ui,sans-serif;margin:2rem;max-width:1100px;color:#111}
 h1{margin:0 0 .3rem} .sub{color:#666;margin-bottom:1.5rem}
 h2{margin:2rem 0 .5rem;border-bottom:1px solid #ddd;padding-bottom:.2rem}
 table{border-collapse:collapse;margin:.5rem 0 1.5rem;font-variant-numeric:tabular-nums}
 th,td{border:1px solid #ddd;padding:.25rem .6rem;text-align:right}
 th:first-child,td:first-child{text-align:left}
 th{background:#f5f5f5} .neg{color:#0a7d2c} .pos{color:#c0392b} .zero{color:#999}
 select{font:inherit;padding:.2rem}
 .card{background:#fafafa;border:1px solid #eee;border-radius:6px;padding:1rem;margin:1rem 0}
 code{background:#f0f0f0;padding:.1rem .3rem;border-radius:3px}
 @media(prefers-color-scheme:dark){body{background:#111;color:#eee}th{background:#222}
   th,td{border-color:#333}.card{background:#1a1a1a;border-color:#2a2a2a}code{background:#222}
   .sub,.zero{color:#999}h2{border-color:#333}}
</style></head><body>
<h1>ukiel macro perf — bench results</h1>
<div class="sub">Generated from <code>bench/results/*.json</code>. Lower ms is better; throughput higher is better.</div>

<h2>Compare two runs</h2>
<div class="card">
 <label>Baseline <select id="base"></select></label>
 &nbsp;→&nbsp;
 <label>Candidate <select id="cand"></select></label>
 <div id="cmp"></div>
</div>

<h2>All runs</h2>
<div id="runs"></div>

<script>
const DATA = __DATA__;
const pct = (b,c)=> (b===0||b==null||c==null) ? {t:'—',cls:'zero'}
  : (()=>{const p=(c-b)/b*100; return {t:(p>=0?'+':'')+p.toFixed(1)+'%', cls: p<-0.5?'neg':p>0.5?'pos':'zero'};})();
const el=(t,a={},...k)=>{const e=document.createElement(t);for(const[x,v]of Object.entries(a))x=='cls'?e.className=v:e.setAttribute(x,v);for(const c of k)e.append(c);return e;};

function tableFor(name, run){
  const wrap=el('div');
  wrap.append(el('h3',{},name));
  if(name.startsWith('hits-queries')){
    const classes=run.tenants.map(t=>t.class);
    const scen=[...new Set(run.tenants.flatMap(t=>t.scenarios.map(s=>s.name)))];
    const tbl=el('table'); const hr=el('tr'); hr.append(el('th',{},'scenario'));
    run.tenants.forEach(t=>hr.append(el('th',{},`${t.class} (fan-out ${t.fan_out})`)));
    tbl.append(hr);
    scen.forEach(s=>{const r=el('tr');r.append(el('td',{},s));
      run.tenants.forEach(t=>{const m=t.scenarios.find(x=>x.name==s);r.append(el('td',{},m?m.median_ms.toFixed(1):'—'));});
      tbl.append(r);});
    wrap.append(tbl);
    wrap.append(el('div',{cls:'sub'},`census rows: `+run.tenants.map(t=>`${t.class}=${t.census_rows}`).join('  ')));
  } else {
    const m=[['produce_rows_per_s','produce rows/s'],['ingest_catchup_secs','ingest catch-up s'],
      ['compaction_secs','compaction s'],['finalization_secs','finalization s'],['stored_rows','stored'],
      ['mapped','mapped'],['poison','poison'],['backpressure_deferrals','deferrals']];
    const tbl=el('table');
    m.forEach(([k,n])=>{if(run[k]==null)return;const r=el('tr');r.append(el('td',{},n));
      r.append(el('td',{},typeof run[k]=='number'&&!Number.isInteger(run[k])?run[k].toFixed(1):run[k]));tbl.append(r);});
    wrap.append(tbl);
    if(run.invariant_violations&&run.invariant_violations.length)
      wrap.append(el('div',{cls:'pos'},'VIOLATIONS: '+run.invariant_violations.join('; ')));
  }
  return wrap;
}

function renderAll(){
  const root=document.getElementById('runs');
  Object.entries(DATA).forEach(([n,r])=>root.append(tableFor(n,r)));
}

function fill(sel){Object.keys(DATA).forEach(n=>sel.append(el('option',{value:n},n)));}
function deltaTable(base,cand){
  const b=DATA[base],c=DATA[cand],out=el('div');
  if(!b||!c||base.split('-').slice(0,2).join('-')!=cand.split('-').slice(0,2).join('-')){
    out.append(el('div',{cls:'pos'},'pick two runs of the same kind (both hits-queries or both bsky-run)'));return out;}
  if(base.startsWith('hits-queries')){
    const scen=[...new Set(b.tenants.flatMap(t=>t.scenarios.map(s=>s.name)))];
    const tbl=el('table');const hr=el('tr');hr.append(el('th',{},'scenario'));
    b.tenants.forEach(t=>hr.append(el('th',{},t.class)));tbl.append(hr);
    const sc=r=>Object.fromEntries(r.tenants.map(t=>[t.class,Object.fromEntries(t.scenarios.map(s=>[s.name,s.median_ms]))]));
    const B=sc(b),C=sc(c);
    scen.forEach(s=>{const r=el('tr');r.append(el('td',{},s));
      b.tenants.forEach(t=>{const bv=B[t.class][s],cv=C[t.class]?.[s];const p=pct(bv,cv);
        r.append(el('td',{cls:p.cls},`${bv?.toFixed(1)}→${cv?.toFixed(1)} ${p.t}`));});
      tbl.append(r);});
    out.append(tbl);
  } else {
    const m=[['produce_rows_per_s','produce rows/s'],['ingest_catchup_secs','ingest catch-up s'],
      ['compaction_secs','compaction s'],['finalization_secs','finalization s'],
      ['backpressure_deferrals','deferrals']];
    const tbl=el('table');const hr=el('tr');['metric','baseline','candidate','Δ'].forEach(h=>hr.append(el('th',{},h)));tbl.append(hr);
    m.forEach(([k,n])=>{if(b[k]==null||c[k]==null)return;const p=pct(b[k],c[k]);const r=el('tr');
      r.append(el('td',{},n));r.append(el('td',{},b[k].toFixed?b[k].toFixed(1):b[k]));
      r.append(el('td',{},c[k].toFixed?c[k].toFixed(1):c[k]));r.append(el('td',{cls:p.cls},p.t));tbl.append(r);});
    out.append(tbl);
  }
  return out;
}
function refreshCmp(){const cmp=document.getElementById('cmp');cmp.innerHTML='';
  cmp.append(deltaTable(document.getElementById('base').value,document.getElementById('cand').value));}

fill(document.getElementById('base'));fill(document.getElementById('cand'));
document.getElementById('base').onchange=refreshCmp;document.getElementById('cand').onchange=refreshCmp;
if(Object.keys(DATA).length>=2){document.getElementById('cand').selectedIndex=1;}
refreshCmp();renderAll();
</script></body></html>"""


def cmd_list():
    runs = all_runs()
    for kind, labels in runs.items():
        print(f"{kind}: {', '.join(labels) if labels else '(none)'}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "compare" and len(sys.argv) == 4:
        cmd_compare(sys.argv[2], sys.argv[3])
    elif cmd == "html":
        cmd_html(sys.argv[2] if len(sys.argv) > 2 else os.path.join(RESULTS, "report.html"))
    elif cmd == "list":
        cmd_list()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
