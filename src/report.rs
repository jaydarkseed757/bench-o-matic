//! HTML dashboard report generator.
//!
//! Produces a single self-contained .html file with Chart.js charts embedded
//! via CDN.  All benchmark data is inlined as a JSON object so the file can
//! be opened from any location without a web server.

use std::fmt::Write as FmtWrite;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::Config;
use crate::metrics::PhaseResult;

const PALETTE: &[&str] = &[
    "#3b82f6", "#10b981", "#f59e0b", "#ef4444",
    "#8b5cf6", "#06b6d4", "#f97316", "#84cc16",
];
const PALETTE_BG: &[&str] = &[
    "rgba(59,130,246,0.25)",  "rgba(16,185,129,0.25)",
    "rgba(245,158,11,0.25)",  "rgba(239,68,68,0.25)",
    "rgba(139,92,246,0.25)",  "rgba(6,182,212,0.25)",
    "rgba(249,115,22,0.25)",  "rgba(132,204,22,0.25)",
];

// ── Public entry point ────────────────────────────────────────────────────────

pub fn generate(cfg: &Config, results: &[(String, PhaseResult)]) -> std::io::Result<()> {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let filename = format!("bench-report-{ts_ms}.html");
    std::fs::write(&filename, build(cfg, results, ts_ms))?;
    println!("  Report saved → ./{filename}");
    Ok(())
}

// ── HTML builder ──────────────────────────────────────────────────────────────

fn build(cfg: &Config, results: &[(String, PhaseResult)], ts_ms: u128) -> String {
    let mut out = String::with_capacity(128 * 1024);
    push_head(&mut out);
    push_body(&mut out, cfg, results, ts_ms);
    out.push_str("</body></html>");
    out
}

// ── <head> ────────────────────────────────────────────────────────────────────

fn push_head(out: &mut String) {
    out.push_str(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>bench-o-matic Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
header{background:#1e293b;padding:1.75rem 2rem;border-bottom:1px solid #334155;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:1rem}
.logo{font-size:1.5rem;font-weight:800;color:#f8fafc;letter-spacing:-0.02em}
.logo span{color:#3b82f6}
.run-meta{text-align:right;color:#64748b;font-size:0.8rem;line-height:1.6}
main{max-width:1400px;margin:0 auto;padding:2rem;display:flex;flex-direction:column;gap:2rem}
.section-label{font-size:0.7rem;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;color:#475569;margin-bottom:0.75rem}
/* Config strip */
.config-strip{display:flex;flex-wrap:wrap;gap:0.75rem}
.cfg{background:#1e293b;border-radius:8px;padding:0.75rem 1.1rem;min-width:140px}
.cfg-k{font-size:0.68rem;text-transform:uppercase;letter-spacing:0.06em;color:#475569}
.cfg-v{font-size:0.95rem;font-weight:600;color:#f1f5f9;margin-top:0.15rem}
/* Cards */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:1rem}
.card{background:#1e293b;border-radius:10px;padding:1.25rem;border-top:3px solid transparent}
.card-name{font-size:0.82rem;font-weight:600;color:#94a3b8;margin-bottom:0.9rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card-stats{display:grid;grid-template-columns:1fr 1fr 1fr;gap:0.5rem}
.stat-val{font-size:1.25rem;font-weight:700;color:#f8fafc;line-height:1}
.stat-unit{font-size:0.65rem;text-transform:uppercase;letter-spacing:0.05em;color:#475569;margin-top:0.2rem}
/* Chart grid */
.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:1.25rem}
@media(max-width:900px){.chart-grid{grid-template-columns:1fr}}
.chart-box{background:#1e293b;border-radius:10px;padding:1.25rem}
.chart-title{font-size:0.75rem;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;color:#475569;margin-bottom:1rem}
.chart-wrap{position:relative;height:240px}
/* Table */
.table-box{background:#1e293b;border-radius:10px;padding:1.5rem;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.82rem;white-space:nowrap}
th{text-align:left;padding:0.4rem 0.75rem;font-size:0.65rem;text-transform:uppercase;letter-spacing:0.06em;color:#475569;border-bottom:1px solid #334155;font-weight:600}
td{padding:0.55rem 0.75rem;border-bottom:1px solid rgba(51,65,85,0.5);color:#cbd5e1}
tr:last-child td{border-bottom:none}
tbody tr:hover td{background:rgba(51,65,85,0.3)}
.td-name{font-weight:600;color:#e2e8f0}
.td-num{text-align:right;font-variant-numeric:tabular-nums}
footer{text-align:center;padding:2rem;color:#334155;font-size:0.75rem}
</style>
</head>
<body>
"#);
}

// ── <body> ────────────────────────────────────────────────────────────────────

fn push_body(out: &mut String, cfg: &Config, results: &[(String, PhaseResult)], ts_ms: u128) {
    let n = results.len();

    // ── Header ────────────────────────────────────────────────────────────────
    out.push_str(r#"<header>
  <div class="logo">bench<span>-o-</span>matic</div>
  <div class="run-meta">
    <div id="ts-display"></div>
    <div>Disk I/O Benchmark Report</div>
  </div>
</header>
<main>
"#);

    // ── Config strip ──────────────────────────────────────────────────────────
    out.push_str(r#"<div><div class="section-label">Run Configuration</div><div class="config-strip">"#);
    let file_size_mb = cfg.file_size as f64 / (1024.0 * 1024.0);
    let block_size_kb = cfg.block_size as f64 / 1024.0;
    let mode_str = if let Some(pat) = cfg.pattern {
        format!("{pat:?}")
    } else {
        format!("{}  /  {}", cfg.workload.as_str(), cfg.mode.as_str())
    };
    let limit_str = match cfg.duration {
        Some(d) => format!("{:.0}s", d.as_secs_f64()),
        None => format!("{} ops", cfg.num_ops),
    };
    push_cfg(out, "Directory", &cfg.dir.display().to_string());
    push_cfg(out, "File Size", &format!("{file_size_mb:.0} MB"));
    push_cfg(out, "Block Size", &format!("{block_size_kb:.0} KB"));
    push_cfg(out, "Mode", &mode_str);
    push_cfg(out, "Limit", &limit_str);
    push_cfg(out, "Threads", &cfg.threads.to_string());
    out.push_str("</div></div>\n");

    // ── Summary cards ─────────────────────────────────────────────────────────
    out.push_str(r#"<div><div class="section-label">Phase Summary</div><div class="cards">"#);
    for (i, (name, r)) in results.iter().enumerate() {
        let color = PALETTE[i % PALETTE.len()];
        write!(out,
            r#"<div class="card" style="border-top-color:{color}">
  <div class="card-name">{name}</div>
  <div class="card-stats">
    <div><div class="stat-val">{:.1}</div><div class="stat-unit">MB/s</div></div>
    <div><div class="stat-val">{:.0}</div><div class="stat-unit">IOPS</div></div>
    <div><div class="stat-val">{:.2}</div><div class="stat-unit">p99 ms</div></div>
  </div>
</div>"#,
            r.throughput_mb_s, r.iops, r.latency_ms.p99
        ).unwrap();
    }
    out.push_str("</div></div>\n");

    // ── Charts ────────────────────────────────────────────────────────────────
    out.push_str(r#"<div><div class="section-label">Charts</div><div class="chart-grid">
  <div class="chart-box"><div class="chart-title">Throughput (MB/s)</div><div class="chart-wrap"><canvas id="cThroughput"></canvas></div></div>
  <div class="chart-box"><div class="chart-title">IOPS</div><div class="chart-wrap"><canvas id="cIops"></canvas></div></div>
  <div class="chart-box"><div class="chart-title">Latency Percentiles (ms)</div><div class="chart-wrap"><canvas id="cLatPct"></canvas></div></div>
  <div class="chart-box"><div class="chart-title">Latency Distribution (ms)</div><div class="chart-wrap"><canvas id="cLatHist"></canvas></div></div>
</div></div>
"#);

    // ── Full stats table ──────────────────────────────────────────────────────
    out.push_str(r#"<div><div class="section-label">Full Results</div><div class="table-box">
<table>
<thead><tr>
  <th>Phase</th><th class="td-num">Ops</th><th class="td-num">MB</th>
  <th class="td-num">MB/s</th><th class="td-num">IOPS</th>
  <th class="td-num">avg ms</th><th class="td-num">min ms</th>
  <th class="td-num">p50 ms</th><th class="td-num">p95 ms</th>
  <th class="td-num">p99 ms</th><th class="td-num">max ms</th><th class="td-num">Errors</th>
</tr></thead>
<tbody>"#);
    for (name, r) in results {
        let lat = &r.latency_ms;
        write!(out,
            r#"<tr>
  <td class="td-name">{name}</td>
  <td class="td-num">{}</td><td class="td-num">{:.2}</td>
  <td class="td-num">{:.2}</td><td class="td-num">{:.1}</td>
  <td class="td-num">{:.3}</td><td class="td-num">{:.3}</td>
  <td class="td-num">{:.3}</td><td class="td-num">{:.3}</td>
  <td class="td-num">{:.3}</td><td class="td-num">{:.3}</td>
  <td class="td-num">{}</td>
</tr>"#,
            r.operations, r.total_mb,
            r.throughput_mb_s, r.iops,
            lat.avg, lat.min, lat.p50, lat.p95, lat.p99, lat.max,
            r.errors
        ).unwrap();
    }
    out.push_str("</tbody></table></div></div>\n");

    // ── Footer ────────────────────────────────────────────────────────────────
    out.push_str(r#"</main>
<footer>bench-o-matic &mdash; disk I/O benchmark</footer>
"#);

    // ── Inline data + Chart.js init ───────────────────────────────────────────
    out.push_str("<script>\n");

    // Timestamp display
    write!(out, "document.getElementById('ts-display').textContent = new Date({ts_ms}).toLocaleString();\n").unwrap();

    // Data arrays
    let labels   = js_str_arr(results.iter().map(|(k, _)| k.as_str()));
    let colors   = js_str_arr((0..n).map(|i| PALETTE[i % PALETTE.len()]));
    let bg_colors = js_str_arr((0..n).map(|i| PALETTE_BG[i % PALETTE_BG.len()]));
    let tp       = js_f64_arr(results.iter().map(|(_, r)| r.throughput_mb_s));
    let iops     = js_f64_arr(results.iter().map(|(_, r)| r.iops));
    let p50      = js_f64_arr(results.iter().map(|(_, r)| r.latency_ms.p50));
    let p95      = js_f64_arr(results.iter().map(|(_, r)| r.latency_ms.p95));
    let p99      = js_f64_arr(results.iter().map(|(_, r)| r.latency_ms.p99));

    write!(out,
        "const L={labels};\nconst C={colors};\nconst CB={bg_colors};\n\
         const TP={tp};\nconst IO={iops};\n\
         const P50={p50};\nconst P95={p95};\nconst P99={p99};\n"
    ).unwrap();

    // Histogram data (shared x-axis across all phases)
    let (hist_labels, hist_datasets) = histogram_data(results, 24);
    write!(out, "const HL={};\n", js_str_arr(hist_labels.iter().map(|s| s.as_str()))).unwrap();
    out.push_str("const HD=[\n");
    for (i, counts) in hist_datasets.iter().enumerate() {
        let color = PALETTE[i % PALETTE.len()];
        let bg    = PALETTE_BG[i % PALETTE_BG.len()];
        let name  = &results[i].0;
        let data  = js_usize_arr(counts.iter().copied());
        write!(out,
            "  {{label:{},data:{data},borderColor:'{color}',backgroundColor:'{bg}',fill:true,tension:0.3,pointRadius:0,borderWidth:1.5}},\n",
            js_str(name)
        ).unwrap();
    }
    out.push_str("];\n");

    // Chart defaults helper
    out.push_str(r#"
function gridOpts(horiz) {
  const base = {
    plugins: { legend: { labels: { color:'#64748b', font:{size:11} } } },
    scales: {
      x: { ticks:{color:'#475569'}, grid:{color:'#334155'} },
      y: { ticks:{color:'#475569'}, grid:{color:'#334155'} }
    },
    responsive: true, maintainAspectRatio: false,
  };
  if (horiz) { base.indexAxis = 'y'; base.plugins.legend = {display:false}; }
  return base;
}

// Throughput
new Chart(document.getElementById('cThroughput'), {
  type:'bar',
  data:{ labels:L, datasets:[{label:'MB/s', data:TP, backgroundColor:C}] },
  options: gridOpts(true),
});

// IOPS
new Chart(document.getElementById('cIops'), {
  type:'bar',
  data:{ labels:L, datasets:[{label:'IOPS', data:IO, backgroundColor:C}] },
  options: gridOpts(true),
});

// Latency percentiles — grouped bar
new Chart(document.getElementById('cLatPct'), {
  type:'bar',
  data:{
    labels:L,
    datasets:[
      {label:'p50', data:P50, backgroundColor:'rgba(59,130,246,0.7)'},
      {label:'p95', data:P95, backgroundColor:'rgba(245,158,11,0.7)'},
      {label:'p99', data:P99, backgroundColor:'rgba(239,68,68,0.7)'},
    ]
  },
  options:{
    responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{ labels:{color:'#64748b',font:{size:11}} } },
    scales:{
      x:{ticks:{color:'#475569'},grid:{color:'#334155'}},
      y:{ticks:{color:'#475569'},grid:{color:'#334155'},title:{display:true,text:'ms',color:'#475569',font:{size:10}}},
    }
  }
});

// Latency distribution — area chart
new Chart(document.getElementById('cLatHist'), {
  type:'line',
  data:{ labels:HL, datasets:HD },
  options:{
    responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{ labels:{color:'#64748b',font:{size:11}} } },
    scales:{
      x:{ticks:{color:'#475569',maxTicksLimit:8},grid:{color:'#334155'},
         title:{display:true,text:'latency (ms)',color:'#475569',font:{size:10}}},
      y:{ticks:{color:'#475569'},grid:{color:'#334155'},
         title:{display:true,text:'count',color:'#475569',font:{size:10}}},
    }
  }
});
"#);

    out.push_str("</script>\n");
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn push_cfg(out: &mut String, key: &str, val: &str) {
    write!(out,
        r#"<div class="cfg"><div class="cfg-k">{key}</div><div class="cfg-v">{val}</div></div>"#
    ).unwrap();
}

fn js_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "\\'"))
}

fn js_str_arr<'a>(it: impl Iterator<Item = &'a str>) -> String {
    let inner: Vec<String> = it.map(|s| js_str(s)).collect();
    format!("[{}]", inner.join(","))
}

fn js_f64_arr(it: impl Iterator<Item = f64>) -> String {
    let inner: Vec<String> = it.map(|v| format!("{v:.4}")).collect();
    format!("[{}]", inner.join(","))
}

fn js_usize_arr(it: impl Iterator<Item = usize>) -> String {
    let inner: Vec<String> = it.map(|v| v.to_string()).collect();
    format!("[{}]", inner.join(","))
}

/// Compute a histogram over the raw latencies for all phases.
///
/// Returns `(bin_labels, per_phase_counts)` where all phases share the same
/// x-axis (global min → global max, `bins` buckets).
fn histogram_data(results: &[(String, PhaseResult)], bins: usize) -> (Vec<String>, Vec<Vec<usize>>) {
    let global_min = results.iter()
        .filter_map(|(_, r)| r.raw_latencies.first().copied())
        .fold(f64::INFINITY, f64::min);
    let global_max = results.iter()
        .filter_map(|(_, r)| r.raw_latencies.last().copied())
        .fold(f64::NEG_INFINITY, f64::max);

    if !global_min.is_finite() || !global_max.is_finite() || (global_max - global_min) < 1e-9 {
        return (vec![], vec![]);
    }

    let width = (global_max - global_min) / bins as f64;

    let labels: Vec<String> = (0..bins)
        .map(|i| format!("{:.3}", global_min + i as f64 * width))
        .collect();

    let datasets: Vec<Vec<usize>> = results.iter().map(|(_, r)| {
        let mut counts = vec![0usize; bins];
        for &v in &r.raw_latencies {
            let idx = ((v - global_min) / width).floor() as usize;
            counts[idx.min(bins - 1)] += 1;
        }
        counts
    }).collect();

    (labels, datasets)
}
