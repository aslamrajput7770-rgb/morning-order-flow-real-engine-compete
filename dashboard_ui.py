"""The dashboard page: one self-contained HTML document.

No build step and no CDN. The page is served as a string and draws on canvas,
so it works on a locked-down box with no outbound network. It renders the
footprint grid, cumulative delta, delta bars, the volume profile and the
imbalance flags.

The browser never computes anything the engine already computed. It receives
numbers and draws them, so the page and the CLI cannot drift apart.
"""

from __future__ import annotations

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Order Flow Engine</title>
<style>
  :root{
    --bg:#0b0e13; --panel:#121722; --panel2:#161c28; --line:#232b3a;
    --txt:#c9d3e3; --dim:#7c8aa3; --buy:#2ecc71; --sell:#ff5252;
    --poc:#ffb020; --vah:#5b8def; --accent:#00d1ff;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--txt);
       font:12px/1.4 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
  header{display:flex;align-items:center;gap:14px;padding:8px 14px;
         background:var(--panel);border-bottom:1px solid var(--line);flex-wrap:wrap}
  h1{font-size:13px;margin:0;letter-spacing:.14em;color:var(--accent);
     text-transform:uppercase}
  .stat{color:var(--dim)}
  .stat b{color:var(--txt);font-weight:600}
  .dot{width:8px;height:8px;border-radius:50%;background:#555;display:inline-block;
       margin-right:5px;vertical-align:middle}
  .dot.on{background:var(--buy);box-shadow:0 0 8px var(--buy)}
  .dot.err{background:var(--sell);box-shadow:0 0 8px var(--sell)}
  button{background:var(--panel2);color:var(--txt);border:1px solid var(--line);
         padding:5px 11px;border-radius:4px;cursor:pointer;font:inherit}
  button:hover{border-color:var(--accent);color:var(--accent)}
  button.primary{background:#0d3b4d;border-color:#12657f;color:#9fe8ff}
  main{display:grid;grid-template-columns:1.15fr 1fr;gap:10px;padding:10px;
       align-items:start}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:6px;
         overflow:hidden}
  .panel h2{margin:0;padding:7px 10px;font-size:11px;letter-spacing:.12em;
            color:var(--dim);text-transform:uppercase;background:var(--panel2);
            border-bottom:1px solid var(--line);display:flex;
            justify-content:space-between}
  .scroll{max-height:74vh;overflow:auto}
  table{border-collapse:collapse;width:100%}
  th,td{padding:2px 8px;text-align:right;white-space:nowrap}
  th{position:sticky;top:0;background:var(--panel2);color:var(--dim);
     font-weight:500;z-index:2}
  td.px{color:var(--txt);font-weight:600}
  td.bid{color:var(--sell)}
  td.ask{color:var(--buy)}
  tr.poc{outline:1px solid var(--poc)}
  tr.poc td.px::before{content:"POC ";color:var(--poc);font-size:10px}
  tr.va td.px{background:rgba(91,141,239,.10)}
  tr.imb-buy td.ask::after{content:" ▸";color:var(--buy)}
  tr.imb-sell td.bid::after{content:" ◂";color:var(--sell)}
  .tabs{display:flex;gap:6px;padding:8px;flex-wrap:wrap}
  .tab{padding:4px 10px;border:1px solid var(--line);border-radius:14px;
       cursor:pointer;color:var(--dim)}
  .tab.sel{border-color:var(--accent);color:var(--accent);background:#0d2531}
  canvas{display:block;width:100%}
  .kv{display:grid;grid-template-columns:auto auto;gap:2px 14px;padding:8px 10px}
  .kv div:nth-child(odd){color:var(--dim)}
  .err{color:var(--sell);padding:8px 10px}
  .hint{color:var(--dim);padding:10px;line-height:1.6}
  .dbar{height:9px;background:#1a2130;border-radius:2px;overflow:hidden}
  .dbar i{display:block;height:100%}
</style>
</head>
<body>
<header>
  <h1>Order Flow Engine</h1>
  <span class="stat"><span id="dot" class="dot"></span><b id="status">idle</b></span>
  <span class="stat">ticks <b id="ticks">0</b></span>
  <span class="stat">trades <b id="trades">0</b></span>
  <span class="stat">src <b id="src">-</b></span>
  <span class="stat">last tick <b id="last">-</b></span>
  <span style="margin-left:auto"></span>
  <button id="btnStart" class="primary">Start live</button>
  <button id="btnReplay">Replay capture</button>
  <button id="btnStop">Stop</button>
</header>

<main>
  <section class="panel">
    <h2><span>Volumetric / Footprint bars</span><span id="chartMeta" class="stat"></span></h2>
    <div class="tabs" id="tabs"></div>
    <div id="err" class="err" style="display:none"></div>
    <div class="scroll" id="fp"></div>
  </section>

  <div style="display:flex;flex-direction:column;gap:10px">
    <section class="panel">
      <h2><span>Cumulative delta</span><span id="cumMeta" class="stat"></span></h2>
      <canvas id="cum" height="150"></canvas>
    </section>
    <section class="panel">
      <h2><span>Delta bars</span></h2>
      <canvas id="dbars" height="110"></canvas>
    </section>
    <section class="panel">
      <h2><span>Volume profile</span><span id="profMeta" class="stat"></span></h2>
      <div class="scroll" style="max-height:36vh"><table id="prof"></table></div>
    </section>
    <section class="panel">
      <h2><span>Session</span></h2>
      <div class="kv" id="session"></div>
    </section>
  </div>
</main>

<script>
let SEL = null;
const $ = id => document.getElementById(id);
const fmt = (v,d=2) => v===null||v===undefined ? "-" : Number(v).toFixed(d);
const int = v => v===null||v===undefined ? "-" : Math.round(v).toLocaleString();

function heat(v, vmax){
  if(!vmax) return "transparent";
  const t = Math.min(1, Math.abs(v)/vmax);
  return `rgba(0,209,255,${(t*0.30).toFixed(3)})`;
}

function renderTabs(watchlist){
  const el = $("tabs"); el.innerHTML = "";
  if(!SEL && watchlist.length) SEL = watchlist[0].key;
  watchlist.forEach(w => {
    const d = document.createElement("div");
    d.className = "tab" + (w.key===SEL ? " sel" : "");
    d.textContent = `${w.label} · ${w.market} · ${fmt(w.tick)}`;
    d.onclick = () => { SEL = w.key; };
    el.appendChild(d);
  });
}

function renderFootprint(chart){
  const bars = chart.bars || [];
  $("chartMeta").textContent =
    `${chart.symbol||""} · ${bars.length} bars · ${chart.bar_seconds||0}s`;
  if(!bars.length){
    $("fp").innerHTML = `<div class="hint">No bars yet. Start the feed, or
      replay a capture. Bars appear once trades land inside a session.</div>`;
    return;
  }
  const bar = bars[bars.length-1];
  const va = chart.profile;
  const imb = {};
  (bar.imbalances||[]).forEach(i => { imb[i.price] = i; });
  const vmax = Math.max(...bar.levels.map(l=>l.total), 1);

  let h = `<table><thead><tr><th>price</th><th>bid</th><th>ask</th>
    <th>total</th><th>delta</th><th>bar</th></tr></thead><tbody>`;
  bar.levels.slice().reverse().forEach(l => {
    const cls = [];
    if(bar.poc!==null && Math.abs(l.price-bar.poc)<1e-9) cls.push("poc");
    if(va && va.vah!==null && l.price<=va.vah && l.price>=va.val) cls.push("va");
    if(imb[l.price]) cls.push(imb[l.price].side==="BUY" ? "imb-buy" : "imb-sell");
    const w = 100*Math.abs(l.delta)/vmax;
    const col = l.delta>=0 ? "var(--buy)" : "var(--sell)";
    h += `<tr class="${cls.join(" ")}" style="background:${heat(l.total,vmax)}">
      <td class="px">${fmt(l.price)}</td>
      <td class="bid">${int(l.bid)}</td>
      <td class="ask">${int(l.ask)}</td>
      <td>${int(l.total)}</td>
      <td style="color:${col}">${int(l.delta)}</td>
      <td><div class="dbar" style="width:70px;display:inline-block">
        <i style="width:${w}%;background:${col}"></i></div></td>
    </tr>`;
  });
  h += `</tbody></table>`;
  $("fp").innerHTML = h;
}

function drawLine(canvas, series, color){
  const c = canvas.getContext("2d");
  const W = canvas.width = canvas.clientWidth;
  const H = canvas.height;
  c.clearRect(0,0,W,H);
  if(series.length < 2) return;
  const vals = series.map(p=>p.d);
  const lo = Math.min(0, ...vals), hi = Math.max(0, ...vals);
  const span = (hi-lo) || 1;
  const x = i => 4 + (W-8)*i/(series.length-1);
  const y = v => H-6 - (H-12)*(v-lo)/span;
  c.strokeStyle = "#232b3a"; c.beginPath();
  c.moveTo(0, y(0)); c.lineTo(W, y(0)); c.stroke();
  c.strokeStyle = color; c.lineWidth = 1.5; c.beginPath();
  series.forEach((p,i) => i ? c.lineTo(x(i), y(p.d)) : c.moveTo(x(i), y(p.d)));
  c.stroke();
  c.fillStyle = "rgba(0,209,255,.10)";
  c.lineTo(x(series.length-1), y(0)); c.lineTo(x(0), y(0)); c.closePath(); c.fill();
}

function drawDeltaBars(canvas, bars){
  const c = canvas.getContext("2d");
  const W = canvas.width = canvas.clientWidth;
  const H = canvas.height;
  c.clearRect(0,0,W,H);
  if(!bars.length) return;
  const vals = bars.map(b=>b.delta);
  const mx = Math.max(1, ...vals.map(Math.abs));
  const bw = Math.max(2, W/bars.length - 1);
  const mid = H/2;
  c.strokeStyle = "#232b3a"; c.beginPath();
  c.moveTo(0,mid); c.lineTo(W,mid); c.stroke();
  bars.forEach((b,i) => {
    const h = (H/2-4)*Math.abs(b.delta)/mx;
    c.fillStyle = b.delta>=0 ? "#2ecc71" : "#ff5252";
    c.fillRect(i*(W/bars.length), b.delta>=0 ? mid-h : mid, bw, h);
  });
}

function renderProfile(chart){
  const p = chart.profile;
  if(!p || !p.levels || !p.levels.length){
    $("prof").innerHTML = `<tr><td class="hint">No profile yet</td></tr>`;
    $("profMeta").textContent = ""; return;
  }
  $("profMeta").textContent = `POC ${fmt(p.poc)} · VAH ${fmt(p.vah)} · VAL ${fmt(p.val)}`;
  const vmax = Math.max(...p.levels.map(l=>l.volume), 1);
  let h = `<thead><tr><th>price</th><th>volume</th></tr></thead><tbody>`;
  p.levels.slice().reverse().forEach(l => {
    const w = 100*l.volume/vmax;
    const col = (Math.abs(l.price-p.poc)<1e-9) ? "var(--poc)"
              : (l.price<=p.vah && l.price>=p.val) ? "var(--vah)" : "#3a4658";
    h += `<tr><td class="px">${fmt(l.price)}</td>
      <td><div class="dbar" style="width:90px;display:inline-block">
      <i style="width:${w}%;background:${col}"></i></div> ${int(l.volume)}</td></tr>`;
  });
  $("prof").innerHTML = h + `</tbody>`;
}

function renderSession(chart){
  const bars = chart.bars||[];
  const last = bars[bars.length-1];
  const cum = (chart.cumulative_delta||[]);
  const cd = cum.length ? cum[cum.length-1].d : 0;
  const rows = [
    ["symbol", chart.symbol||chart.label||"-"],
    ["session", chart.session||"-"],
    ["bar size", (chart.bar_seconds||0)+"s"],
    ["tick", fmt(chart.tick)],
    ["last", last?fmt(last.close):"-"],
    ["bar volume", last?int(last.volume):"-"],
    ["bar delta", last?int(last.delta):"-"],
    ["cum delta", int(cd)],
    ["bars", bars.length],
  ];
  $("session").innerHTML = rows.map(([k,v])=>
    `<div>${k}</div><div>${v}</div>`).join("");
}

let WATCH = [];
async function tick(){
  try{
    const r = await fetch("/api/status"); const s = await r.json();
    WATCH = s.watchlist||[];
    renderTabs(WATCH);
    $("ticks").textContent = s.ticks;
    $("trades").textContent = s.trades;
    $("src").textContent = s.source;
    $("last").textContent = s.last_tick_at
      ? new Date(s.last_tick_at*1000).toLocaleTimeString() : "-";
    $("status").textContent = s.status;
    const dot = $("dot");
    dot.className = "dot" + (s.status==="streaming" ? " on"
                  : s.status==="error" ? " err" : "");
    if(s.last_error){ $("err").style.display="block"; $("err").textContent = s.last_error; }
    else $("err").style.display="none";

    if(SEL){
      const cr = await fetch(`/api/chart/${encodeURIComponent(SEL)}`);
      const ch = await cr.json();
      renderFootprint(ch);
      drawLine($("cum"), ch.cumulative_delta||[], "#00d1ff");
      drawDeltaBars($("dbars"), (ch.bars||[]).slice(-60));
      renderProfile(ch);
      renderSession(ch);
    }
  }catch(e){ /* keep polling */ }
}
setInterval(tick, 900); tick();

async function post(url, body){
  const r = await fetch(url, {method:"POST",
    headers:{"Content-Type":"application/json"},
    body: JSON.stringify(body||{})});
  return r.json();
}
$("btnStart").onclick = async () => {
  const r = await post("/api/feed/start", {mode:"live"});
  if(r.detail) alert(r.detail);
};
$("btnReplay").onclick = async () => {
  const path = prompt("Capture file path (from live_feed.py --record):",
                      "/tmp/cap_live.txt");
  if(!path) return;
  const r = await post("/api/feed/start", {mode:"replay", path});
  if(r.detail) alert(r.detail);
};
$("btnStop").onclick = () => post("/api/feed/stop", {});
</script>
</body>
</html>
"""
