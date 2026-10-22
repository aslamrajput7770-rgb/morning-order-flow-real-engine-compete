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
  .pill{padding:2px 8px;border-radius:10px;border:1px solid var(--line);
        font-size:11px;color:var(--dim)}
  .pill.on{border-color:#12657f;color:#9fe8ff;background:#0d2531}
  .pill.off{border-color:#4a2530;color:#ff9a9a;background:#2a1418}
  .pos{color:var(--buy)} .neg{color:var(--sell)}
  details summary{cursor:pointer;color:var(--dim);padding:2px 0}
  input[type=text]{background:var(--panel2);border:1px solid var(--line);
        color:var(--txt);padding:4px 7px;border-radius:4px;font:inherit;width:230px}
  /* The four numbers a trader checks first, on one line, above everything.
     Equity and costs stay in the paper panel: the strip is a glance, not a
     report, and crowding it defeats the point of having it. */
  .strip{display:flex;align-items:baseline;gap:26px;padding:10px 14px;
         background:var(--panel2);border-bottom:1px solid var(--line);
         flex-wrap:wrap}
  .strip .cell{display:flex;align-items:baseline;gap:8px}
  .strip .k{color:var(--dim);font-size:10px;letter-spacing:.16em;
            text-transform:uppercase}
  .strip .v{font-size:24px;font-weight:700;letter-spacing:.02em;line-height:1}
  .strip .v.pos{color:var(--buy)} .strip .v.neg{color:var(--sell)}
  .strip .v.dim{color:var(--dim);font-weight:600}
  .strip .stale{margin-left:auto;color:var(--dim);font-size:11px;
                border:1px solid var(--line);border-radius:10px;padding:2px 9px}
  .strip .stale.warn{border-color:var(--poc);color:var(--poc)}
  /* One column below tablet width. The two-column grid squeezed both columns on
     a phone and clipped table cells; the desk reads this on a laptop, but a
     clipped number is worse than a stacked one. */
  @media (max-width:900px){
    main{grid-template-columns:1fr}
    .strip .v{font-size:19px}
    .strip{gap:16px}
    .scroll{max-height:none}
  }
</style>
</head>
<body>
<header>
  <h1>Order Flow Engine</h1>
  <span class="stat"><span id="dot" class="dot"></span><b id="status">starting</b></span>
  <span class="stat">ticks <b id="ticks">0</b></span>
  <span class="stat">trades <b id="trades">0</b></span>
  <span class="stat">src <b id="src">-</b></span>
  <span class="stat">last tick <b id="last">-</b></span>
  <span class="stat">feed <b id="feedState">-</b></span>
  <span class="stat">paper <span id="paperPill" class="pill off">off</span></span>
  <span style="margin-left:auto"></span>
  <label class="stat"><input type="checkbox" id="paperToggle" checked> auto-trade</label>
  <button id="btnPause">Pause feed</button>
</header>

<div class="strip">
  <div class="cell"><span class="k">Net PnL</span><span class="v dim" id="sNet">-</span></div>
  <div class="cell"><span class="k">Hold</span><span class="v" id="sHold">-</span></div>
  <div class="cell"><span class="k">Exits</span><span class="v" id="sExits">-</span></div>
  <div class="cell"><span class="k">Live</span><span class="v dim" id="sLive">-</span></div>
  <span class="stale" id="sStale" style="display:none"></span>
</div>

<main>
  <section class="panel">
    <h2><span>Volumetric / Footprint bars</span><span id="chartMeta" class="stat"></span></h2>
    <div class="tabs" id="tabs"></div>
    <div id="err" class="err" style="display:none"></div>
    <div class="scroll" id="fp"></div>
    <details style="padding:6px 10px">
      <summary>validate on a capture (backtest)</summary>
      <div style="display:flex;gap:6px;padding:6px 0">
        <input type="text" id="replayPath" value="recordings" placeholder="capture dir or .oft.gz file">
        <button id="btnReplay">Replay</button>
      </div>
    </details>
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
    <section class="panel">
      <h2><span>Paper account</span><span id="paperMeta" class="stat"></span></h2>
      <div class="kv" id="paper"></div>
    </section>
    <section class="panel">
      <h2><span>Open positions</span></h2>
      <div class="scroll" style="max-height:22vh"><table id="positions"></table></div>
    </section>
    <section class="panel">
      <h2><span>Closed trades</span></h2>
      <div class="scroll" style="max-height:22vh"><table id="trades-log"></table></div>
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
  $("chartMeta").textContent = bars.length
    ? `${chart.symbol||""} · ${bars.length} bars · ${chart.bar_seconds||0}s`
    : (chart.symbol ? `${chart.symbol} · no bars` : "");
  if(!bars.length){
    $("fp").innerHTML = `<div class="hint">No bars yet. The feed comes up on
      its own; bars appear once trades land inside a session. To check the
      engine off-session, replay a capture below.</div>`;
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
  const cd = cum.length ? cum[cum.length-1].d : null;
  // No chart means no data, and the panel must say so. A zero bar size and a
  // zero cumulative delta look like a broken feed; a dash reads as "nothing
  // here yet", which is the truth.
  const has = bars.length > 0;
  const rows = [
    ["symbol", chart.symbol || chart.label || "-"],
    ["session", chart.session || "-"],
    ["bar size", has ? (chart.bar_seconds||0)+"s" : "-"],
    ["tick", has ? fmt(chart.tick) : "-"],
    ["last", last?fmt(last.close):"-"],
    ["bar volume", last?int(last.volume):"-"],
    ["bar delta", last?int(last.delta):"-"],
    ["cum delta", cd===null?"-":int(cd)],
    ["bars", bars.length],
  ];
  $("session").innerHTML = rows.map(([k,v])=>
    `<div>${k}</div><div>${v}</div>`).join("");
}

function setStrip(id, text, cls){
  const el = $(id);
  el.textContent = text;
  el.className = "v" + (cls ? " " + cls : "");
}

function renderStrip(s, stale){
  const p = (stale && s.last_session) ? s.last_session.paper : s.paper;
  const acct = p || {};
  // No account means no numbers, not zero numbers. A run with paper trading off
  // has no PnL and no exits, and showing "0" would read as a flat session.
  const hasAcct = p !== null && p !== undefined;
  const net = hasAcct ? acct.net_pnl : null;
  setStrip("sNet", net===null||net===undefined ? "-" : int(net),
           net===null||net===undefined ? "dim" : (net>=0 ? "pos" : "neg"));
  setStrip("sHold", hasAcct ? int((acct.positions||[]).length) : "-",
           hasAcct ? "" : "dim");
  setStrip("sExits", hasAcct ? int(acct.exits) : "-", hasAcct ? "" : "dim");
  const live = s.running
    ? (s.feed && s.feed.state ? s.feed.state : s.status)
    : (s.live_configured ? "paused" : "no session");
  setStrip("sLive", live, s.running ? "pos" : "dim");

  const pill = $("sStale");
  if(stale && s.last_session && s.last_session.saved_at){
    const when = new Date(s.last_session.saved_at*1000).toLocaleString();
    pill.textContent = `showing last session · ${when}`;
    pill.className = "stale warn";
    pill.style.display = "";
  } else {
    pill.style.display = "none";
  }
}

function renderPaper(p){
  const acct = p || {};
  const cls = v => v===null||v===undefined ? "" : (v>=0 ? "pos" : "neg");
  const rows = [
    ["equity", int(acct.equity), ""],
    ["net PnL", int(acct.net_pnl), cls(acct.net_pnl)],
    ["realized", int(acct.realized_pnl), cls(acct.realized_pnl)],
    ["unrealized", int(acct.unrealized_pnl), cls(acct.unrealized_pnl)],
    ["costs", int(acct.costs), "neg"],
    ["entries", int(acct.entries), ""],
    ["exits", int(acct.exits), ""],
  ];
  $("paper").innerHTML = rows.map(([k,v,c])=>
    `<div>${k}</div><div class="${c}">${v}</div>`).join("");

  const pos = acct.positions || [];
  const head = "<tr><th>symbol</th><th>qty</th><th>avg</th><th>last</th>"
             + "<th>stop</th><th>target</th><th>uPnL</th></tr>";
  $("positions").innerHTML = head + (pos.length ? pos.map(p=>
    `<tr><td class="px">${p.symbol}</td><td>${int(p.qty)}</td>`
    + `<td>${fmt(p.avg_price)}</td><td>${fmt(p.last_price)}</td>`
    + `<td>${fmt(p.stop)}</td><td>${fmt(p.target)}</td>`
    + `<td class="${cls(p.unrealized)}">${int(p.unrealized)}</td></tr>`
  ).join("") : '<tr><td colspan="7" style="text-align:center;color:var(--dim)">'
      + 'no open positions</td></tr>');

  const rt = (acct.round_trips || []).slice().reverse();
  const rhead = "<tr><th>symbol</th><th>side</th><th>qty</th><th>entry</th>"
              + "<th>exit</th><th>gross</th><th>costs</th><th>net</th></tr>";
  $("trades-log").innerHTML = rhead + (rt.length ? rt.map(r=>
    `<tr><td class="px">${r.symbol}</td>`
    + `<td class="${r.side==="BUY"?"ask":"bid"}">${r.side}</td>`
    + `<td>${int(r.qty)}</td><td>${fmt(r.entry_price)}</td>`
    + `<td>${fmt(r.exit_price)}</td>`
    + `<td class="${cls(r.gross)}">${int(r.gross)}</td>`
    + `<td class="neg">${int(r.costs)}</td>`
    + `<td class="${cls(r.net)}">${int(r.net)}</td></tr>`
  ).join("") : '<tr><td colspan="8" style="text-align:center;color:var(--dim)">'
      + 'no closed trades yet</td></tr>');
}

let WATCH = [];
let STALE = false;
let RUNNING = false;
async function tick(){
  try{
    const r = await fetch("/api/status"); const s = await r.json();
    WATCH = s.watchlist||[];
    RUNNING = !!s.running;
    // The one feed button says what it will do, so a stray click is never a
    // surprise: "Pause feed" while streaming, "Start live" while stopped.
    $("btnPause").textContent = s.running ? "Pause feed" : "Start live";
    renderTabs(WATCH);
    // Nothing running and nothing received this process: whatever the page
    // shows can only come from the last saved session. This is what stops a
    // fresh boot from reading as a dead engine.
    STALE = !s.running && !s.ticks && !!s.last_session;
    $("ticks").textContent = s.ticks;
    $("trades").textContent = s.trades;
    $("src").textContent = s.source;
    $("last").textContent = s.last_tick_at
      ? new Date(s.last_tick_at*1000).toLocaleTimeString() : "-";
    $("status").textContent = s.status;
    // The feed line says *why* there is no stream, not just that there is none.
    let feedTxt = s.running ? s.status : "paused";
    if(!s.running && !s.live_configured) feedTxt = "no live session";
    else if(s.feed && s.feed.state) feedTxt = s.feed.state;
    if(s.feed && s.feed.next_open_at && s.feed.state === "waiting"){
      feedTxt += " · next open "
        + new Date(s.feed.next_open_at*1000).toLocaleTimeString();
    }
    $("feedState").textContent = feedTxt;
    const dot = $("dot");
    dot.className = "dot" + (s.status==="streaming" ? " on"
                  : s.status==="error" ? " err" : "");
    if(s.last_error){ $("err").style.display="block"; $("err").textContent = s.last_error; }
    else $("err").style.display="none";

    // Paper trading is on by default and the engine trades on its own; the
    // page only reports what it did. There is no per-trade button by design.
    const paper = STALE ? (s.last_session||{}).paper : s.paper;
    // The toggle reflects the *current* configuration, never the stale one:
    // flipping a checkbox to match a saved session would misrepresent whether
    // the engine is trading now.
    const paperOn = s.paper !== null && s.paper !== undefined;
    const pill = $("paperPill");
    pill.textContent = paperOn ? "on" : "off";
    pill.className = "pill " + (paperOn ? "on" : "off");
    $("paperToggle").checked = paperOn;
    renderPaper(paper);
    renderStrip(s, STALE);

    if(SEL){
      let ch;
      const cached = STALE && s.last_session && s.last_session.charts
        ? s.last_session.charts[SEL] : null;
      if(cached){
        ch = cached;
      } else {
        const cr = await fetch(`/api/chart/${encodeURIComponent(SEL)}`);
        ch = await cr.json();
      }
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
$("btnPause").onclick = async () => {
  // One button, two jobs. It used to only ever stop, so a stray click killed
  // the feed for the rest of the process with no way back from the page --
  // exactly the trap the desk fell into. Now it mirrors the feed state: while
  // streaming it pauses, while stopped it brings the live session back.
  if(RUNNING){
    await post("/api/feed/stop", {});
  } else {
    const r = await post("/api/feed/start", {mode:"live"});
    if(r.detail) alert(r.detail);
  }
  tick();
};
$("paperToggle").onchange = async (e) => {
  await post("/api/paper", {enabled: e.target.checked});
};
$("btnReplay").onclick = async () => {
  // A capture is the only honest way to check the engine without risking a
  // live session: it drives the identical decision code over real prints.
  const path = $("replayPath").value.trim();
  if(!path) return;
  const r = await post("/api/feed/start", {mode:"replay", path});
  if(r.detail) alert(r.detail);
};
</script>
</body>
</html>
"""
