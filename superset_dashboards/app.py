"""Superset Dashboard Register — a small web app.

Point it at any running Superset URL, enter read-only credentials, and it
pulls the full dashboard inventory (team, owner, frequency, disposition)
into a browsable table you can download as Excel.

Run:
    uvicorn app:app --port 8099          (from this folder)
    # or:  python app.py

Nothing is written to Superset. All calls are GET-only.
"""
from __future__ import annotations

import base64
import time
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel

from excel_export import build_workbook
from superset_client import FetchResult, SupersetClient, SupersetError

app = FastAPI(title="Superset Dashboard Register")

# Small in-memory cache so "Fetch" then "Download Excel" doesn't re-hit
# Superset. Keyed by an opaque token handed to the browser. Not persisted.
_CACHE: dict[str, tuple[float, FetchResult]] = {}
_CACHE_TTL = 1800  # 30 min


class FetchRequest(BaseModel):
    url: str
    username: str
    password: str
    provider: str = "db"
    verify_ssl: bool = True


def _prune_cache() -> None:
    now = time.time()
    for k in [k for k, (ts, _) in _CACHE.items() if now - ts > _CACHE_TTL]:
        _CACHE.pop(k, None)


def _run_fetch(req: FetchRequest) -> FetchResult:
    with SupersetClient(req.url, verify=req.verify_ssl) as client:
        client.login(req.username, req.password, provider=req.provider)
        return client.fetch_dashboards()


@app.post("/api/fetch")
def api_fetch(req: FetchRequest) -> JSONResponse:
    if not req.url.strip():
        return JSONResponse({"error": "Superset URL is required."}, status_code=400)
    try:
        result = _run_fetch(req)
    except SupersetError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception as exc:  # noqa: BLE001 — surface anything unexpected cleanly
        return JSONResponse({"error": f"Unexpected error: {exc}"}, status_code=500)

    _prune_cache()
    token = base64.urlsafe_b64encode(f"{req.url}:{time.time()}".encode()).decode()
    _CACHE[token] = (time.time(), result)

    return JSONResponse(
        {
            "token": token,
            "base_url": result.base_url,
            "version": result.version,
            "count": result.count,
            "warnings": result.warnings,
            "dashboards": [d.as_dict() for d in result.dashboards],
        }
    )


@app.get("/api/export/{token}")
def api_export(token: str) -> Response:
    entry = _CACHE.get(token)
    if not entry:
        return JSONResponse(
            {"error": "Result expired — please fetch again."}, status_code=404
        )
    _, result = entry
    xlsx = build_workbook(result)
    fname = f"superset_dashboards_{time.strftime('%Y%m%d')}.xlsx"
    return Response(
        content=xlsx,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_INDEX_HTML)


_INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Superset Dashboard Register</title>
<style>
  :root{
    --bg:#0f172a; --panel:#111827; --card:#1f2937; --line:#374151;
    --text:#e5e7eb; --muted:#9ca3af; --accent:#38bdf8; --accent-d:#0284c7;
    --ok:#10b981; --warn:#f59e0b; --err:#ef4444;
  }
  *{box-sizing:border-box}
  body{margin:0;font:14px/1.5 system-ui,Segoe UI,Roboto,sans-serif;
       background:var(--bg);color:var(--text)}
  header{padding:20px 28px;border-bottom:1px solid var(--line);
         background:var(--panel)}
  header h1{margin:0;font-size:19px}
  header p{margin:4px 0 0;color:var(--muted);font-size:13px}
  main{padding:24px 28px;max-width:1400px;margin:0 auto}
  .form{display:grid;grid-template-columns:2fr 1fr 1fr auto;gap:12px;
        align-items:end;background:var(--card);padding:18px;border-radius:10px;
        border:1px solid var(--line)}
  .field{display:flex;flex-direction:column;gap:6px}
  .field.full{grid-column:1/-1}
  label{font-size:12px;color:var(--muted)}
  input{background:#0b1220;border:1px solid var(--line);color:var(--text);
        padding:9px 11px;border-radius:7px;font-size:14px}
  input:focus{outline:none;border-color:var(--accent)}
  .row{display:flex;gap:16px;align-items:center;flex-wrap:wrap}
  .check{display:flex;align-items:center;gap:7px;color:var(--muted);font-size:13px}
  button{background:var(--accent-d);color:#fff;border:none;padding:10px 20px;
         border-radius:7px;font-size:14px;font-weight:600;cursor:pointer}
  button:hover{background:var(--accent)}
  button:disabled{opacity:.5;cursor:not-allowed}
  button.ghost{background:transparent;border:1px solid var(--line);color:var(--text)}
  .toolbar{display:flex;gap:12px;align-items:center;margin:22px 0 12px;
           flex-wrap:wrap}
  .search{flex:1;min-width:200px}
  .msg{padding:12px 14px;border-radius:8px;margin-top:14px;font-size:13px}
  .msg.err{background:rgba(239,68,68,.12);border:1px solid var(--err);color:#fecaca}
  .msg.warn{background:rgba(245,158,11,.12);border:1px solid var(--warn);color:#fde68a}
  .stat{color:var(--muted);font-size:13px}
  .stat b{color:var(--text)}
  table{width:100%;border-collapse:collapse;margin-top:8px;font-size:13px}
  th,td{text-align:left;padding:9px 11px;border-bottom:1px solid var(--line);
        vertical-align:top}
  th{position:sticky;top:0;background:var(--panel);cursor:pointer;
     user-select:none;white-space:nowrap}
  th:hover{color:var(--accent)}
  tr:hover td{background:rgba(56,189,248,.05)}
  .pill{display:inline-block;padding:2px 9px;border-radius:999px;font-size:12px}
  .pill.pub{background:rgba(16,185,129,.16);color:#6ee7b7}
  .pill.draft{background:rgba(156,163,175,.18);color:#d1d5db}
  .pill.super{background:rgba(56,189,248,.16);color:#7dd3fc}
  a{color:var(--accent);text-decoration:none}
  a:hover{text-decoration:underline}
  .empty{color:var(--muted);padding:40px;text-align:center}
  .spin{display:inline-block;width:15px;height:15px;border:2px solid #fff;
        border-top-color:transparent;border-radius:50%;animation:s .7s linear infinite;
        vertical-align:-2px;margin-right:6px}
  @keyframes s{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<header>
  <h1>Superset Dashboard Register</h1>
  <p>Point at a running Superset instance and pull its full dashboard inventory — team, owner, frequency, disposition — then export to Excel. Read-only; nothing is modified.</p>
</header>
<main>
  <form class="form" id="form">
    <div class="field full">
      <label>Superset URL</label>
      <input id="url" placeholder="https://superset.yourcompany.com" autocomplete="off"/>
    </div>
    <div class="field">
      <label>Username</label>
      <input id="username" autocomplete="username"/>
    </div>
    <div class="field">
      <label>Password</label>
      <input id="password" type="password" autocomplete="current-password"/>
    </div>
    <div class="field">
      <label>Auth provider</label>
      <input id="provider" value="db" title="db (built-in) or ldap"/>
    </div>
    <div class="field">
      <button type="submit" id="go">Fetch dashboards</button>
    </div>
    <div class="field full">
      <div class="row">
        <label class="check"><input type="checkbox" id="verify" checked/> Verify SSL certificate</label>
      </div>
    </div>
  </form>

  <div id="messages"></div>

  <div class="toolbar" id="toolbar" style="display:none">
    <input class="search" id="search" placeholder="Filter by title, owner, team, tag…"/>
    <span class="stat" id="stat"></span>
    <button class="ghost" id="export">⬇ Download Excel</button>
  </div>

  <div id="tableWrap"></div>
</main>

<script>
const $ = (id) => document.getElementById(id);
let STATE = { token:null, rows:[], sortKey:"title", sortDir:1 };

const COLS = [
  ["title","Dashboard"], ["team","Team"], ["owners","Owner(s)"],
  ["frequency","Frequency"], ["disposition","Disposition"], ["status","Status"],
  ["tags","Tags"], ["last_modified","Last modified"], ["last_modified_by","Modified by"],
];

function msg(html){ $("messages").innerHTML = html; }

$("form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const body = {
    url: $("url").value.trim(),
    username: $("username").value,
    password: $("password").value,
    provider: $("provider").value.trim() || "db",
    verify_ssl: $("verify").checked,
  };
  if(!body.url){ msg(`<div class="msg err">Enter a Superset URL.</div>`); return; }
  $("go").disabled = true;
  $("go").innerHTML = `<span class="spin"></span>Fetching…`;
  msg("");
  try{
    const r = await fetch("/api/fetch", {
      method:"POST", headers:{"Content-Type":"application/json"},
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if(!r.ok){ msg(`<div class="msg err">${data.error||"Failed"}</div>`); return; }
    STATE.token = data.token;
    STATE.rows = data.dashboards;
    let warnHtml = (data.warnings||[]).map(w=>`<div class="msg warn">${w}</div>`).join("");
    msg(warnHtml);
    $("toolbar").style.display = "flex";
    $("stat").innerHTML = `<b>${data.count}</b> dashboards · Superset <b>${data.version}</b>`;
    render();
  }catch(err){
    msg(`<div class="msg err">Network error: ${err.message}</div>`);
  }finally{
    $("go").disabled = false; $("go").textContent = "Fetch dashboards";
  }
});

$("search").addEventListener("input", render);
$("export").addEventListener("click", () => {
  if(STATE.token) window.location = "/api/export/" + encodeURIComponent(STATE.token);
});

function render(){
  const q = ($("search").value||"").toLowerCase();
  let rows = STATE.rows.filter(d =>
    !q || [d.title,d.owners,d.team,d.tags,d.frequency].join(" ").toLowerCase().includes(q));
  rows.sort((a,b)=>{
    const k = STATE.sortKey;
    return String(a[k]||"").localeCompare(String(b[k]||"")) * STATE.sortDir;
  });
  if(!rows.length){ $("tableWrap").innerHTML = `<div class="empty">No dashboards match.</div>`; return; }
  const head = COLS.map(([k,label]) =>
    `<th data-k="${k}">${label}${STATE.sortKey===k?(STATE.sortDir>0?" ▲":" ▼"):""}</th>`).join("");
  const body = rows.map(d=>{
    const statusPill = d.status==="Published"
      ? `<span class="pill pub">Published</span>` : `<span class="pill draft">Draft</span>`;
    const title = d.url ? `<a href="${d.url}" target="_blank" rel="noopener">${esc(d.title)}</a>` : esc(d.title);
    return `<tr>
      <td>${title}</td><td>${esc(d.team)}</td><td>${esc(d.owners)}</td>
      <td>${esc(d.frequency)||'<span class="stat">—</span>'}</td>
      <td><span class="pill super">${esc(d.disposition)}</span></td>
      <td>${statusPill}</td><td>${esc(d.tags)||'<span class="stat">—</span>'}</td>
      <td>${esc(d.last_modified)}</td><td>${esc(d.last_modified_by)}</td>
    </tr>`;
  }).join("");
  $("tableWrap").innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  document.querySelectorAll("th").forEach(th=>th.addEventListener("click",()=>{
    const k = th.dataset.k;
    if(STATE.sortKey===k) STATE.sortDir*=-1; else {STATE.sortKey=k; STATE.sortDir=1;}
    render();
  }));
}
function esc(s){ return String(s==null?"":s).replace(/[&<>"]/g,c=>(
  {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c])); }
</script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8099)
