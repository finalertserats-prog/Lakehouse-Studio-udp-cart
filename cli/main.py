"""`lks` command tree.

Top-level groups:
  catalog | templates | stacks | install | health | backup | tables | ai | export

Each command body does three things:
  1. Build an httpx client (from ctx.obj — set in `cli` root callback)
  2. Issue one or two HTTP calls via `client.request`
  3. Hand the response to a render helper (table or json)

No business logic lives here — the backend is the source of truth.
"""
from __future__ import annotations

from typing import Any, Optional

import click

from cli.client import DEFAULT_SERVER, download, make_client, request, resolve_token
from cli import render
from cli.ws import follow_logs


# ---------- root ----------

@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--server", default=DEFAULT_SERVER, show_default=True,
              help="Lakehouse Studio server base URL.")
@click.option("--token", "auth", default=None,
              help=("API auth credential. PREFER the LHS_TOKEN env var — "
                    "passing --token on the command line exposes the value "
                    "in process lists (ps), shell history, and system logs."))
@click.option("--output", type=click.Choice(["table", "json"]), default="table",
              show_default=True, help="Output format.")
@click.pass_context
def cli(ctx: click.Context, server: str, auth: Optional[str], output: str) -> None:
    """Lakehouse Studio CLI — talk to the running Studio backend."""
    ctx.ensure_object(dict)
    ctx.obj["server"] = server
    ctx.obj["auth"] = resolve_token(auth)
    ctx.obj["output"] = output


def _client(ctx: click.Context):
    """Build a Client from the root ctx. One per command — never reuse across
    commands, since httpx.Client owns sockets that must be closed."""
    return make_client(ctx.obj["server"], ctx.obj["auth"])


def _emit(ctx: click.Context, rows, columns, *, json_data=None, kv=None, title=None) -> None:
    """Branch on --output. `json_data` defaults to `rows` if not given —
    handy for kv-style emissions that still want JSON of the raw record."""
    if ctx.obj["output"] == "json":
        render.emit_json(json_data if json_data is not None else rows)
        return
    if kv is not None:
        render.emit_kv(kv, title=title)
    if rows is not None:
        render.emit_table(rows, columns, title=title)


# ---------- catalog ----------

@cli.group()
def catalog() -> None:
    """Component catalog (what can be installed)."""


@catalog.command("list")
@click.pass_context
def catalog_list(ctx: click.Context) -> None:
    """List every category and its components."""
    with _client(ctx) as c:
        data = request(c, "GET", "/api/catalog")
    rows: list[dict[str, Any]] = []
    for cat in data.get("categories", []):
        for comp in cat.get("components", []):
            rows.append({
                "category": cat.get("id"),
                "component": comp.get("id"),
                "name": comp.get("name"),
                "version": comp.get("version"),
                "readiness": comp.get("readiness"),
            })
    _emit(ctx, rows, ["category", "component", "name", "version", "readiness"],
          json_data=data, title="catalog")


# ---------- templates ----------

@cli.group()
def templates() -> None:
    """Use-case templates (curated stack recipes)."""


@templates.command("list")
@click.pass_context
def templates_list(ctx: click.Context) -> None:
    """List every template card."""
    with _client(ctx) as c:
        data = request(c, "GET", "/api/templates")
    rows = data.get("templates", [])
    _emit(ctx, rows, ["id", "label", "pitch", "readiness", "tags"],
          json_data=data, title="templates")


# ---------- stacks ----------

@cli.group()
def stacks() -> None:
    """Stacks — the deployable units."""


@stacks.command("list")
@click.pass_context
def stacks_list(ctx: click.Context) -> None:
    """List every stack manifest."""
    with _client(ctx) as c:
        data = request(c, "GET", "/api/stacks")
    rows = [
        {
            "id": s.get("id"),
            "name": s.get("name"),
            "version": s.get("version"),
            "maturity": s.get("maturity"),
            "components": len(s.get("components") or []),
        }
        for s in (data or [])
    ]
    _emit(ctx, rows, ["id", "name", "version", "maturity", "components"],
          json_data=data, title="stacks")


@stacks.command("compat")
@click.argument("stack_id")
@click.pass_context
def stacks_compat(ctx: click.Context, stack_id: str) -> None:
    """Show the compatibility lock summary plus any drift."""
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/stacks/{stack_id}/compatibility")
    summary = data.get("summary") or {}
    kv = [
        ("stack_id", data.get("stack_id")),
        ("certified", data.get("certified")),
        ("components_in_catalog", data.get("components_in_catalog")),
        ("lock_components", summary.get("components_count") if isinstance(summary, dict) else None),
        ("verified_on", summary.get("verified_on") if isinstance(summary, dict) else None),
        ("drift_count", len(data.get("drift") or [])),
    ]
    drift = data.get("drift") or []
    drift_rows = [{"problem": p} for p in drift]
    if ctx.obj["output"] == "json":
        render.emit_json(data)
        return
    render.emit_kv(kv, title=f"compatibility[{stack_id}]")
    if drift_rows:
        render.emit_table(drift_rows, ["problem"], title="drift")


@stacks.command("upgrades")
@click.argument("stack_id")
@click.pass_context
def stacks_upgrades(ctx: click.Context, stack_id: str) -> None:
    """List the curated upgrade candidates for a stack."""
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/stacks/{stack_id}/upgrades")
    rows = data.get("candidates") or []
    _emit(ctx, rows, ["id", "name", "from", "to", "risk", "notes"],
          json_data=data, title=f"upgrades[{stack_id}]")


# ---------- install ----------

@cli.group()
def install() -> None:
    """Install lifecycle (create / status / logs / retry / skip / cancel)."""


@install.command("create")
@click.argument("stack_id")
@click.option("--host", default="localhost", show_default=True)
@click.option("--install-dir", "install_dir", default=None,
              help="Target install dir. Defaults to the server's WORK_DIR/udp.")
@click.option("--lake", "lake_name", default=None, help="Friendly lake name.")
@click.option("--goal", default=None)
@click.pass_context
def install_create(ctx: click.Context, stack_id: str, host: str,
                   install_dir: Optional[str], lake_name: Optional[str],
                   goal: Optional[str]) -> None:
    """Kick off a new install of STACK_ID."""
    body: dict[str, Any] = {"stack_id": stack_id, "host": host}
    if install_dir:
        body["install_dir"] = install_dir
    if lake_name:
        body["lake_name"] = lake_name
    if goal:
        body["goal"] = goal
    with _client(ctx) as c:
        data = request(c, "POST", "/api/installs", json_body=body)
    kv = [
        ("install_id", data.get("install_id")),
        ("stack_id", data.get("stack_id")),
        ("state", data.get("state")),
        ("host", data.get("host")),
        ("install_dir", data.get("install_dir")),
        ("lake_name", data.get("lake_name")),
    ]
    if ctx.obj["output"] == "json":
        render.emit_json(data)
    else:
        render.emit_kv(kv, title="install created")


@install.command("status")
@click.argument("install_id")
@click.pass_context
def install_status(ctx: click.Context, install_id: str) -> None:
    """Show install state and per-step status."""
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/installs/{install_id}")
    kv = [
        ("install_id", data.get("install_id")),
        ("stack_id", data.get("stack_id")),
        ("state", data.get("state")),
        ("host", data.get("host")),
        ("install_dir", data.get("install_dir")),
        ("error", data.get("error")),
    ]
    steps = data.get("steps") or []
    rows = [
        {
            "id": s.get("id"),
            "name": s.get("name"),
            "status": s.get("status"),
            "exit_code": s.get("exit_code"),
            "duration_ms": s.get("duration_ms"),
        }
        for s in steps
    ]
    if ctx.obj["output"] == "json":
        render.emit_json(data)
        return
    render.emit_kv(kv, title=f"install[{install_id}]")
    render.emit_table(rows, ["id", "name", "status", "exit_code", "duration_ms"], title="steps")


@install.command("logs")
@click.argument("install_id")
@click.option("--follow", "-f", is_flag=True, default=False,
              help="Stream new events via WebSocket. Default: print history only.")
@click.pass_context
def install_logs(ctx: click.Context, install_id: str, follow: bool) -> None:
    """Print log history; with --follow, stream live events until interrupted."""
    if follow:
        exit_code = follow_logs(ctx.obj["server"], install_id, ctx.obj["auth"])
        ctx.exit(exit_code)
        return

    # No-follow path: pull buffered history. The diagnose endpoint includes a
    # log tail; we use it as our history source so we don't need to add a
    # backend route for CLI-only consumption.
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/installs/{install_id}")
    # Best-effort: if the install has events, the diagnose endpoint will surface them.
    # Fall back to printing the steps inline if no log tail is available.
    history: list[dict[str, Any]] = []
    if data.get("state") == "FAILED":
        with _client(ctx) as c:
            diag = request(c, "GET", f"/api/installs/{install_id}/diagnose")
        if isinstance(diag, dict):
            explanation = diag.get("explanation")
            if explanation:
                history.append({"kind": "diagnose", "line": str(explanation)})
    for s in (data.get("steps") or []):
        if s.get("status"):
            history.append({
                "kind": "step",
                "step": s.get("id"),
                "line": f"{s.get('status')} (exit={s.get('exit_code')})",
            })
    if ctx.obj["output"] == "json":
        render.emit_json(history)
        return
    if not history:
        render.echo("(no history — use --follow to stream live events)")
        return
    for evt in history:
        render.echo(f"[{evt.get('kind')}] {evt.get('step', '')}: {evt.get('line', '')}")


def _step_action(ctx: click.Context, install_id: str, step_id: str, action: str) -> None:
    with _client(ctx) as c:
        data = request(c, "POST", f"/api/installs/{install_id}/steps/{action}",
                       json_body={"step_id": step_id})
    if ctx.obj["output"] == "json":
        render.emit_json(data)
    else:
        render.emit_kv(list((data or {}).items()), title=f"{action} {step_id}")


@install.command("retry")
@click.argument("install_id")
@click.argument("step_id")
@click.pass_context
def install_retry(ctx: click.Context, install_id: str, step_id: str) -> None:
    """Resume a FAILED install starting from STEP_ID."""
    _step_action(ctx, install_id, step_id, "retry")


@install.command("skip")
@click.argument("install_id")
@click.argument("step_id")
@click.pass_context
def install_skip(ctx: click.Context, install_id: str, step_id: str) -> None:
    """Skip STEP_ID (if it's marked skippable) and continue."""
    _step_action(ctx, install_id, step_id, "skip")


@install.command("cancel")
@click.argument("install_id")
@click.pass_context
def install_cancel(ctx: click.Context, install_id: str) -> None:
    """Cancel an in-flight install."""
    with _client(ctx) as c:
        data = request(c, "POST", f"/api/installs/{install_id}/cancel")
    if ctx.obj["output"] == "json":
        render.emit_json(data)
    else:
        render.emit_kv(list((data or {}).items()), title="cancel")


# ---------- health ----------

@cli.command("health")
@click.argument("install_id")
@click.pass_context
def health(ctx: click.Context, install_id: str) -> None:
    """Per-service health snapshot for an installed stack."""
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/installs/{install_id}/health")
    services = data.get("services") or data.get("components") or []
    if isinstance(services, dict):
        rows = [{"service": k, **(v if isinstance(v, dict) else {"value": v})} for k, v in services.items()]
    else:
        rows = services
    _emit(ctx, rows, ["service", "container_state", "probe", "healthy", "message"],
          json_data=data, title=f"health[{install_id}]")


# ---------- backup ----------

@cli.group()
def backup() -> None:
    """Backup lifecycle (create / list / restore / delete)."""


@backup.command("create")
@click.argument("install_id")
@click.option("--kind", type=click.Choice(["metadata", "full"]), default="metadata",
              show_default=True)
@click.pass_context
def backup_create(ctx: click.Context, install_id: str, kind: str) -> None:
    """Create a backup tarball for INSTALL_ID."""
    with _client(ctx) as c:
        data = request(c, "POST", f"/api/installs/{install_id}/backups",
                       json_body={"kind": kind})
    if ctx.obj["output"] == "json":
        render.emit_json(data)
    else:
        render.emit_kv(list((data or {}).items()), title="backup created")


@backup.command("list")
@click.argument("install_id")
@click.pass_context
def backup_list(ctx: click.Context, install_id: str) -> None:
    """List backups for INSTALL_ID."""
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/installs/{install_id}/backups")
    rows = data or []
    _emit(ctx, rows, ["backup_id", "kind", "created_at", "size_bytes", "path"],
          json_data=data, title=f"backups[{install_id}]")


@backup.command("restore")
@click.argument("backup_id")
@click.pass_context
def backup_restore(ctx: click.Context, backup_id: str) -> None:
    """Restore BACKUP_ID over its source install."""
    with _client(ctx) as c:
        data = request(c, "POST", f"/api/backups/{backup_id}/restore")
    if ctx.obj["output"] == "json":
        render.emit_json(data)
    else:
        render.emit_kv(list((data or {}).items()), title=f"restore {backup_id}")


@backup.command("delete")
@click.argument("backup_id")
@click.pass_context
def backup_delete(ctx: click.Context, backup_id: str) -> None:
    """Delete BACKUP_ID."""
    with _client(ctx) as c:
        request(c, "DELETE", f"/api/backups/{backup_id}")
    if ctx.obj["output"] == "json":
        render.emit_json({"deleted": backup_id})
    else:
        render.echo(f"deleted backup {backup_id}")


# ---------- tables ----------

@cli.group()
def tables() -> None:
    """Iceberg table catalog browser."""


@tables.command("list")
@click.argument("install_id")
@click.pass_context
def tables_list(ctx: click.Context, install_id: str) -> None:
    """List every (namespace, table) pair the Iceberg catalog knows about."""
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/installs/{install_id}/tables")
    rows = data.get("tables") or []
    _emit(ctx, rows, ["namespace", "name", "location"],
          json_data=data, title=f"tables[{install_id}]")


@tables.command("describe")
@click.argument("install_id")
@click.argument("namespace")
@click.argument("name")
@click.pass_context
def tables_describe(ctx: click.Context, install_id: str, namespace: str, name: str) -> None:
    """Describe a single Iceberg table."""
    with _client(ctx) as c:
        data = request(c, "GET", f"/api/installs/{install_id}/tables/{namespace}/{name}")
    if ctx.obj["output"] == "json":
        render.emit_json(data)
        return
    kv = [
        ("namespace", namespace),
        ("name", name),
        ("location", data.get("location")),
        ("schema_id", data.get("schema_id") if not isinstance(data.get("schema"), dict)
         else (data.get("schema") or {}).get("schema-id")),
        ("snapshot_count", len(data.get("snapshots") or [])),
    ]
    render.emit_kv(kv, title=f"{namespace}.{name}")
    schema = data.get("schema") or {}
    fields = schema.get("fields") if isinstance(schema, dict) else None
    if fields:
        rows = [
            {"id": f.get("id"), "name": f.get("name"),
             "type": f.get("type"), "required": f.get("required")}
            for f in fields
        ]
        render.emit_table(rows, ["id", "name", "type", "required"], title="schema")


# ---------- ai ----------

@cli.group()
def ai() -> None:
    """Ask Studio (grounded LLM over your project)."""


@ai.command("ask")
@click.argument("install_id")
@click.argument("question")
@click.pass_context
def ai_ask(ctx: click.Context, install_id: str, question: str) -> None:
    """Ask QUESTION in the context of INSTALL_ID."""
    body = {"install_id": install_id, "question": question}
    with _client(ctx) as c:
        data = request(c, "POST", "/api/ai/ask", json_body=body)
    if ctx.obj["output"] == "json":
        render.emit_json(data)
        return
    render.echo((data or {}).get("answer") or "(no answer)")
    sources = (data or {}).get("sources") or []
    if sources:
        render.emit_table(
            [{"source": s} if isinstance(s, str) else s for s in sources],
            ["source"], title="sources",
        )


# ---------- export ----------

@cli.command("export")
@click.argument("install_id")
@click.option("-o", "--output-file", "out_path", required=True,
              type=click.Path(dir_okay=False, writable=True),
              help="Where to write the .tar.gz bundle.")
@click.pass_context
def export(ctx: click.Context, install_id: str, out_path: str) -> None:
    """Download a GitOps export bundle for INSTALL_ID."""
    with _client(ctx) as c:
        written = download(c, f"/api/installs/{install_id}/export", out_path)
    if ctx.obj["output"] == "json":
        render.emit_json({"install_id": install_id, "path": out_path, "bytes": written})
    else:
        render.echo(f"wrote {written} bytes to {out_path}")


if __name__ == "__main__":  # pragma: no cover
    cli()
