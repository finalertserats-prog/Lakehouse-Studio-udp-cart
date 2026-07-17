"""stack_composer.py — dynamic docker-compose.yml generator.

Reads COMPONENTS from component_registry, resolves dependencies, and emits
a complete docker-compose.yml for any arbitrary component selection.
stack_composer is the single source of truth for runtime docker-compose structure;
ai_configurator generates the config files that the services reference.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from .component_registry import COMPONENTS, resolve_dependencies
from .config import ROOT
from . import compose_safety


def _will_be_generated(local_path: str, resolved: list[str]) -> bool:
    """Return False for optional config files that ai_configurator won't generate
    when the relevant component isn't in the stack — prevents Docker bind-mount errors."""
    has_hdfs = any(c in resolved for c in ("hdfs", "hadoop-yarn", "hadoop-yarn-nm"))
    has_yarn = any(c in resolved for c in ("hadoop-yarn", "hadoop-yarn-nm"))
    has_hms  = "hive-metastore" in resolved
    has_tez  = "tez" in resolved

    if local_path in ("config/hadoop/core-site.xml", "config/hadoop/hdfs-site.xml"):
        return has_hdfs
    if local_path in ("config/hadoop/yarn-site.xml", "config/hadoop/mapred-site.xml"):
        return has_yarn
    if local_path == "config/hive/hive-site.xml":
        return has_hms
    if local_path == "config/tez/tez-site.xml":
        return has_tez
    return True


def compose(
    selected: list[str],
    version_overrides: dict[str, str] | None = None,
    include_experimental: bool = False,
    output_dir: Path | None = None,
) -> dict:
    """Build a docker-compose structure for the given selection.

    Returns:
      compose_yaml         full YAML text ready to write to disk
      resolved             component list after dep resolution (ordered)
      auto_added           components pulled in automatically
      skipped_experimental components omitted because experimental=True and
                           include_experimental=False
      volumes              named volumes declared
      config_files_needed  relative paths that ai_configurator must create
      port_map             service_id → [host_port, ...]
      warnings             list of human-readable warning strings
    """
    version_overrides = version_overrides or {}
    resolved_all = resolve_dependencies(selected)
    auto_added_all = [c for c in resolved_all if c not in selected]

    # Partition experimental vs normal
    skipped_experimental: list[str] = []
    if not include_experimental:
        resolved = [
            c for c in resolved_all
            if not COMPONENTS.get(c, {}).get("experimental", False)
        ]
        skipped_experimental = [
            c for c in resolved_all
            if COMPONENTS.get(c, {}).get("experimental", False)
        ]
    else:
        resolved = resolved_all

    auto_added = [c for c in resolved if c not in selected]
    warnings: list[str] = []
    if skipped_experimental:
        for cid in skipped_experimental:
            comp = COMPONENTS.get(cid, {})
            warnings.append(
                f"{cid} ({comp.get('name', cid)}) was skipped — "
                "no verified Docker Hub image. Enable 'include_experimental' to force-include."
            )

    services: dict[str, Any] = {}
    named_volumes: set[str] = set()
    config_files: list[str] = []
    port_map: dict[str, list[int]] = {}

    for cid in resolved:
        comp = COMPONENTS.get(cid)
        if not comp:
            continue

        version = version_overrides.get(cid, comp.get("default_version", "latest"))
        image_tag = f"{comp['image']}:{version}"
        svc: dict[str, Any] = {
            "image":          image_tag,
            "container_name": f"udp-{cid}",
            "hostname":       f"udp-{cid}",   # makes udp-* DNS names resolve in the network
            # One-shot init containers (bucket creation, etc.) must not restart
            # on clean exit, or they loop forever showing "Restarting".
            "restart":        comp.get("restart_policy", "unless-stopped"),
            "networks":       ["lakehouse"],
        }

        # If a local Dockerfile exists, add a build: section so docker-compose
        # builds the image when it's not available on Docker Hub.
        if build_df := comp.get("build_dockerfile"):
            if output_dir is not None:
                ctx = os.path.relpath(str(ROOT), str(output_dir))
            else:
                ctx = str(ROOT)
            svc["build"] = {"context": ctx, "dockerfile": build_df}

        env = comp.get("env", {})
        if env:
            svc["environment"] = dict(env)

        ports = comp.get("ports", [])
        if ports:
            mapped: list[str] = []
            host_ports: list[int] = []
            for p in ports:
                if isinstance(p, str):
                    mapped.append(p)
                    host_ports.append(int(p.split(":")[0]))
                else:
                    mapped.append(f"{p}:{p}")
                    host_ports.append(int(p))
            svc["ports"] = mapped
            port_map[cid] = host_ports

        if "command" in comp:
            svc["command"] = comp["command"]
        if "entrypoint" in comp:
            svc["entrypoint"] = comp["entrypoint"]
        if "user" in comp:
            svc["user"] = comp["user"]

        raw_vols = comp.get("volumes", [])
        svc_vols: list[str] = []
        for v in raw_vols:
            if v.startswith("./"):
                local = v.split(":")[0][2:]   # strip "./"
                if not _will_be_generated(local, resolved):
                    continue  # skip — file won't be created for this stack
                svc_vols.append(v)
                config_files.append(local)
            else:
                named_volumes.add(v.split(":")[0])
                svc_vols.append(v)
        if svc_vols:
            svc["volumes"] = svc_vols

        if hc := comp.get("healthcheck"):
            svc["healthcheck"] = dict(hc)

        raw_deps = [d for d in comp.get("depends_on", []) if d in resolved]
        if raw_deps:
            dep_map: dict[str, dict] = {}
            for dep in raw_deps:
                dep_comp = COMPONENTS.get(dep, {})
                cond = "service_healthy" if dep_comp.get("healthcheck") else "service_started"
                dep_map[dep] = {"condition": cond}
            svc["depends_on"] = dep_map

        services[cid] = svc

    doc: dict[str, Any] = {
        "version":  "3.9",
        "services": services,
        # Fixed network name WITHOUT underscores. Compose otherwise prefixes the
        # project dir (e.g. "local-demo_lakehouse") and the underscore makes the
        # resulting hostname an invalid Java URI — StarRocks/HMS catalog lookups
        # then fail with "Illegal character in hostname".
        "networks": {"lakehouse": {"name": "lakehousenet", "driver": "bridge"}},
    }
    if named_volumes:
        doc["volumes"] = {v: {} for v in sorted(named_volumes)}

    # Host-escape safety gate (P0.1). The composed doc is assembled from
    # registry components + AI/user selections and is then `docker compose up`'d,
    # so scan it for privileged/host-mount/socket/host-namespace constructs.
    # Reported here for the preview; write_compose() blocks the install on any.
    safety_violations = [str(v) for v in compose_safety.scan_compose_doc(doc)]

    compose_yaml = yaml.dump(
        doc,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=9999999,
    )

    # deduplicate config_files while preserving order
    seen: set[str] = set()
    deduped_cfg: list[str] = []
    for f in config_files:
        if f not in seen:
            seen.add(f)
            deduped_cfg.append(f)

    return {
        "compose_yaml":           compose_yaml,
        "resolved":               resolved,
        "auto_added":             auto_added,
        "skipped_experimental":   skipped_experimental,
        "volumes":                sorted(named_volumes),
        "config_files_needed":    deduped_cfg,
        "port_map":               port_map,
        "warnings":               warnings,
        "safety_violations":      safety_violations,
    }


def write_compose(
    output_dir: Path,
    selected: list[str],
    version_overrides: dict[str, str] | None = None,
    include_experimental: bool = False,
) -> dict:
    """Write docker-compose.yml to *output_dir*. Returns compose plan + output_path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    plan = compose(selected, version_overrides, include_experimental, output_dir=output_dir)
    # Block the install if the composed stack contains a host-escape construct.
    if plan.get("safety_violations"):
        lines = "\n  ".join(plan["safety_violations"])
        raise compose_safety.ComposeSafetyError(
            f"refusing to write an unsafe docker-compose.yml — "
            f"{len(plan['safety_violations'])} construct(s):\n  {lines}"
        )
    path = output_dir / "docker-compose.yml"
    path.write_text(plan["compose_yaml"], encoding="utf-8")
    plan["output_path"] = str(path)
    return plan
