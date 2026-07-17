from __future__ import annotations
from pathlib import Path
from typing import Any
import yaml

from .config import STACKS_DIR


class StackManifest:
    def __init__(self, data: dict[str, Any], path: Path):
        self.data = data
        self.path = path

    @property
    def id(self) -> str:
        return self.data["id"]

    @property
    def name(self) -> str:
        return self.data["name"]

    @property
    def repository(self) -> dict[str, Any]:
        return self.data.get("repository", {})

    @property
    def requirements(self) -> dict[str, Any]:
        return self.data.get("requirements", {})

    @property
    def required_ports(self) -> list[int]:
        return list(self.data.get("ports", {}).get("required", []))

    @property
    def env_defaults(self) -> dict[str, str]:
        return {k: str(v) for k, v in self.data.get("env_defaults", {}).items()}

    @property
    def mode(self) -> str:
        return self.data.get("mode", "docker-compose")

    @property
    def is_remote_cluster(self) -> bool:
        return self.mode == "remote-cluster"

    @property
    def components(self) -> list[dict[str, Any]]:
        return list(self.data.get("components", []))

    def command(self, name: str) -> dict[str, Any]:
        cmds = self.data.get("commands", {})
        if name not in cmds:
            raise KeyError(f"Stack {self.id} has no command '{name}'")
        return cmds[name]

    def output_urls(self, host: str) -> dict[str, dict[str, str]]:
        out = {}
        urls = self.data.get("outputs", {}).get("urls", {})
        for key, spec in urls.items():
            out[key] = {
                "label": spec.get("label", key),
                "url": spec["url"].format(host=host),
            }
        return out

    def output_connections(self, host: str) -> dict[str, str]:
        conns = self.data.get("outputs", {}).get("connections", {})
        return {k: v.format(host=host) for k, v in conns.items()}


# Files in stacks/ that are NOT stack manifests (e.g. the component catalog).
# These are handled by their own loaders.
_NON_STACK_YAML = {"components-catalog.yaml"}


def list_manifests() -> list[StackManifest]:
    """Load every *.yaml in stacks/ that looks like a stack manifest.

    A file is treated as a stack manifest only if it (a) is not on the
    skip list and (b) parses to a dict with an `id` field. Anything else
    is logged and skipped — adding a new YAML file to stacks/ for an
    unrelated purpose should never crash the install endpoint."""
    import logging
    log = logging.getLogger("lhs.stack_manifest")
    out: list[StackManifest] = []
    for p in sorted(STACKS_DIR.glob("*.yaml")):
        if p.name in _NON_STACK_YAML:
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except Exception as e:
            log.warning("skipping %s: yaml parse failed (%s)", p.name, e)
            continue
        if not isinstance(data, dict) or "id" not in data:
            log.warning("skipping %s: not a stack manifest (missing top-level 'id')", p.name)
            continue
        out.append(StackManifest(data, p))
    return out


def load_manifest(stack_id: str) -> StackManifest:
    for m in list_manifests():
        if m.id == stack_id:
            return m
    raise KeyError(f"Stack '{stack_id}' not found")
