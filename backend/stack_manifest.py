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


def list_manifests() -> list[StackManifest]:
    out = []
    for p in sorted(STACKS_DIR.glob("*.yaml")):
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        out.append(StackManifest(data, p))
    return out


def load_manifest(stack_id: str) -> StackManifest:
    for m in list_manifests():
        if m.id == stack_id:
            return m
    raise KeyError(f"Stack '{stack_id}' not found")
