from __future__ import annotations
import re
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field, field_validator


# Limits used across endpoints that take user-supplied identifiers.
MAX_CART_ITEMS = 50
MAX_COMPONENT_ID_LEN = 64
MAX_GOAL_ID_LEN = 64
MAX_LAKE_NAME_LEN = 32
_IDENT_RE = re.compile(r"^[a-z][a-z0-9_\-]{0,62}[a-z0-9]$|^[a-z]$")


def _validate_component_id_list(v: Any, field_name: str = "cart") -> list[str]:
    """Shared validator: list of lowercase identifiers, no duplicates, bounded length."""
    if v is None:
        return []
    if not isinstance(v, list):
        raise ValueError(f"{field_name} must be a list")
    if len(v) > MAX_CART_ITEMS:
        raise ValueError(f"{field_name} has {len(v)} items (max {MAX_CART_ITEMS})")
    cleaned: list[str] = []
    seen: set[str] = set()
    for i, item in enumerate(v):
        if not isinstance(item, str):
            raise ValueError(f"{field_name}[{i}] must be a string (got {type(item).__name__})")
        if not item or len(item) > MAX_COMPONENT_ID_LEN:
            raise ValueError(f"{field_name}[{i}] length must be 1..{MAX_COMPONENT_ID_LEN}")
        if not _IDENT_RE.match(item):
            raise ValueError(f"{field_name}[{i}]={item!r}: invalid identifier (lowercase letters/digits/-_)")
        if item in seen:
            raise ValueError(f"{field_name}: duplicate component id '{item}'")
        seen.add(item)
        cleaned.append(item)
    return cleaned


InstallState = Literal[
    "DRAFT",
    "INSPECTING",
    "READY_TO_INSTALL",
    "CLONING_REPO",
    "WRITING_ENV",
    "RUNNING_DOCTOR",
    "STARTING_STACK",
    "BOOTSTRAPPING",
    "SMOKE_TESTING",
    "READY",
    "FAILED",
    "STOPPED",
    "CLEANED",
]


class InspectionCheck(BaseModel):
    name: str
    status: Literal["passed", "warning", "failed", "skipped"]
    message: str
    detail: Optional[str] = None


class InspectionReport(BaseModel):
    host: str
    overall: Literal["passed", "warning", "failed"]
    checks: list[InspectionCheck]
    recommended: bool


EnvironmentTier = Literal["dev", "staging", "prod"]


class InstallRequest(BaseModel):
    stack_id: str = Field(min_length=1, max_length=128)
    host: str = Field(default="localhost", min_length=1, max_length=253)
    install_dir: Optional[str] = Field(default=None, max_length=4096)
    env_overrides: dict[str, str] = Field(default_factory=dict)
    lake_name: Optional[str] = Field(default=None, max_length=MAX_LAKE_NAME_LEN)
    goal: Optional[str] = Field(default=None, max_length=MAX_GOAL_ID_LEN)
    cart: Optional[list[str]] = Field(default=None)
    # Optional environment tier. When set, the runner derives a unique
    # install_dir suffix and injects UDP_ENV + UDP_PROJECT_NAME so multiple
    # environments can co-exist on the same host without container or
    # volume collisions. None = single-environment install (legacy default).
    environment: Optional[EnvironmentTier] = None
    # SSH credentials for remote installs. When host is not localhost/127.0.0.1
    # and ssh_user is set, the runner SSHes into the target and runs all steps
    # there instead of locally.
    ssh_user: Optional[str] = Field(default=None, max_length=64)
    ssh_key_path: Optional[str] = Field(default=None, max_length=4096)
    ssh_password: Optional[str] = Field(default=None, max_length=256)
    ssh_port: int = Field(default=22, ge=1, le=65535)

    @field_validator("cart")
    @classmethod
    def _validate_cart(cls, v: Any) -> Optional[list[str]]:
        if v is None:
            return None
        return _validate_component_id_list(v, "cart")


class StepStatus(BaseModel):
    id: str
    title: str
    status: Literal["pending", "running", "success", "failed", "skipped"] = "pending"
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    exit_code: Optional[int] = None
    message: Optional[str] = None


class InstallRecord(BaseModel):
    install_id: str
    stack_id: str
    host: str
    install_dir: str
    state: InstallState = "DRAFT"
    created_at: float
    updated_at: float
    steps: list[StepStatus]
    error: Optional[str] = None
    outputs: dict[str, Any] = Field(default_factory=dict)
    lake_name: Optional[str] = None
    goal: Optional[str] = None
    cart: list[str] = Field(default_factory=list)
    environment: Optional[EnvironmentTier] = None
    # SSH metadata for remote installs (no password — key-path auth only for retry)
    ssh_user: Optional[str] = None
    ssh_key_path: Optional[str] = None
    ssh_port: int = 22


class LogEvent(BaseModel):
    install_id: str
    ts: float
    kind: Literal["step_start", "step_end", "log", "state", "error", "result", "reset"]
    step: Optional[str] = None
    stream: Optional[Literal["stdout", "stderr"]] = None
    line: Optional[str] = None
    status: Optional[str] = None
    payload: Optional[dict[str, Any]] = None
    seq: Optional[int] = None  # assigned by the bus on publish
