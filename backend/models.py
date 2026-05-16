from __future__ import annotations
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field


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


class InstallRequest(BaseModel):
    stack_id: str
    host: str = "localhost"
    install_dir: Optional[str] = None
    env_overrides: dict[str, str] = Field(default_factory=dict)
    lake_name: Optional[str] = None
    goal: Optional[str] = None
    cart: Optional[list[str]] = None


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


class LogEvent(BaseModel):
    install_id: str
    ts: float
    kind: Literal["step_start", "step_end", "log", "state", "error", "result"]
    step: Optional[str] = None
    stream: Optional[Literal["stdout", "stderr"]] = None
    line: Optional[str] = None
    status: Optional[str] = None
    payload: Optional[dict[str, Any]] = None
