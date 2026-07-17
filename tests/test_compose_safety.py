"""Tests for backend.compose_safety — the composed-stack host-escape gate."""
from __future__ import annotations

import pytest

from backend import compose_safety as cs


def _doc(svc: dict) -> dict:
    return {"services": {"app": svc}}


# ---------------------------------------------------------------------------
# Safe specs — a normal data-plane service must pass cleanly.
# ---------------------------------------------------------------------------

def test_ordinary_service_is_clean():
    doc = _doc({
        "image": "minio/minio:latest",
        "ports": ["9000:9000"],
        "volumes": ["minio-data:/data", "./config/minio.env:/etc/minio.env:ro"],
        "networks": ["default"],
        "cap_add": ["NET_BIND_SERVICE"],
    })
    assert cs.scan_compose_doc(doc) == []
    cs.assert_compose_safe(doc)  # does not raise


def test_named_volume_and_relative_bind_are_allowed():
    doc = _doc({"image": "x", "volumes": ["udp-mysql-hms-data:/var/lib/mysql", "./conf:/conf:ro"]})
    assert cs.scan_compose_doc(doc) == []


def test_empty_and_malformed_docs_do_not_crash():
    assert cs.scan_compose_doc({}) == []
    assert cs.scan_compose_doc({"services": None}) == []
    assert cs.scan_compose_doc({"services": {"a": "notadict"}}) == []


# ---------------------------------------------------------------------------
# Host-escape primitives — each must be flagged.
# ---------------------------------------------------------------------------

def test_privileged_is_rejected():
    v = cs.scan_compose_doc(_doc({"image": "x", "privileged": True}))
    assert [x.kind for x in v] == ["privileged"]


def test_privileged_string_true_is_rejected():
    assert cs.scan_compose_doc(_doc({"image": "x", "privileged": "true"}))


def test_docker_socket_mount_is_rejected():
    v = cs.scan_compose_doc(_doc({"image": "x", "volumes": ["/var/run/docker.sock:/var/run/docker.sock"]}))
    assert any(x.kind == "docker-socket" for x in v)


@pytest.mark.parametrize("host", ["/", "/etc:/etc", "/root/.ssh:/keys", "/proc:/host/proc", "/usr/bin:/x"])
def test_sensitive_host_bind_mounts_are_rejected(host):
    vol = host if ":" in host else f"{host}:/mnt"
    v = cs.scan_compose_doc(_doc({"image": "x", "volumes": [vol]}))
    assert any(x.kind in ("host-path", "docker-socket") for x in v), f"{host} not flagged"


def test_path_traversal_bind_mount_is_rejected():
    v = cs.scan_compose_doc(_doc({"image": "x", "volumes": ["../../etc:/x"]}))
    assert any(x.kind == "path-traversal" for x in v)


@pytest.mark.parametrize("key", ["pid", "ipc", "userns_mode", "uts"])
def test_host_namespace_sharing_is_rejected(key):
    v = cs.scan_compose_doc(_doc({"image": "x", key: "host"}))
    assert any(x.kind == f"{key}-host" for x in v)


def test_network_mode_host_is_rejected():
    v = cs.scan_compose_doc(_doc({"image": "x", "network_mode": "host"}))
    assert any(x.kind == "network-host" for x in v)


@pytest.mark.parametrize("cap", ["SYS_ADMIN", "NET_ADMIN", "SYS_PTRACE", "ALL", "sys_admin"])
def test_dangerous_capabilities_are_rejected(cap):
    v = cs.scan_compose_doc(_doc({"image": "x", "cap_add": [cap]}))
    assert any(x.kind == "cap-add" for x in v)


def test_host_devices_are_rejected():
    v = cs.scan_compose_doc(_doc({"image": "x", "devices": ["/dev/sda:/dev/sda"]}))
    assert any(x.kind == "devices" for x in v)


@pytest.mark.parametrize("opt", ["seccomp:unconfined", "apparmor:unconfined"])
def test_unconfined_security_opt_is_rejected(opt):
    v = cs.scan_compose_doc(_doc({"image": "x", "security_opt": [opt]}))
    assert any(x.kind == "security-opt" for x in v)


def test_long_form_bind_mount_of_socket_is_rejected():
    doc = _doc({"image": "x", "volumes": [
        {"type": "bind", "source": "/var/run/docker.sock", "target": "/sock"}
    ]})
    assert any(x.kind == "docker-socket" for x in cs.scan_compose_doc(doc))


# ---------------------------------------------------------------------------
# assert_compose_safe raises with a helpful message listing every violation.
# ---------------------------------------------------------------------------

def test_assert_raises_and_lists_all_violations():
    doc = _doc({"image": "x", "privileged": True, "network_mode": "host",
                "volumes": ["/var/run/docker.sock:/s"]})
    with pytest.raises(cs.ComposeSafetyError) as ei:
        cs.assert_compose_safe(doc)
    msg = str(ei.value)
    assert "privileged" in msg and "network-host" in msg and "docker-socket" in msg


def test_multiple_services_are_all_scanned():
    doc = {"services": {
        "good": {"image": "x", "volumes": ["data:/data"]},
        "bad": {"image": "y", "privileged": True},
    }}
    v = cs.scan_compose_doc(doc)
    assert [x.service for x in v] == ["bad"]
