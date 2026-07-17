"""v0.6.1 — tests for backend/stack_compose_fragments.py.

Covers:
  - write_fragment returns None for stacks without a registered renderer
    (the stable udp-local-v0.2 stack is the canonical case)
  - write_fragment writes a docker-compose.fragment.yml for each of the
    four candidate stacks
  - The written YAML parses cleanly via yaml.safe_load
  - Every service declared in FRAGMENT_SERVICES for a given stack
    actually appears at the top-level services: map in the rendered YAML
  - Each rendered service carries the contract keys: image, container_name,
    healthcheck
  - FRAGMENT_SERVICES is consistent with the renderers (the runner relies
    on this mapping to extend the `docker compose up -d <services>` argv)
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from backend import stack_compose_fragments as scf


CANDIDATE_STACK_IDS = [
    # 2026-05-17 bug fix: udp-trino-local-v0.1 now needs a fragment too —
    # UDP's upstream docker-compose.yml has no `trino` service definition,
    # so `docker compose up -d ... trino ...` failed with `no such service`.
    "udp-trino-local-v0.1",
    "iceberg-nessie-trino-local-v0.1",
    "hudi-hms-spark-local-v0.1",
    "delta-hms-spark-trino-local-v0.1",
    "iceberg-polaris-spark-local-v0.1",
]


# ---------------------------------------------------------------------------
# write_fragment dispatch — None for stacks with no renderer
# ---------------------------------------------------------------------------

def test_write_fragment_returns_none_for_stable_udp_stack(tmp_path: Path):
    """udp-local-v0.2 is the stable stack — UDP's upstream compose ships
    every service it needs, so no fragment is required. The runner
    relies on the None return to skip overlay injection for stable
    installs."""
    result = scf.write_fragment("udp-local-v0.2", tmp_path, {})
    assert result is None
    # And critically: NO file should have been written.
    assert not (tmp_path / scf.FRAGMENT_FILENAME).exists()


def test_write_fragment_returns_none_for_unknown_stack(tmp_path: Path):
    result = scf.write_fragment("does-not-exist-stack", tmp_path, {})
    assert result is None
    assert not (tmp_path / scf.FRAGMENT_FILENAME).exists()


# ---------------------------------------------------------------------------
# write_fragment writes a valid YAML file for each candidate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stack_id", CANDIDATE_STACK_IDS)
def test_write_fragment_writes_file_for_candidate(stack_id: str, tmp_path: Path):
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    assert path.exists()
    assert path.name == scf.FRAGMENT_FILENAME
    assert path.parent == tmp_path
    # File must be non-empty (we wrote actual content, not an empty placeholder).
    assert path.stat().st_size > 0


@pytest.mark.parametrize("stack_id", CANDIDATE_STACK_IDS)
def test_written_yaml_parses_cleanly(stack_id: str, tmp_path: Path):
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    body = path.read_text(encoding="utf-8")
    # yaml.safe_load must not raise — broken YAML would break compose.
    doc = yaml.safe_load(body)
    assert isinstance(doc, dict)
    assert "services" in doc, "fragment must declare a services: map"
    assert isinstance(doc["services"], dict)
    # Modern compose v2 — no top-level `version:` key.
    assert "version" not in doc, (
        "compose v2 fragments must omit the legacy `version:` key"
    )


# ---------------------------------------------------------------------------
# Services in the rendered YAML match FRAGMENT_SERVICES
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stack_id", CANDIDATE_STACK_IDS)
def test_fragment_services_match_rendered_services(stack_id: str, tmp_path: Path):
    """The runner extends the `docker compose up -d <services>` argv with
    FRAGMENT_SERVICES[stack_id]. If those names don't actually exist as
    top-level keys in the rendered YAML, compose will reject the
    command with `no such service`. This test is the contract guard."""
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = doc["services"]
    expected_services = scf.FRAGMENT_SERVICES[stack_id]
    for svc_name in expected_services:
        assert svc_name in services, (
            f"FRAGMENT_SERVICES says '{svc_name}' is part of '{stack_id}' "
            f"but it's missing from the rendered YAML "
            f"(services present: {sorted(services.keys())})"
        )
    # Conversely: every service in the YAML should be in FRAGMENT_SERVICES
    # (otherwise the runner won't bring it up explicitly).
    for svc_name in services:
        assert svc_name in expected_services, (
            f"Rendered YAML has service '{svc_name}' but FRAGMENT_SERVICES "
            f"for '{stack_id}' doesn't list it — runner won't include it "
            f"in `docker compose up -d`."
        )


# ---------------------------------------------------------------------------
# Each service has the required keys: image, container_name, healthcheck
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stack_id", CANDIDATE_STACK_IDS)
def test_each_service_has_required_keys(stack_id: str, tmp_path: Path):
    """Every fragment service MUST declare:
      - image: (otherwise compose can't pull it)
      - container_name: (so the runner's logs and `docker exec` work)
      - healthcheck: (so downstream services can `depends_on:
        condition: service_healthy` against it)
    """
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    for svc_name, svc_def in doc["services"].items():
        assert isinstance(svc_def, dict), (
            f"service '{svc_name}' in '{stack_id}' is not a dict"
        )
        for key in ("image", "container_name", "healthcheck"):
            assert key in svc_def, (
                f"service '{svc_name}' in fragment for '{stack_id}' "
                f"is missing required key '{key}'"
            )


# ---------------------------------------------------------------------------
# Stack-specific service-name expectations (regression guard)
# ---------------------------------------------------------------------------

EXPECTED_SERVICES_PER_STACK = {
    # 2026-05-17 bug fix: udp-trino-local-v0.1 needs a Trino service
    # (UDP's upstream compose doesn't ship one).
    "udp-trino-local-v0.1":              {"trino"},
    # iceberg-nessie-trino: Nessie catalog + Trino query engine, both
    # missing from the upstream compose file.
    "iceberg-nessie-trino-local-v0.1":  {"nessie", "trino"},
    # v0.6.2 refactor 2026-05-17: HMS backing is MySQL (not Postgres) —
    # bitsondatadev/hive-metastore image is MySQL-only by design.
    "hudi-hms-spark-local-v0.1":        {"mysql-hms", "hive-metastore"},
    # delta-hms-spark-trino: HMS pair + Trino, but Hudi uses ONLY the
    # HMS pair (no Trino), so this stack gets its own renderer to avoid
    # bleeding Trino into the Hudi install.
    "delta-hms-spark-trino-local-v0.1": {"mysql-hms", "hive-metastore", "trino"},
    "iceberg-polaris-spark-local-v0.1": {"postgres-polaris", "polaris"},
}


@pytest.mark.parametrize("stack_id,expected", list(EXPECTED_SERVICES_PER_STACK.items()))
def test_specific_services_present_per_stack(stack_id: str, expected: set, tmp_path: Path):
    """Pin the exact service names the spec called for so a future
    rename of an internal helper can't silently change the contract."""
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert set(doc["services"].keys()) == expected


# ---------------------------------------------------------------------------
# Idempotency: writing twice overwrites cleanly
# ---------------------------------------------------------------------------

def test_write_fragment_is_idempotent(tmp_path: Path):
    """Re-running with the same install_dir overwrites the fragment
    atomically without leaving .tmp files behind."""
    p1 = scf.write_fragment("hudi-hms-spark-local-v0.1", tmp_path, {})
    p2 = scf.write_fragment("hudi-hms-spark-local-v0.1", tmp_path, {})
    assert p1 == p2
    assert p1.exists()
    # No leftover .tmp file from the atomic-write dance.
    assert list(tmp_path.glob("*.tmp")) == []


# ---------------------------------------------------------------------------
# Network attachment: external default network is declared on every fragment
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stack_id", CANDIDATE_STACK_IDS)
def test_fragment_attaches_to_default_network(stack_id: str, tmp_path: Path):
    """Every fragment service joins `default`. Per Codex P0 review
    (2026-05-17) the network is NO LONGER declared `external: true` —
    that required a pre-created network and broke first-time installs.
    The new contract: fragment references `default`, base compose
    creates it on `up -d`, and compose merges them transparently."""
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "networks" in doc, "fragment must declare networks: top-level"
    assert "default" in doc["networks"]
    # MUST NOT be external — compose would refuse to create it.
    network_spec = doc["networks"]["default"] or {}
    assert network_spec.get("external") is not True, (
        f"'{stack_id}' fragment declares default network external — "
        "this breaks fresh installs (Codex P0 2026-05-17)"
    )
    # Each service must be on the default network.
    for svc_name, svc_def in doc["services"].items():
        nets = svc_def.get("networks") or []
        assert "default" in nets, (
            f"service '{svc_name}' in '{stack_id}' is not attached to the "
            f"shared default network"
        )


# ---------------------------------------------------------------------------
# Postgres services use named volumes (not bind mounts) — idempotency
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stack_id", [
    "hudi-hms-spark-local-v0.1",
    "delta-hms-spark-trino-local-v0.1",
    "iceberg-polaris-spark-local-v0.1",
])
def test_postgres_services_use_named_volumes(stack_id: str, tmp_path: Path):
    """DB backing services (postgres-* OR mysql-*) must use named volumes —
    bind mounts to install_dir/data/ would leak across re-installs and
    cause permission grief on Windows + macOS.

    Refactored 2026-05-17: HMS stacks now use mysql-hms (bitsondatadev
    image is MySQL-only); polaris stack still uses postgres-polaris.
    Match BOTH service-name prefixes for the same volume-contract check.
    """
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    db_services = [s for s in doc["services"]
                   if s.startswith("postgres-") or s.startswith("mysql-")]
    assert db_services, f"expected a postgres-* or mysql-* service in '{stack_id}'"
    for svc in db_services:
        volumes = doc["services"][svc].get("volumes") or []
        assert volumes, f"DB backing service '{svc}' must declare a volume"
        for vol in volumes:
            assert not vol.startswith("./"), (
                f"'{svc}' in '{stack_id}' uses a bind mount: {vol}"
            )
            assert not vol.startswith("/"), (
                f"'{svc}' in '{stack_id}' uses an absolute bind mount: {vol}"
            )
    assert "volumes" in doc, (
        f"fragment for '{stack_id}' must declare top-level volumes: for "
        f"its named DB volume"
    )


# ---------------------------------------------------------------------------
# Module-level constants the runner imports
# ---------------------------------------------------------------------------

def test_fragment_filename_constant():
    assert scf.FRAGMENT_FILENAME == "docker-compose.fragment.yml"


def test_fragment_services_covers_all_renderers():
    """Every stack_id with a renderer MUST have a FRAGMENT_SERVICES
    entry so the runner knows which services to include in `up -d`."""
    for stack_id in scf._FRAGMENT_RENDERERS:
        assert stack_id in scf.FRAGMENT_SERVICES, (
            f"renderer for '{stack_id}' has no FRAGMENT_SERVICES entry"
        )
    # And vice versa — every FRAGMENT_SERVICES entry needs a renderer.
    for stack_id in scf.FRAGMENT_SERVICES:
        assert stack_id in scf._FRAGMENT_RENDERERS, (
            f"FRAGMENT_SERVICES entry '{stack_id}' has no renderer"
        )


# ---------------------------------------------------------------------------
# Env interpolation: password defaults can be overridden via env
# ---------------------------------------------------------------------------

def test_hms_fragment_uses_compose_interpolation_for_password(tmp_path: Path):
    """The HMS_DB_PASSWORD should be wired via compose's
    `${VAR:-default}` syntax so the operator can override it via the
    install's .env without re-rendering the fragment."""
    path = scf.write_fragment("hudi-hms-spark-local-v0.1", tmp_path, {})
    assert path is not None
    body = path.read_text(encoding="utf-8")
    assert "${HMS_DB_PASSWORD:-hive_password_pilot}" in body


def test_polaris_fragment_uses_compose_interpolation_for_password(tmp_path: Path):
    path = scf.write_fragment("iceberg-polaris-spark-local-v0.1", tmp_path, {})
    assert path is not None
    body = path.read_text(encoding="utf-8")
    assert "${POLARIS_DB_PASSWORD:-polaris_password_pilot}" in body


# ---------------------------------------------------------------------------
# Network name override via env
# ---------------------------------------------------------------------------

def test_default_network_has_no_external_or_name_override(tmp_path: Path):
    """Per Codex P0 fix (2026-05-17): the fragment defines `default: {}`
    (empty mapping) and lets `docker compose -f base -f fragment` merge
    the networks naturally. Explicit `name:` or `external: true` here
    would either (a) collide with the base's auto-named network or
    (b) fail to find the not-yet-existent network.

    LHS_DOCKER_NETWORK can still be set to influence the BASE compose
    project name; that's the right knob, not a fragment-level override.
    """
    # With env override — should NOT influence fragment networks block.
    path = scf.write_fragment(
        "iceberg-nessie-trino-local-v0.1",
        tmp_path,
        {"LHS_DOCKER_NETWORK": "my-custom-net"},
    )
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    net = doc["networks"]["default"] or {}
    assert net.get("external") is not True
    assert "name" not in net or not net.get("name"), (
        "fragment must NOT pin a network name — would conflict with base compose merge"
    )

    # Without env override — same behavior.
    path2 = scf.write_fragment(
        "iceberg-nessie-trino-local-v0.1",
        tmp_path / "fresh",
        {},
    )
    assert path2 is not None
    doc2 = yaml.safe_load(path2.read_text(encoding="utf-8"))
    net2 = doc2["networks"]["default"] or {}
    assert net2.get("external") is not True


# ---------------------------------------------------------------------------
# Trino service: present in all 3 Trino-using stacks; absent from the others
# ---------------------------------------------------------------------------

TRINO_STACK_IDS = [
    "udp-trino-local-v0.1",
    "iceberg-nessie-trino-local-v0.1",
    "delta-hms-spark-trino-local-v0.1",
]

NON_TRINO_STACK_IDS = [
    "hudi-hms-spark-local-v0.1",
    "iceberg-polaris-spark-local-v0.1",
]


@pytest.mark.parametrize("stack_id", TRINO_STACK_IDS)
def test_trino_service_present_in_trino_stacks(stack_id: str, tmp_path: Path):
    """Bug fix 2026-05-17: UDP's upstream docker-compose.yml does NOT
    ship a `trino` service definition. Every stack whose `start` step
    references trino must get a Trino service via the fragment."""
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "trino" in doc["services"], (
        f"'{stack_id}' must include a trino service "
        f"(present: {sorted(doc['services'].keys())})"
    )
    trino_svc = doc["services"]["trino"]
    assert trino_svc["image"].startswith("trinodb/trino:"), (
        f"trino service in '{stack_id}' has unexpected image: {trino_svc['image']}"
    )
    # The Trino UI / API container port 8080 must be published so the operator
    # can hit the web console. The HOST side is env-overridable
    # (${TRINO_HTTP_PORT:-8080}) so shared hosts can dodge a busy 8080 — so we
    # assert on the container port (:8080), not a fixed host:container literal.
    assert any(
        str(p).rstrip('"').endswith(":8080") for p in trino_svc.get("ports") or []
    ), f"trino in '{stack_id}' must publish container port 8080"


@pytest.mark.parametrize("stack_id", NON_TRINO_STACK_IDS)
def test_trino_service_absent_from_non_trino_stacks(stack_id: str, tmp_path: Path):
    """Hudi (Spark-only) and Polaris (Spark-only) stacks must NOT get a
    Trino service — that would burn ~3 GB of RAM for no reason and
    publish port 8080 they don't use."""
    path = scf.write_fragment(stack_id, tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "trino" not in doc["services"], (
        f"'{stack_id}' must NOT include a trino service "
        f"(present: {sorted(doc['services'].keys())})"
    )


def test_udp_trino_fragment_is_trino_only(tmp_path: Path):
    """udp-trino-local-v0.1 uses the upstream iceberg-rest catalog — its
    fragment supplies ONLY the missing Trino service (no Nessie, no HMS)."""
    path = scf.write_fragment("udp-trino-local-v0.1", tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert set(doc["services"].keys()) == {"trino"}


def test_nessie_fragment_includes_nessie_and_trino(tmp_path: Path):
    """iceberg-nessie-trino-local-v0.1 needs BOTH services from its
    fragment because UDP ships neither."""
    path = scf.write_fragment("iceberg-nessie-trino-local-v0.1", tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert set(doc["services"].keys()) == {"nessie", "trino"}


def test_delta_fragment_includes_hms_pair_and_trino(tmp_path: Path):
    """delta-hms-spark-trino-local-v0.1 needs HMS + MySQL backing + Trino
    — three services, all missing from UDP's upstream compose."""
    path = scf.write_fragment("delta-hms-spark-trino-local-v0.1", tmp_path, {})
    assert path is not None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert set(doc["services"].keys()) == {"mysql-hms", "hive-metastore", "trino"}


def test_trino_service_env_uses_compose_interpolation(tmp_path: Path):
    """JVM/memory caps must be wired via `${VAR:-default}` so the operator
    can tune via the install's .env without re-rendering the fragment."""
    path = scf.write_fragment("udp-trino-local-v0.1", tmp_path, {})
    assert path is not None
    body = path.read_text(encoding="utf-8")
    assert "${TRINO_JAVA_OPTS:-" in body
    assert "${TRINO_QUERY_MAX_MEMORY_PER_NODE:-1.5GB}" in body
    assert "${TRINO_QUERY_MAX_MEMORY:-1.5GB}" in body
