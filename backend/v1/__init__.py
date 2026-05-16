"""Starter scaffolds for the v1.0 architecture.

This package holds interface designs and migration scripts only. Nothing here
is wired into the running app. Switching v1.0 on requires explicit work in
``backend/main.py`` to flip the imports.

What lives here
---------------
- ``executor_interface``  — ``Executor`` protocol that abstracts "how do we
  drive a stack" (currently inline subprocess calls in ``runner.py``,
  ``health.py``, ``backup.py``). Includes a reference ``LocalDockerExecutor``
  plus stubs for ``KubernetesExecutor`` and ``SshAgentExecutor`` (future Go
  agent over gRPC).
- ``multi_tenant_schema`` — SQLite schema (CREATE TABLE strings, no ORM) for
  tenants/users/roles/installs/audit_log, plus a one-shot migration script
  that bulk-loads the current ``state.json`` into a single default tenant.
- ``rbac``                — Role / Permission model and a stub FastAPI
  dependency. Permissive in scaffold; documents the future enforcement.
- ``proto/agent.proto``   — gRPC service contract for the future Go agent
  that runs on customer infrastructure (control plane → agent, outbound
  mTLS).

Migration order (current → v1.0)
--------------------------------
1. Adopt the ``Executor`` abstraction. Route every shell-out through
   ``LocalDockerExecutor`` (no behaviour change).
2. Add RBAC enforcement. Flip ``rbac_check`` from always-True to real.
3. Migrate state.json to SQLite (``migrate_from_json``). Single "default"
   tenant for backward compat.
4. Add the ``KubernetesExecutor`` so installs can target a K8s cluster.
5. Add the ``SshAgentExecutor`` + ship the Go agent binary.

Nothing in this package should be imported by any module outside
``backend/v1/``.
"""
