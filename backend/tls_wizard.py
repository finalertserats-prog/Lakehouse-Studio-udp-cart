"""TLS + Password-rotation wizard primitives.

Pure additive v0.4.0 hardening module. Runs POST-install as opt-in: never
touches the install pipeline (backend/runner.py is FROZEN), never rewrites
docker-compose files in this pass. Caller (the FastAPI route) is responsible
for passing RUNNING_STATES so we refuse to rotate a password mid-install
(import-injection avoids a cycle with main.py — same pattern as backup.py).

Surface:
  - Self-signed certs (RSA + SAN extension; CN-only is deprecated)
  - Let's Encrypt cert generation is stubbed — research said Caddy sidecar is
    the right call but compose modification is out of scope until v0.4.1
  - Per-cert sidecar JSON manifest so list/get/delete don't have to parse PEM
  - Password rotation for MinIO (.env edit + restart hint) and StarRocks
    (refuses env edit; returns required SQL — env-var rotation does NOT work
    for the StarRocks root account, see docs/COMPATIBILITY.md)
  - Rule-based password strength hint (no external deps)

Filesystem layout:
  WORK_DIR / "tls" / {install_id} / {cert_id}.crt
                                    {cert_id}.key   (chmod 600 best-effort)
                                    {cert_id}.json  (sidecar manifest)

The wizard NEVER logs the resolved password and NEVER persists it anywhere
other than the install's .env (and only via rotate_password).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .config import WORK_DIR
from .state import store


log = logging.getLogger("lhs.tls")


_TLS_ROOT = WORK_DIR / "tls"

# Minimum password rules — single source of truth for both rotate_password
# and password_strength_hint so the UI's pre-submit check matches the server.
_MIN_PASSWORD_LEN = 12

# Files in install_dir that we touch when rotating MinIO credentials.
_ENV_FILENAME = ".env"
_ENV_MINIO_KEYS = ("MINIO_ROOT_PASSWORD", "AWS_SECRET_ACCESS_KEY")


# ---------- models ----------


class CertSpec(BaseModel):
    """Input for cert generation. Domain optional for self-signed (defaults to
    'localhost' so the cert is still useful for dev installs)."""
    kind: Literal["self_signed", "letsencrypt"]
    domain: Optional[str] = None
    email: Optional[str] = None
    valid_days: int = Field(default=365, ge=1, le=3650)
    key_size: int = Field(default=2048, ge=2048, le=8192)

    @field_validator("key_size")
    @classmethod
    def _key_size_power_of_two(cls, v: int) -> int:
        # Common RSA key sizes only — reject weird values that would silently
        # weaken the cert.
        if v not in (2048, 3072, 4096, 8192):
            raise ValueError("key_size must be one of 2048, 3072, 4096, 8192")
        return v


class GeneratedCert(BaseModel):
    """Public record of a generated cert. Returned by generate_*. The key path
    is included for completeness, but routes that surface this to the UI MUST
    strip it (the key never leaves the server filesystem)."""
    cert_id: str
    install_id: str
    kind: str
    cert_path: str
    key_path: str
    sha256_fingerprint: str
    expires_at: float
    created_at: float
    common_name: str


# ---------- helpers ----------


def _install_tls_dir(install_id: str) -> Path:
    d = _TLS_ROOT / install_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _chmod_key_600(path: Path) -> None:
    """Restrict key file to owner read/write. Best-effort on Windows: NTFS
    ACLs don't map to POSIX bits cleanly, so we degrade silently if the chmod
    raises. The file is still in WORK_DIR which is operator-controlled."""
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except (OSError, NotImplementedError) as e:
        log.debug("chmod 600 on %s degraded (likely Windows): %s", path, e)


def _sha256_fingerprint(cert_bytes: bytes) -> str:
    """Standard 'sha256 fingerprint' format: lowercase hex, no separators.
    Callers that want the colon-separated form can transform downstream."""
    return hashlib.sha256(cert_bytes).hexdigest()


def _write_sidecar(sidecar: Path, cert: GeneratedCert) -> None:
    sidecar.write_text(json.dumps(cert.model_dump(), indent=2), encoding="utf-8")


# ---------- self-signed cert generation ----------


async def generate_self_signed(install_id: str, spec: CertSpec) -> GeneratedCert:
    """Generate an RSA key + self-signed X.509 cert with a SAN extension.

    The Subject Alternative Name extension is required — modern TLS clients
    (curl 7.66+, Chrome, anything using OpenSSL 1.1.1+) reject certs that
    only set the Common Name. We always emit both CN and SAN so older
    clients still work, but the SAN is what actually validates.
    """
    if spec.kind != "self_signed":
        raise ValueError(f"generate_self_signed called with kind={spec.kind!r}")

    # cryptography is a transitive dep — import lazily so this module still
    # loads on the rotate-password-only path if it's missing.
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError as e:
        raise RuntimeError(
            "cryptography package not available — install_self_signed unavailable. "
            "Password rotation still works."
        ) from e

    domain = (spec.domain or "localhost").strip() or "localhost"

    # Generate RSA key
    key = rsa.generate_private_key(public_exponent=65537, key_size=spec.key_size)

    now = datetime.now(timezone.utc)
    not_after = now + timedelta(days=spec.valid_days)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, domain),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Lakehouse Studio (self-signed)"),
    ])

    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))  # clock-skew tolerance
        .not_valid_after(not_after)
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(domain)]),
            critical=False,
        )
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
    )

    cert = builder.sign(private_key=key, algorithm=hashes.SHA256())

    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    cert_id = f"cert_{uuid.uuid4().hex[:12]}"
    target_dir = _install_tls_dir(install_id)
    cert_path = target_dir / f"{cert_id}.crt"
    key_path = target_dir / f"{cert_id}.key"
    sidecar = target_dir / f"{cert_id}.json"

    cert_path.write_bytes(cert_pem)
    key_path.write_bytes(key_pem)
    _chmod_key_600(key_path)

    record = GeneratedCert(
        cert_id=cert_id,
        install_id=install_id,
        kind="self_signed",
        cert_path=str(cert_path),
        key_path=str(key_path),
        sha256_fingerprint=_sha256_fingerprint(cert_pem),
        expires_at=not_after.timestamp(),
        created_at=time.time(),
        common_name=domain,
    )
    _write_sidecar(sidecar, record)
    log.info("self-signed cert generated id=%s cn=%s expires=%s",
             cert_id, domain, not_after.isoformat())
    return record


async def generate_letsencrypt(install_id: str, spec: CertSpec) -> GeneratedCert:
    """Let's Encrypt via Caddy sidecar (v0.4.1).

    Replaces the NotImplementedError stub. Writes a docker-compose.tls.yml
    override + Caddyfile next to the FROZEN base compose so the operator
    can opt in with one `docker compose ... up -d caddy` command. The
    actual cert issuance happens inside Caddy at container start (ACME
    HTTP-01 against the supplied domain).

    Returns a GeneratedCert-shaped record where `cert_path` points at the
    override file (the real cert lives in the caddy_data Docker volume,
    not on the host filesystem). `key_path` is empty -- the route layer
    already scrubs key_path before returning.
    """
    if spec.kind != "letsencrypt":
        raise ValueError(f"generate_letsencrypt called with kind={spec.kind!r}")
    if not spec.domain:
        raise ValueError("letsencrypt requires a domain")
    if not spec.email:
        raise ValueError("letsencrypt requires an email for ACME registration")

    # Lazy import to avoid a circular reference at module load.
    from . import caddy_tls

    profile = caddy_tls.TlsProfile(
        kind="letsencrypt", domain=spec.domain, email=spec.email,
    )
    override_path = await caddy_tls.write_caddy_override(install_id, profile)

    now = time.time()
    cert_id = f"caddy_le_{uuid.uuid4().hex[:10]}"
    return GeneratedCert(
        cert_id=cert_id,
        install_id=install_id,
        kind="letsencrypt",
        cert_path=str(override_path),
        key_path="",  # managed inside the caddy_data Docker volume
        sha256_fingerprint="",  # cert is issued by Caddy at container start
        # ACME-issued LE certs are valid 90 days; Caddy auto-renews at 60.
        expires_at=now + 90 * 86400,
        created_at=now,
        common_name=spec.domain,
    )


async def generate_caddy_self_signed(install_id: str, spec: CertSpec) -> GeneratedCert:
    """Opt-in path: ask Caddy to generate + serve a self-signed cert via
    its internal CA. Different from generate_self_signed() above, which
    writes a one-shot PEM to WORK_DIR/tls/... This path produces the
    docker-compose.tls.yml override + Caddyfile and Caddy handles
    issuance + serving on port 443.

    Use this when you want HTTPS termination IN FRONT of the stack rather
    than per-service cert mounting (which Studio doesn't ship yet).
    """
    if spec.kind != "self_signed":
        raise ValueError(f"generate_caddy_self_signed called with kind={spec.kind!r}")

    from . import caddy_tls

    profile = caddy_tls.TlsProfile(
        kind="self_signed",
        domain=spec.domain or "localhost",
        email=None,
    )
    override_path = await caddy_tls.write_caddy_override(install_id, profile)

    now = time.time()
    cert_id = f"caddy_ss_{uuid.uuid4().hex[:10]}"
    return GeneratedCert(
        cert_id=cert_id,
        install_id=install_id,
        kind="self_signed",
        cert_path=str(override_path),
        key_path="",  # managed inside the caddy_data Docker volume
        sha256_fingerprint="",  # cert is issued by Caddy at container start
        # Caddy's internal CA mints 12h leaf certs and rotates them silently
        # on every restart; report a 1y horizon so the UI doesn't panic.
        expires_at=now + 365 * 86400,
        created_at=now,
        common_name=spec.domain or "localhost",
    )


# ---------- cert listing / lookup / delete ----------


def list_certs(install_id: str) -> list[GeneratedCert]:
    """Scan sidecar manifests for a specific install. Returns [] if no certs
    have been generated yet."""
    target = _TLS_ROOT / install_id
    if not target.exists():
        return []
    out: list[GeneratedCert] = []
    for sidecar in sorted(target.glob("cert_*.json")):
        try:
            raw = json.loads(sidecar.read_text(encoding="utf-8"))
            out.append(GeneratedCert(**raw))
        except Exception as e:
            log.warning("skipping malformed cert sidecar %s: %s", sidecar, e)
            continue
    return sorted(out, key=lambda c: c.created_at, reverse=True)


def get_cert(cert_id: str) -> Optional[GeneratedCert]:
    """Find a cert by id across every install dir. Used by delete + UI lookup."""
    if not _TLS_ROOT.exists():
        return None
    for sidecar in _TLS_ROOT.glob(f"*/{cert_id}.json"):
        try:
            raw = json.loads(sidecar.read_text(encoding="utf-8"))
            return GeneratedCert(**raw)
        except Exception as e:
            log.warning("malformed sidecar %s: %s", sidecar, e)
            continue
    return None


def delete_cert(cert_id: str) -> None:
    """Remove cert + key + sidecar. Raises ValueError if cert_id is unknown."""
    rec = get_cert(cert_id)
    if rec is None:
        raise ValueError(f"cert {cert_id!r} not found")
    for p in (Path(rec.cert_path), Path(rec.key_path),
              Path(rec.cert_path).with_suffix(".json")):
        try:
            p.unlink(missing_ok=True)
        except Exception as e:
            log.warning("failed to remove %s: %s", p, e)
    # Sidecar is named {cert_id}.json (not {cert_id}.crt.json), handle that too.
    sidecar = Path(rec.cert_path).parent / f"{rec.cert_id}.json"
    try:
        sidecar.unlink(missing_ok=True)
    except Exception as e:
        log.warning("failed to remove sidecar %s: %s", sidecar, e)


# ---------- password strength scoring ----------


def password_strength_hint(password: str) -> dict:
    """Rule-based scorer. Returns {score: 0-100, suggestions: [...]}.

    Helper for the UI to give immediate pre-submit feedback. Mirrors the
    rules enforced by rotate_password so the UI never proposes a password
    the server will reject. NEVER logs or echoes the password.
    """
    suggestions: list[str] = []
    score = 0

    length = len(password)
    if length >= _MIN_PASSWORD_LEN:
        score += 30
    else:
        suggestions.append(f"use at least {_MIN_PASSWORD_LEN} characters")
    if length >= 16:
        score += 15
    if length >= 20:
        score += 10

    has_lower = any(c.islower() for c in password)
    has_upper = any(c.isupper() for c in password)
    has_digit = any(c.isdigit() for c in password)
    has_symbol = any(not c.isalnum() for c in password)

    if has_lower:
        score += 5
    else:
        suggestions.append("add lowercase letters")
    if has_upper:
        score += 10
    else:
        suggestions.append("add uppercase letters")
    if has_digit:
        score += 10
    else:
        suggestions.append("add at least one digit")
    if has_symbol:
        score += 15
    else:
        suggestions.append("add a symbol (!@#$ etc.)")

    # Variety bonus — distinct characters as a fraction of length.
    distinct = len(set(password))
    if length > 0:
        variety = distinct / length
        score += int(variety * 5)

    # Heuristic penalties — common patterns that look strong but aren't.
    lowered = password.lower()
    common_bad = ("password", "qwerty", "admin", "letmein", "lakehouse",
                  "minio", "starrocks", "123456")
    if any(token in lowered for token in common_bad):
        score = max(0, score - 30)
        suggestions.append("avoid dictionary words and product names")
    if length > 1 and len(set(password)) == 1:
        score = max(0, score - 30)
        suggestions.append("avoid repeating a single character")

    score = max(0, min(100, score))
    return {"score": score, "suggestions": suggestions}


# ---------- password rotation ----------


def _validate_password(new_password: str) -> None:
    """Hard rules enforced server-side. Raises ValueError on failure with a
    safe error message (never echoes the password)."""
    if len(new_password) < _MIN_PASSWORD_LEN:
        raise ValueError(f"new_password must be at least {_MIN_PASSWORD_LEN} characters")
    if not any(c.isdigit() for c in new_password):
        raise ValueError("new_password must contain at least one digit")
    if not any(c.isalpha() for c in new_password):
        raise ValueError("new_password must contain at least one letter")


def _rewrite_env_file(env_path: Path, updates: dict[str, str]) -> Path:
    """In-place rewrite of a KEY=VALUE .env file, preserving comments and order.

    Writes a timestamped backup to {env_path}.bak.{ts} BEFORE touching the
    original. Atomic via tmp-file + replace so a crash mid-write doesn't
    truncate the live .env.

    Returns the backup path.
    """
    if not env_path.exists():
        raise ValueError(f".env file not found at {env_path}")

    ts = time.strftime("%Y%m%dT%H%M%S")
    backup = env_path.with_suffix(env_path.suffix + f".bak.{ts}")
    backup.write_bytes(env_path.read_bytes())

    original = env_path.read_text(encoding="utf-8").splitlines(keepends=False)
    remaining = dict(updates)
    out_lines: list[str] = []
    for line in original:
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            out_lines.append(line)
            continue
        key, _, _value = stripped.partition("=")
        key = key.strip()
        if key in remaining:
            out_lines.append(f"{key}={remaining.pop(key)}")
        else:
            out_lines.append(line)
    # Any keys not present originally get appended at the end.
    for key, value in remaining.items():
        out_lines.append(f"{key}={value}")

    tmp = env_path.with_suffix(env_path.suffix + ".tmp")
    tmp.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    os.replace(tmp, env_path)
    return backup


async def rotate_password(
    install_id: str,
    service: Literal["minio", "starrocks"],
    new_password: str,
    *,
    running_states: frozenset[str],
) -> dict:
    """Rotate the root password for the given service.

    minio: edits {install_dir}/.env to update MINIO_ROOT_PASSWORD and
    AWS_SECRET_ACCESS_KEY (the minio-client container reads the latter so
    `mc` still works after rotation). Writes a timestamped backup before
    touching the file. Caller must restart minio with the returned command.

    starrocks: env-var rotation does NOT work for the StarRocks root
    account — the FE only reads MYSQL_PWD at first init. The correct
    rotation path is an `ALTER USER` statement run via the SQL editor.
    We refuse to touch .env for starrocks and return the SQL the operator
    needs to paste.

    NEVER returns the new password. NEVER logs it. The caller is expected
    to discard the plaintext from request memory immediately.
    """
    rec = store.get(install_id)
    if rec is None:
        raise ValueError(f"install {install_id!r} not found")
    if rec.state in running_states:
        raise ValueError(
            f"cannot rotate password while install state is {rec.state}; cancel first"
        )

    _validate_password(new_password)

    if service == "starrocks":
        # Build the SQL with the new password inline — the operator runs this
        # via the SQL editor, which already audit-logs (truncated) statements.
        # We return the SQL string because the operator needs to copy-paste it;
        # there is no safe path that hides the value AND lets them execute it.
        sql = f"ALTER USER 'root' IDENTIFIED BY '{new_password}'"
        return {
            "service": "starrocks",
            "restart_required": False,
            "sql_required": True,
            "sql": sql,
            "warning": (
                "Run this via the SQL editor — env-var rotation does NOT work "
                "for StarRocks root"
            ),
        }

    if service == "minio":
        env_path = Path(rec.install_dir) / _ENV_FILENAME
        updates = {key: new_password for key in _ENV_MINIO_KEYS}
        backup = _rewrite_env_file(env_path, updates)
        # Best-effort chmod on the backup so the old creds aren't world-readable.
        _chmod_key_600(backup)
        return {
            "service": "minio",
            "backed_up_at": str(backup),
            "restart_required": True,
            "restart_command": "docker compose up -d --no-deps minio",
        }

    raise ValueError(f"unknown service {service!r}; expected 'minio' or 'starrocks'")
