"""Secret redaction for log lines and command strings.

Used in two places: streamed subprocess logs (live + persisted to evidence)
and the `$ cmd` echo we print before invoking each subprocess. Better to
overredact than to leak a credential.
"""
from __future__ import annotations
import re

MASK = "********"

# Keys whose VALUE must be hidden. Add to this set defensively.
SECRET_KEYS: frozenset[str] = frozenset({
    "MINIO_ROOT_PASSWORD",
    "MINIO_SECRET_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "STARROCKS_ROOT_PASSWORD",
    "POSTGRES_PASSWORD",
    "DATABASE_PASSWORD",
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "API_KEY",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "TOKEN",
    "PRIVATE_KEY",
    "ACCESS_KEY",
})

_SECRET_ALT = "|".join(re.escape(k) for k in sorted(SECRET_KEYS, key=len, reverse=True))

# KEY=value, KEY = value, KEY: value, --key value, --key=value
# Captures the leading key/separator so we can keep it, masks the value.
_PATTERNS: list[tuple[re.Pattern, str]] = [
    # KEY=value or KEY = value (env-file / shell style). Mask everything to whitespace/quote/end.
    (re.compile(rf'(?i)\b({_SECRET_ALT})\s*=\s*[^\s"\'`;|&\n\r]+'), rf"\1=" + MASK),
    # "KEY": "value" (JSON)
    (re.compile(rf'(?i)"({_SECRET_ALT})"\s*:\s*"[^"]*"'), rf'"\1": "{MASK}"'),
    # KEY: value (YAML / log lines)
    (re.compile(rf'(?i)\b({_SECRET_ALT})\s*:\s*[^\s,;\n\r]+'), rf"\1: " + MASK),
    # --key value or --key=value (CLI flags). Match the value after = or space.
    (re.compile(rf'(?i)(--[a-z0-9-]*(?:{_SECRET_ALT.lower()})[a-z0-9-]*)(?:=|\s+)\S+'),
     rf"\1=" + MASK),
    # URLs with embedded credentials: scheme://user:secret@host
    (re.compile(r'([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:@/]+):([^\s@/]+)@'), rf"\1:{MASK}@"),
    # AWS access key IDs (AKIA…) and long bearer/JWT-looking blobs
    (re.compile(r'\b(AKIA|ASIA)[A-Z0-9]{16,}\b'), MASK),
    # Bearer tokens
    (re.compile(r'(?i)\b(bearer)\s+\S+'), rf"\1 {MASK}"),
]


def redact(text: str) -> str:
    if not text:
        return text
    out = text
    for pat, repl in _PATTERNS:
        out = pat.sub(repl, out)
    return out


def safe_env_key(key: str) -> bool:
    """A safe .env key: starts with letter/underscore, then alnum/underscore. Bounded length."""
    if not (0 < len(key) <= 128):
        return False
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key))


def safe_env_value(value: str) -> tuple[bool, str]:
    """Return (ok, reason). Reject anything that could break docker-compose's env_file parser
    or escape from single-quotes when sourced by bash."""
    if not isinstance(value, str):
        return False, "value must be a string"
    if len(value) > 4096:
        return False, "value too long (>4096)"
    bad = {"\n": "newline", "\r": "carriage return", "\x00": "null byte"}
    for ch, name in bad.items():
        if ch in value:
            return False, f"value contains {name}"
    return True, ""


def sanitize_env_overrides(raw: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    """Filter the user-supplied env_overrides; return (clean, rejections)."""
    clean: dict[str, str] = {}
    rejections: list[str] = []
    for k, v in raw.items():
        if not safe_env_key(k):
            rejections.append(f"{k!r}: invalid key (must match [A-Za-z_][A-Za-z0-9_]*)")
            continue
        ok, why = safe_env_value(str(v))
        if not ok:
            rejections.append(f"{k}: {why}")
            continue
        clean[k] = str(v)
    return clean, rejections


def quote_env_value(value: str) -> str:
    """Single-quote a value for safe inclusion in a .env file consumed by bash/compose.

    Pre-condition: safe_env_value(value)[0] is True (no newlines).
    """
    # Bash single-quote escape: end quote, escaped quote, start quote.
    escaped = value.replace("'", "'\\''")
    return f"'{escaped}'"
