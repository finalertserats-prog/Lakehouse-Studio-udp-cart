# API Versioning

LakeHouse Studio's HTTP + WebSocket API supports a versioned path prefix
alongside the original un-versioned surface. The two are interchangeable
today; the versioned prefix is the forward-compatible contract.

## TL;DR

| Path shape          | Status                           | Use when                                  |
|---------------------|----------------------------------|-------------------------------------------|
| `/api/...`          | Active, default                  | Existing frontend, ad-hoc scripts          |
| `/api/v1/...`       | Active, alias of `/api/...`      | New integrations, external API consumers   |
| `/api/v2/...`       | Reserved (not implemented)       | Future major changes only                  |

Both `/api/auth/status` and `/api/v1/auth/status` return the same response.
Both `/api/cart/validate` and `/api/v1/cart/validate` accept the same body.
The WebSocket at `/api/installs/{id}/logs` is reachable as
`/api/v1/installs/{id}/logs` with identical behaviour.

## How it works

A single pure-ASGI middleware (`V1AliasMiddleware` in `backend/main.py`)
rewrites the `path` field of the incoming scope from `/api/v1/<rest>` to
`/api/<rest>` BEFORE Starlette's router matches. The middleware covers
both `http` and `websocket` scope types — `@app.middleware("http")` alone
would silently skip WebSocket handshakes, so the alias is implemented as
a class-based ASGI middleware that handles both.

Consequences:

- **Zero duplicate routes.** `len(app.routes)` is unchanged. Each handler
  is registered once under its canonical un-versioned path.
- **No HTTP redirects.** Clients never see a 3xx; the rewrite is internal
  to the ASGI scope. An unknown `/api/v1/...` path returns FastAPI's
  standard `{"detail": "Not Found"}` 404, never a redirect to `/api/`.
- **Single source of truth for OpenAPI.** `/openapi.json` documents the
  canonical un-versioned paths only. The v1 surface mirrors them
  transparently and is not separately enumerated.
- **WebSocket parity.** The scope rewrite applies to WebSocket handshakes
  too, so `wss://.../api/v1/installs/{id}/logs` reaches the same handler
  as the un-versioned path.

## Stability and deprecation

- `/api/v1/` is the **forward-compatible contract**. Once a request shape
  or response body is published under `/api/v1/`, any breaking change
  lands at `/api/v2/` — never as a silent change to v1.
- `/api/...` (un-versioned) **will eventually deprecate.** No timeline is
  set yet; the announcement will land in `CHANGELOG.md` with a reasonable
  notice period (target: two minor releases warning before removal).
  Until then it remains a permanent alias of `/api/v1/`.
- Authentication, rate limits, and authorization apply identically to
  both paths — the middleware rewrites the scope before any auth
  dependency runs, so the same `Depends(...)` chain protects both.

## When v2 lands

Adding `/api/v2/` will follow the same pattern, but routes that change
shape under v2 will be registered as **separate handlers** rather than
as aliases. The middleware will then:

1. Continue to alias `/api/v1/<rest>` → `/api/<rest>` for routes that
   did not change shape between v1 and v2.
2. Route `/api/v2/<rest>` to the new v2 handlers explicitly (no
   rewrite), so v2-shape responses come from dedicated code paths.

In other words: the middleware is the cheap path for "no semantic
change, just version the URL." Real version-skewed behaviour will use
a sub-router under `/api/v2/`.

## Migration guide for API consumers

- **Existing scripts hitting `/api/...`** — no change required today.
  Plan to update to `/api/v1/...` before the deprecation window opens.
- **New integrations** — write against `/api/v1/...` from day one.
- **CI / monitoring probes** — pin to `/api/v1/...` so the deprecation
  of un-versioned paths does not silently break health checks.

## Verification

```bash
# Canonical and aliased paths return identical bodies
curl -s http://127.0.0.1:7878/api/auth/status
curl -s http://127.0.0.1:7878/api/v1/auth/status

# Unknown v1 path -> standard 404, no redirect
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:7878/api/v1/nonexistent

# OpenAPI doc reflects un-versioned paths only
curl -s http://127.0.0.1:7878/openapi.json | jq '.paths | keys[] | select(startswith("/api/"))'
```

The middleware ships with regression tests in
`tests/test_api_versioning.py` covering GET parity, POST parity, 404
behaviour, route-count invariance, OpenAPI invariance, and WebSocket
aliasing.
