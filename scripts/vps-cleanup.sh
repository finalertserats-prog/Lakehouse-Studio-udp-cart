#!/usr/bin/env bash
# Lakehouse Studio — VPS cleanup
# ================================
# Removes ONLY Studio-created Docker artifacts (containers, networks,
# volumes) from the Finalert VPS so leftover state from failed install
# attempts doesn't accumulate and steal memory/disk from the other
# applications running on that box.
#
# HARD GATE: This script refuses to run on any host other than the
# Finalert VPS (hostname == srv1541349). The check is the first thing
# the script does — there is no flag to bypass it.
#
# Allowlist-only: the script targets containers/volumes by EXPLICIT
# name pattern. It never calls `docker system prune` against the whole
# daemon, never wipes arbitrary volumes, and never touches any
# container/volume that doesn't match the Studio naming convention.
#
# Modes:
#   (default)            Dry-run report. Counts and lists everything
#                        that WOULD be removed. Touches nothing.
#   --apply              Actually remove the matched containers + networks.
#                        Volumes are NOT touched in this mode.
#   --apply --with-volumes
#                        Also remove Studio's named volumes. DESTRUCTIVE:
#                        wipes MinIO buckets, HMS metadata, Nessie /
#                        Polaris catalog state, Prometheus/Grafana data.
#                        Requires interactive y/N confirmation.
#
# Patterns matched (allowlist):
#   Containers:  ^udp-     (base UDP stack — minio/starrocks/spark/etc)
#                ^lhs-     (Studio overlays — airflow/dagster/caddy/etc)
#   Volumes:     udp-mysql-hms-data, udp-postgres-polaris-data,
#                spark_jdbc_jars, airflow-pgdata, dagster-pgdata,
#                prometheus-data, grafana-data, loki-data,
#                and any volume whose name starts with `udp-` or `lhs-`.
#   Networks:    any network whose name contains `udp` or `lhs` AND has
#                no containers attached (dangling only).
#
# Things this script will NEVER touch:
#   * Containers / volumes / networks NOT matching the patterns above.
#   * Docker images (use `docker image prune` manually if you want that).
#   * Build cache (use `docker buildx prune` manually).
#   * Any running container OTHER than the Studio ones above.
#
# Usage on the VPS:
#   bash scripts/vps-cleanup.sh                # report only (dry-run)
#   bash scripts/vps-cleanup.sh --apply        # remove containers + networks
#   bash scripts/vps-cleanup.sh --apply --with-volumes   # also wipe data volumes

set -euo pipefail

# ---------------------------------------------------------------------------
# 1. Hard gate — Finalert VPS hostname check
# ---------------------------------------------------------------------------
EXPECTED_HOSTNAME="srv1541349"
ACTUAL_HOSTNAME="$(hostname)"

if [[ "${ACTUAL_HOSTNAME}" != "${EXPECTED_HOSTNAME}" ]]; then
  echo "ERROR: vps-cleanup.sh is gated to the Finalert VPS only." >&2
  echo "       expected hostname: ${EXPECTED_HOSTNAME}" >&2
  echo "       this host:         ${ACTUAL_HOSTNAME}" >&2
  echo "" >&2
  echo "       Refusing to run. This script intentionally has no override." >&2
  echo "       If you need to clean a different host, write a separate" >&2
  echo "       script with that host's allowlist + gate." >&2
  exit 2
fi

# ---------------------------------------------------------------------------
# 2. Parse flags
# ---------------------------------------------------------------------------
APPLY=0
WITH_VOLUMES=0
for arg in "$@"; do
  case "$arg" in
    --apply)         APPLY=1 ;;
    --with-volumes)  WITH_VOLUMES=1 ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      echo "ERROR: unknown flag: $arg" >&2
      echo "       see: bash scripts/vps-cleanup.sh --help" >&2
      exit 2
      ;;
  esac
done

if [[ "${WITH_VOLUMES}" -eq 1 && "${APPLY}" -eq 0 ]]; then
  echo "NOTE: --with-volumes has no effect in dry-run mode (it's a report)." >&2
fi

# ---------------------------------------------------------------------------
# 3. Pre-flight — confirm docker is reachable
# ---------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found in PATH on ${ACTUAL_HOSTNAME}" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: docker daemon unreachable (is the user in the docker group?)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 4. Collect matches — pure read, nothing destructive yet
# ---------------------------------------------------------------------------
echo "=========================================================================="
echo "Lakehouse Studio — VPS cleanup report"
echo "Host:    ${ACTUAL_HOSTNAME}"
echo "Mode:    $([[ ${APPLY} -eq 1 ]] && echo APPLY || echo dry-run)"
echo "Volumes: $([[ ${WITH_VOLUMES} -eq 1 ]] && echo INCLUDED || echo skipped)"
echo "Time:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=========================================================================="

# Containers matching ^udp- or ^lhs- (running OR stopped)
mapfile -t STUDIO_CONTAINERS < <(
  docker ps -a --format '{{.Names}}' \
    | grep -E '^(udp|lhs)-' || true
)

# Networks: dangling networks (no containers attached) whose name contains udp or lhs
mapfile -t STUDIO_NETWORKS < <(
  docker network ls --format '{{.Name}}' \
    | grep -iE '(udp|lhs)' \
    | while read -r net; do
        # bridge / host / none are baked in — never touch them
        case "$net" in bridge|host|none) continue ;; esac
        # Check attached container count (jq-free)
        attached=$(docker network inspect "$net" \
          --format '{{len .Containers}}' 2>/dev/null || echo "?")
        if [[ "$attached" == "0" ]]; then
          echo "$net"
        fi
      done
)

# Volumes — strict allowlist by exact name OR udp-/lhs- prefix.
# Per-install compose-prefixed volumes (e.g. `udp_minio-data`) also caught
# by the prefix match.
STUDIO_VOLUME_EXACT=(
  "spark_jdbc_jars"
  "airflow-pgdata"
  "dagster-pgdata"
  "prometheus-data"
  "grafana-data"
  "loki-data"
)
mapfile -t STUDIO_VOLUMES < <(
  docker volume ls --format '{{.Name}}' \
    | while read -r vol; do
        # Prefix match (udp- / lhs- / udp_ / lhs_)
        case "$vol" in
          udp-*|lhs-*|udp_*|lhs_*) echo "$vol"; continue ;;
        esac
        # Exact-name allowlist
        for exact in "${STUDIO_VOLUME_EXACT[@]}"; do
          if [[ "$vol" == "$exact" ]]; then
            echo "$vol"
            break
          fi
        done
      done
)

# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------
echo
echo "Containers matched (${#STUDIO_CONTAINERS[@]}):"
if [[ ${#STUDIO_CONTAINERS[@]} -eq 0 ]]; then
  echo "  (none)"
else
  for c in "${STUDIO_CONTAINERS[@]}"; do echo "  - $c"; done
fi

echo
echo "Dangling networks matched (${#STUDIO_NETWORKS[@]}):"
if [[ ${#STUDIO_NETWORKS[@]} -eq 0 ]]; then
  echo "  (none)"
else
  for n in "${STUDIO_NETWORKS[@]}"; do echo "  - $n"; done
fi

echo
echo "Volumes matched (${#STUDIO_VOLUMES[@]}):"
if [[ ${#STUDIO_VOLUMES[@]} -eq 0 ]]; then
  echo "  (none)"
else
  for v in "${STUDIO_VOLUMES[@]}"; do echo "  - $v"; done
fi

# Disk usage snapshot — useful for both report and post-clean delta
echo
echo "Docker disk usage (before any action):"
docker system df

# ---------------------------------------------------------------------------
# 6. Apply (only if --apply was passed)
# ---------------------------------------------------------------------------
if [[ ${APPLY} -eq 0 ]]; then
  echo
  echo "=========================================================================="
  echo "Dry-run complete. Nothing was removed."
  echo "Re-run with --apply to remove the listed containers + networks."
  echo "Add --with-volumes to ALSO wipe the listed volumes (destructive)."
  echo "=========================================================================="
  exit 0
fi

# Confirm if --with-volumes
if [[ ${WITH_VOLUMES} -eq 1 ]]; then
  echo
  echo "WARNING: --with-volumes will DELETE the volumes listed above."
  echo "         This wipes MinIO buckets, HMS metadata, Nessie/Polaris"
  echo "         catalog state, and observability data. Lake data is GONE."
  read -r -p "Type 'YES' to proceed: " confirm
  if [[ "$confirm" != "YES" ]]; then
    echo "Aborted by user. No changes made."
    exit 0
  fi
fi

# 6a. Remove containers (force; -v on docker rm releases anon volumes only)
removed_containers=0
for c in "${STUDIO_CONTAINERS[@]}"; do
  if docker rm -f "$c" >/dev/null 2>&1; then
    echo "  removed container: $c"
    removed_containers=$((removed_containers + 1))
  else
    echo "  FAILED to remove container: $c" >&2
  fi
done

# 6b. Remove dangling networks
removed_networks=0
for n in "${STUDIO_NETWORKS[@]}"; do
  if docker network rm "$n" >/dev/null 2>&1; then
    echo "  removed network: $n"
    removed_networks=$((removed_networks + 1))
  else
    echo "  FAILED to remove network: $n (may have re-acquired containers)" >&2
  fi
done

# 6c. Remove volumes (only if --with-volumes)
removed_volumes=0
if [[ ${WITH_VOLUMES} -eq 1 ]]; then
  for v in "${STUDIO_VOLUMES[@]}"; do
    if docker volume rm "$v" >/dev/null 2>&1; then
      echo "  removed volume: $v"
      removed_volumes=$((removed_volumes + 1))
    else
      echo "  FAILED to remove volume: $v (still attached to a container?)" >&2
    fi
  done
fi

# ---------------------------------------------------------------------------
# 7. Post-action summary
# ---------------------------------------------------------------------------
echo
echo "=========================================================================="
echo "Cleanup complete on ${ACTUAL_HOSTNAME}"
echo "  containers removed: ${removed_containers} / ${#STUDIO_CONTAINERS[@]}"
echo "  networks removed:   ${removed_networks} / ${#STUDIO_NETWORKS[@]}"
if [[ ${WITH_VOLUMES} -eq 1 ]]; then
  echo "  volumes removed:    ${removed_volumes} / ${#STUDIO_VOLUMES[@]}"
else
  echo "  volumes:            preserved (--with-volumes not passed)"
fi
echo
echo "Docker disk usage (after):"
docker system df
echo "=========================================================================="
