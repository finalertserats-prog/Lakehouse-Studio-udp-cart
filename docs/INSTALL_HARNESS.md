# Install Harness — Runbook

`scripts/install_harness.py` drives any Lakehouse Studio stack manifest
end-to-end and writes a structured evidence record ready for paste into the
matching `stacks/compatibility/<stack>.lock.yaml`. It's the mechanical tool
behind the "install 6 stacks, certify the ones that pass" workflow.

The harness does NOT promote a stack itself. It runs the install, captures
proof, and tells you exactly what to do next. Promotion stays a human edit
of the lock file so the certification trail remains auditable.

---

## One-line usage per stack

All installs target `D:\Projects\ClaudeCode\PNC` by default. Override with
`--work-dir`.

```bash
# 1. Pilot-stable baseline (already certified — re-run to validate the harness)
python scripts/install_harness.py --stack udp-local-v0.2

# 2. Trino candidate
python scripts/install_harness.py --stack udp-trino-local-v0.1

# 3. Iceberg + Nessie + Trino
python scripts/install_harness.py --stack iceberg-nessie-trino-local-v0.1

# 4. Hudi + HMS + Spark
python scripts/install_harness.py --stack hudi-hms-spark-local-v0.1

# 5. Delta + HMS + Spark + Trino
python scripts/install_harness.py --stack delta-hms-spark-trino-local-v0.1

# 6. Iceberg + Polaris + Spark
python scripts/install_harness.py --stack iceberg-polaris-spark-local-v0.1
```

Useful flags:

| Flag                 | Effect                                                                      |
|----------------------|-----------------------------------------------------------------------------|
| `--work-dir DIR`     | Override the WORK_DIR (default `D:\Projects\ClaudeCode\PNC`).               |
| `--keep`             | After success, leave containers running so you can poke at the install.    |
| `--no-teardown`      | After ANY outcome, skip `docker compose down` and skip removing install_dir. Use during debugging. |
| `--json [PATH]`      | Also emit the evidence record as JSON (to stdout if `PATH` omitted, else to the file). The YAML block always goes to stderr. |

The harness prints:

1. Header — stack id, work_dir, install_dir, install_id, flags.
2. Live runner logs (proxied through the event bus exactly as the UI sees them).
3. A summary table (STEP | STATUS | DURATION | EXIT_CODE).
4. On failure: the last 30 lines of the failing step's combined stdout+stderr.
5. The evidence YAML block (paste-ready).
6. Promotion instructions if smoke passed.

Exit code: `0` on smoke pass. Otherwise the 1-based index of the first failing
step (1=prepare, 2=clone, 3=env, 4=doctor, 5=start, 6=bootstrap, 7=smoke,
8=finalize). This lets CI distinguish "infra didn't come up" from
"infra fine, smoke failed".

---

## What to do with the evidence YAML

The harness prints a YAML list item that looks like this:

```yaml
- id: "2026-05-17-myhost-7f3a91c2c1"
  timestamp: "2026-05-17T10:24:53+00:00"
  operator: "vishnu.wildeagle@gmail.com"
  host:
    os: "Windows-11-10.0.26200-SP0"
    docker: "28.3.0"
    ram_gb: 15.3
    cpu_cores: 16
  via: "install_harness.py (Lakehouse Studio v0.6.1)"
  install_id: "inst_7f3a91c2c1"
  result:
    prepare: passed
    clone: passed
    env: passed
    doctor: passed
    start: passed
    bootstrap: passed
    smoke: passed
    finalize: passed
  proof:
    - "..."
    - "..."
```

If smoke passed, promote the stack:

1. Open `stacks/compatibility/<stack>.lock.yaml`.
2. Append the YAML block above to the `evidence:` list (preserve indentation —
   it's a list item, prefix with two-space indent under `evidence:`).
3. Change `status: candidate` -> `status: pilot-stable`.
4. Bump `version_id: 0.x.0` -> `version_id: 0.x.1` (patch bump per
   re-certification).
5. Set `certified_at: <ISO timestamp from the harness output>`.
6. Commit: `cert(<stack>): promote to pilot-stable`.

The harness prints these instructions verbatim at the end of every successful
run.

If smoke FAILED, the evidence block instead contains:

```yaml
smoke_failure_root_cause: |
  <last 20 lines of smoke step's stderr>
```

Do NOT promote. File this evidence under your investigation notes
(e.g. `notebook/sessions/<date>-<stack>-debug.md`), reproduce the failure,
fix it, then re-run the harness.

---

## How to interpret a failure

Look at three signals in this order:

### 1. Exit code
- `0` — full pipeline + smoke passed.
- `1..8` — first failing step's 1-based index.
- `130` — interrupted (Ctrl-C). The harness ran teardown.
- `2` — argument error (no such stack id, etc).

### 2. The summary table
```
STEP        | STATUS   | DURATION | EXIT
--------------------------------------------
prepare     | passed   | 0.1s     | —
clone       | passed   | 14.2s    | 0
env         | passed   | 1.8s     | —
doctor      | passed   | 6.4s     | 0
start       | passed   | 1m24s    | 0
bootstrap   | passed   | 2m11s    | 0
smoke       | failed   | 0m38s    | 1
finalize    | pending  | —        | —
```

The first row whose STATUS is `failed` tells you what broke.

### 3. The failure tail
For the failing step the harness prints the last 30 lines of combined
stdout+stderr. Common patterns:

| Step      | Symptom                                          | Likely cause                                                                |
|-----------|--------------------------------------------------|-----------------------------------------------------------------------------|
| clone     | `fatal: unable to access`                        | Network or repo URL drift. Check `stacks/<stack>.yaml` `repository.url`.    |
| env       | `compose image patch warning`                    | Manifest references an image not present in the cloned compose. Investigate. |
| start     | `pull access denied` / `manifest not found`      | A pinned image tag was removed upstream. Update `stacks/compatibility/<stack>.lock.yaml`. |
| start     | `port is already allocated`                      | Another install is running on the same port set. Stop it or change `--work-dir`. |
| bootstrap | timeout                                          | A service didn't come up in time. Inspect with `--no-teardown` then `docker compose ps`. |
| smoke     | `UnknownHostException` / S3 errors               | OS-specific networking gap (documented for udp-local-v0.2 on Windows).      |
| smoke     | `Table or view not found`                        | Bootstrap silently skipped its DDL. Look at the bootstrap step's stdout.    |
| finalize  | `evidence capture failed`                        | Disk full or permission issue under `evidence/`. Stack is still READY.      |

To debug live: re-run with `--no-teardown`, then go look:

```bash
cd D:\Projects\ClaudeCode\PNC\<install_dir>
docker compose ps
docker compose logs <service>
```

When done, tear down manually:

```bash
docker compose -p <UDP_PROJECT_NAME> down
```

(Project name lives in `stacks/<stack>.yaml` under `env_defaults.UDP_PROJECT_NAME`.)

---

## Recommended install order

Run installs from smallest RAM footprint up so you don't tie up the host on a
heavy stack before knowing the lightweight ones work.

| Order | Stack                                  | Why this slot                                                           | Est. RAM |
|------:|----------------------------------------|-------------------------------------------------------------------------|---------:|
| 1     | `udp-local-v0.2`                       | Pilot-stable baseline — validates the harness end-to-end before risking a candidate. | ~12 GB   |
| 2     | `udp-trino-local-v0.1`                 | Smallest candidate (Trino is lighter than Spark + StarRocks combined).  | ~8 GB    |
| 3     | `iceberg-nessie-trino-local-v0.1`      | Adds Nessie versioning on top of Trino — still no Spark.                | ~9 GB    |
| 4     | `iceberg-polaris-spark-local-v0.1`     | Polaris catalog + Spark.                                                | ~12 GB   |
| 5     | `delta-hms-spark-trino-local-v0.1`     | Two engines + HMS — heaviest of the Delta candidates.                   | ~14 GB   |
| 6     | `hudi-hms-spark-local-v0.1`            | Hudi-on-Spark + HMS. Last because Hudi has the most fragile bootstrap.  | ~12 GB   |

After each install:

1. If smoke passed -> promote per the YAML block above, commit, push.
2. If smoke failed -> file evidence under `notebook/sessions/`, do NOT
   promote. Continue to the next stack so you don't lose a full evening's
   verification time waiting on one broken candidate.

---

## Expected install time per stack

On a 16-core / 16-32 GB Docker Desktop host with warm image cache (i.e. you
ran one prior install today and the base images are cached):

| Stack                                  | Cold cache | Warm cache | Bootstrap | Smoke | Total (warm) |
|----------------------------------------|-----------:|-----------:|----------:|------:|-------------:|
| `udp-local-v0.2`                       | 12-18 min  | 4-6 min    | 1-3 min   | 30-60s | ~7 min       |
| `udp-trino-local-v0.1`                 | 10-15 min  | 3-5 min    | 1-2 min   | 30s   | ~6 min       |
| `iceberg-nessie-trino-local-v0.1`      | 11-16 min  | 4-6 min    | 1-2 min   | 30s   | ~7 min       |
| `iceberg-polaris-spark-local-v0.1`     | 14-20 min  | 5-7 min    | 2-3 min   | 60s   | ~9 min       |
| `delta-hms-spark-trino-local-v0.1`     | 16-22 min  | 6-9 min    | 3-5 min   | 60s   | ~12 min      |
| `hudi-hms-spark-local-v0.1`            | 14-20 min  | 5-8 min    | 3-5 min   | 60-90s | ~11 min      |

First install of the day (cold cache) is dominated by image pulls. Subsequent
installs reuse cached layers. Doctor + env steps are always under 15s.

If a step exceeds 2x the table above, kill the harness (Ctrl-C — it tears
down cleanly) and inspect Docker Desktop's resource usage. Common culprit:
WSL2 memory pressure causing Docker to swap.

---

## Cross-references

- Stack manifests: `stacks/*.yaml`
- Lock files (where evidence is appended): `stacks/compatibility/*.lock.yaml`
- Pipeline source: `backend/runner.py` (the harness imports `UDPRunner` directly)
- Evidence record shape reference: `stacks/compatibility/udp-local-v0.2.lock.yaml` (the `evidence[0]` entry)
- Harness tests: `tests/test_install_harness.py`
