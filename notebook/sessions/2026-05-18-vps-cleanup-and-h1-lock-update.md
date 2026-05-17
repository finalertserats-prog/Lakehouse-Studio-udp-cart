# Session: 2026-05-18 — VPS deep clean + udp-local-v0.2 H1 evidence update

Short focused session that resolved two loose ends from the 2026-05-17 push: (1) the udp-local-v0.2 lock file was stale and still showed the pre-fix `smoke: failed` with a wrong root-cause attribution, and (2) the Finalert VPS had accumulated ~28 GB of dead Studio state across the multi-day nessie/polaris/delta debug loop. Both addressed cleanly.

The session also surfaced two recurring meta-issues — a credential-blindness habit (claimed no VPS access when the SSH key was sitting in `~/.ssh/`) and an over-narration habit (sentence-per-tool-call during investigations). Both saved as feedback memory.

## What Was Done

### Commits landed (3, in order)
1. `94cb540` docs(lock): udp-local-v0.2 — record H1 fix landed in 8a5914c (smoke re-verify pending) — `stacks/compatibility/udp-local-v0.2.lock.yaml` (+83/-7)
2. `68df6e0` feat(scripts): vps-cleanup.sh — allowlist-only cleanup gated to Finalert VPS — `scripts/vps-cleanup.sh` (NEW, 277 lines)
3. `582849a` fix(vps-cleanup): also match per-install compose-prefixed volumes — `scripts/vps-cleanup.sh` (+10/-3)

### Lock file evidence update — udp-local-v0.2
- Marked the `[minio, starrocks-fe, starrocks-be]` constraint's `known_issue` as **RESOLVED in commit 8a5914c**, preserving the original wrong-Windows-attribution text as historical context
- Added a new constraint `[iceberg-rest, starrocks-fe, starrocks-be]` with a full `required_keys` list (9 keys spanning both `aws.s3.*` and `s3.*` namespaces) so future bumps can't silently drop half
- Appended evidence record `2026-05-17-h1-fix-landed-pending-reverify` documenting the fix landing + cross-stack proof from `udp-trino-local-v0.1` (which uses the identical pattern and recorded `smoke: passed`)
- Added an `incompatible` entry capturing the "aws.s3.* only" wrong-config pattern with 3 upstream issue references (apache/iceberg-python#908, apache/iceberg#7709, trinodb/trino#25187)
- Refreshed `status_notes` with the full chronology (failure → root cause on Linux VPS → fix → sister-stack proof → awaiting re-verify)
- **Did NOT flip `smoke: failed` → `smoke: passed`** — append-only evidence convention requires an actual smoke run, not just a code-fix landing

### VPS cleanup script — scripts/vps-cleanup.sh
- New 277-line bash script with hard hostname gate (`srv1541349` only, no override flag)
- Allowlist-only matching: containers `^(udp|lhs)-`, dangling networks containing `udp`/`lhs`, volumes by prefix (`udp-*|lhs-*|udp_*|lhs_*`) + per-install compose prefix (`*_udp_*|*_udp-*|*_lhs_*|*_lhs-*`) + exact-name allowlist (`spark_jdbc_jars`, `airflow-pgdata`, `dagster-pgdata`, `prometheus-data`, `grafana-data`, `loki-data`)
- Default dry-run; `--apply` for containers+networks; `--apply --with-volumes` for full wipe (interactive YES confirm required)
- Reports `docker system df` before + after so disk delta is visible
- Image cleanup explicitly NOT in the script (to avoid nuking shared Finalert app images); handled separately by targeted `docker rmi` for known Studio repos

### VPS deep-clean execution (live on srv1541349)
Connected via `ssh -i ~/.ssh/lhs_deploy root@187.127.139.234` (deploy key was already on disk — see feedback memory for why this discovery took longer than it should have).

| Resource | Before | After | Delta |
|---|---|---|---|
| Containers | 0 | 0 | (already gone) |
| Networks | 1 dangling (`udp_default`) | 0 | -1 |
| Volumes | 18 (462 MB) | 0 | -18 / -462 MB |
| Lakehouse images | 12 (~28 GB) | 0 | -12 / -28 GB |
| Build cache | 7.4 GB | 0 | -7.4 GB |
| Host disk | 89/388 GB (23%) | 61/388 GB (16%) | -28 GB |

Generic images kept: `alpine:latest`, `mysql:8.0`, `postgres:15-alpine` (likely shared with other Finalert apps). Host-native `postgres` on port 5432 untouched.

### Memory writes
- `reference_finalert_vps_ssh.md` — SSH access details (key path, IP, hostname, user, repo location on VPS)
- `feedback_check_local_creds_before_claiming_no_access.md` — check `~/.ssh/`, public-key comments, known_hosts before disclaiming external access
- `feedback_check_first_then_talk.md` — batch investigative tool calls into one report; don't narrate per step
- `project_finalert_vps_state_2026_05_18.md` — post-wipe state + next-install-pulls-28GB caveat
- `MEMORY.md` index updated with all four entries

## Key Decisions Made

- **Did NOT flip `smoke: failed` → `smoke: passed` for udp-local-v0.2.** The H1 fix landed in `8a5914c` and the identical dual-property pattern produced `smoke: passed` on the sister stack `udp-trino-local-v0.1`, but the append-only evidence convention requires an actual smoke run on this stack's bootstrap path before the result can change. The lock file now contains a new evidence record explicitly stating "re-verification required" with the exact command sequence to run.

- **Image cleanup excluded from `vps-cleanup.sh`.** The script's safety contract is "allowlist-only against Studio's footprint" — `docker image prune` operates against the whole daemon and can't be constrained to a name pattern without becoming a different tool. Kept image cleanup as a separate explicit `docker rmi` step where the operator passes specific repo names. Trade-off: cleanup is two-phase (script for vols/containers, manual for images) but preserves the "can never harm non-Studio resources" guarantee.

- **Hostname gate, not env-var gate, for the cleanup script.** `srv1541349` is the Finalert VPS hostname literal (already in `hudi-hms-spark-local-v0.1.lock.yaml`). The gate is the first thing the script does, fires before flag parsing, and has no override. Reasoning: the threat model here is "accidental wrong-host invocation" (e.g. SSH'd into the wrong box, ran the script in the wrong terminal), not "defend against renamed-host attacker". For the latter we'd want machine-id pinning.

- **Per-install volume substring pattern (`*_udp_*`, `*_udp-*`)** added to the script after the first VPS dry-run revealed the original prefix-only pattern matched 7/18 volumes. The miss was real: 11 volumes used the per-install compose-project prefix (`delta-hms-spark-trino_udp_minio_data` etc., from commit `06fde64`'s install_dir-as-project-name fix). Fix preserves allowlist semantics — substring must be `_udp_` / `_udp-` / `_lhs_` / `_lhs-` specifically, not bare `udp` anywhere.

- **Lock file `incompatible` block captures the wrong-config pattern in plain text.** Even if commit messages fade from history (rebases, squash merges), the lock file's append-only `incompatible` array preserves the "aws.s3.* only is broken — must set both namespaces" lesson. Cost of writing it is one entry; cost of someone re-introducing the bug because they didn't read commit 8a5914c is another multi-day debug loop.

## What's Pending / Next Steps

### Highest priority — needs the VPS
- **Re-run `udp-local-v0.2` smoke on a clean VPS** to flip `smoke: failed` → `smoke: passed` and append a fresh evidence record. Sequence (from the new lock-file evidence record):
  ```bash
  cd /root/lakehouse_studio
  # The udp/ subdir clone may need re-cloning if it was inside an install_dir
  ./udp clean && ./udp start
  bash scripts/lhs-bootstrap.sh
  bash scripts/lhs-smoke.sh
  ```
  Note: will pull ~28 GB of images (alpine/mysql/postgres are cached; the 12 lakehouse images need fresh pulls). Budget ~15-30 min for first pull.

### From the candidate-stack chain (in-flight before this session, untouched today)
- `delta-hms-spark-trino-local-v0.1` — last commit `3775f7d` (`fix(delta): --jars explicit classpath for hadoop-aws + aws-sdk`); validation pending
- `iceberg-nessie-trino-local-v0.1` — stuck on Nessie 0.99 SmallRye URN env-var resolution; latest attempt was literal `access-key-id`/`secret-access-key` keys (`c2596dd`)
- `iceberg-polaris-spark-local-v0.1` — Polaris OAuth2 bootstrap + healthcheck; multi-round Codex/Gemini audit fix chain still iterating

### Carry-over from 2026-05-17 session (still open)
- Codex's `provision_push_api` one-line wire-up patch (deferred — needs a deliberate "go live" moment)
- Codex's Trino bash safety diff (M2/M3/L5 fixes to `_STUDIO_TRINO_BOOTSTRAP_SH` + `_STUDIO_TRINO_SMOKE_SH`)
- HIGH-priority sub-agent findings: audit_log silent-drop, audit_log PRAGMA-failure observability, backup.py sidecar tampering, gitops_import TOCTOU window
- 4 pre-existing test failures in `tests/test_compat_explainer.py` + `tests/test_expanded_catalog.py` — they assume hudi + trino stacks are still `candidate` but both promoted to `pilot-stable`. Tests need updating to reflect current cert status.

### Cleanup-tooling enhancement ideas (not requested)
- Add `--with-images` flag to `vps-cleanup.sh` that runs the targeted `docker rmi` loop for the known lakehouse repo names — would consolidate today's two-phase cleanup into one command
- Add `--scrub-evidence` to remove orphaned `evidence/{stack}/{install_id}/` dirs whose backing volumes are gone

## Patterns Learned

- **Credential blindness is a real failure mode.** Three turns of "I have no VPS access" disclaimers when the deploy key was sitting in `~/.ssh/lhs_deploy` the whole time. Future sessions: check `ls ~/.ssh/`, public-key comments (`awk '{print $NF}' ~/.ssh/*.pub`), and `~/.ssh/known_hosts` BEFORE claiming inability to reach an external system. The whole exchange could have been one tool call. Saved as `feedback_check_local_creds_before_claiming_no_access.md`.

- **User pacing pattern: "check first, then talk".** Mid-session correction ("i expect you to check first and then talk to me!") confirmed the user wants batched discovery + single report, not sentence-per-tool-call narration. The fix is to use multi-command bash (`&&`-chained) or parallel tool uses, then emit one consolidated report. Exception: still pause for confirmation on destructive operations (e.g. `--with-volumes`). Saved as `feedback_check_first_then_talk.md`.

- **Lock files lie when not actively maintained.** `udp-local-v0.2.lock.yaml` showed `smoke: failed` with a "Windows + AWS SDK" attribution for 36 hours after commit `8a5914c` had landed the correct fix on Linux VPS evidence. The sister stack's lock file (`udp-trino-local-v0.1`) was updated promptly because that promotion *required* the evidence flip; this one wasn't because no one re-ran its smoke. **Rule emerging:** whenever a fix lands that resolves a `known_issue` recorded in a lock file, write the "RESOLVED" marker in the same commit even if a re-verification run isn't immediately possible. Future readers should see the alignment between code state and lock-file state without needing to cross-reference git log.

- **Per-install compose-project naming changes what allowlist matches.** Commit `06fde64` (`teardown uses install_dir name (real compose project)`) made the install_dir name become the compose project prefix on all volumes. Any tool that filters Studio's docker artifacts by "starts with `udp-`" misses anything created after that commit when the install_dir wasn't named `udp`. Today's vps-cleanup.sh originally had this exact bug — 7/18 match. Fix: substring `_udp_` / `_udp-` patterns in addition to prefix matches. The same trap will hit anything else that filters docker resources Studio-side (status dashboards, monitoring overlays, evidence collectors).

- **Disk pressure on shared VPS comes from images, not volumes.** Today's accounting: 462 MB of volumes vs 28 GB of images + 7.4 GB of build cache. When users say "lake taking memory" on a shared host, the actual hog is almost always Docker images from failed-build iterations. Volumes are tiny by comparison until real data lands. Targeted `docker rmi` by repo name (not blanket `prune`) keeps other apps' images safe.

## Files Changed

### Source (committed + pushed to finalertserats-prog/Lakehouse-Studio-udp-cart `main`)
- `stacks/compatibility/udp-local-v0.2.lock.yaml` — modified (+83/-7) in commit `94cb540`
- `scripts/vps-cleanup.sh` — new file (277 lines) in commit `68df6e0`; later +10/-3 in commit `582849a`

### Notebook + memory (this session)
- `notebook/sessions/2026-05-18-vps-cleanup-and-h1-lock-update.md` — this file
- `memory/reference_finalert_vps_ssh.md` — new
- `memory/feedback_check_local_creds_before_claiming_no_access.md` — new
- `memory/feedback_check_first_then_talk.md` — new
- `memory/project_finalert_vps_state_2026_05_18.md` — new
- `memory/MEMORY.md` — updated index

### VPS side-effects (not source, no git artifact)
- All 18 named volumes removed on srv1541349
- 12 lakehouse Docker images removed on srv1541349 (lakehousestudio/spark-hudi, lakehousestudio/spark-delta, apache/polaris, trinodb/trino, minio/minio, minio/mc, starrocks/fe-ubuntu, starrocks/be-ubuntu, tabulario/spark-iceberg, ghcr.io/projectnessie/nessie, tabulario/iceberg-rest, bitsondatadev/hive-metastore)
- Build cache (7.4 GB) wiped on srv1541349
- 1 dangling network (`udp_default`) removed on srv1541349
- Net 28 GB disk reclaimed
