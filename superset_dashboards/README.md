# Superset Dashboard Register

A small standalone web app that points at **any running Superset instance**,
pulls the full dashboard inventory over the Superset REST API, and lets you
browse and **export it to Excel**. Read-only — nothing in Superset is modified.

Built to answer the recurring ask: *"give me an exhaustive list of all
dashboards — team, owner, frequency, disposition (Superset vs Excel) — and
check if we can pull it as Excel from Superset."*

## What it collects per dashboard

| Column | Source in Superset |
| --- | --- |
| Dashboard | `dashboard_title` (links to the live dashboard) |
| Team | best-effort: custom **tags** → **roles** → owner (Superset has no first-class "team" field) |
| Owner(s) | `owners` |
| Frequency | real cadence from **Alerts & Reports** (crontab); falls back to the dashboard's auto-refresh interval |
| Disposition | always `Superset` here — Excel-sourced dashboards are added to the register manually |
| Status | Published / Draft |
| Tags / Roles | `tags`, `roles` |
| Last modified / by / Created | `changed_on`, `changed_by`, `created_on` |

## Run

```powershell
# from the repo root
.\superset_dashboards\run.ps1
```

or manually:

```powershell
..\.venv\Scripts\python.exe -m uvicorn app:app --port 8099   # from this folder
```

Then open http://127.0.0.1:8099, enter the Superset URL + read-only
credentials, click **Fetch dashboards**, and use **Download Excel**.

Use an account with at least `can read on Dashboard`. To populate the
**Frequency** column, the account also needs `can read on ReportSchedule`
(Alerts & Reports); without it that column is left blank and a note is shown.

## Can dashboards be extracted as Excel from Superset natively?

Short answer, so the register question is settled:

- **The dashboard *inventory* (this list) — yes, via the REST API.** That is
  exactly what this tool does, and it writes a formatted `.xlsx`.
- **A whole dashboard's layout — no.** Superset's native "Export" produces a
  ZIP of YAML (for import into another Superset), not Excel.
- **A single chart's underlying data — yes.** Each chart on a dashboard has an
  "Export to Excel/CSV" action for its result set. That is per-chart data, not
  the dashboard as a whole.

So: the *catalogue* of dashboards exports cleanly to Excel (here); the
*visuals* do not, and per-chart data export is manual and one chart at a time.

## Files

- `app.py` — FastAPI app + single-page UI
- `superset_client.py` — read-only Superset REST client
- `excel_export.py` — `.xlsx` builder (openpyxl)
- `run.ps1` — launcher using the repo `.venv`
