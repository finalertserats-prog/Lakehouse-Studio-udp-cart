# Component logos

Served by the FastAPI app at `/assets/logos/{component_id}.svg` (the
`/assets` mount lives in `backend/main.py`). The catalog's `logo` field
points here directly.

## What lives here

One SVG per component referenced by `stacks/components-catalog.yaml`:

- 5 **certified** components carry hand-drawn brand-flavoured marks
  (recognisable silhouette / colour, drawn fresh so we don't ship
  third-party trademarked artwork without clearance).
- 17 **coming-soon** components carry a consistent monogram template —
  gradient circle + 1-2 letter mark. Same template, per-project palette.

This consistency is intentional: the cart UI was showing half-real,
half-monogram before, which read as half-finished. One coherent visual
system reads as deliberate.

## Why we didn't ship the real upstream SVGs

Most of these projects publish official logos under their own brand
guidelines (Apache, MinIO, StarRocks, etc.). Brand guidelines are
generally permissive for "showing that the project is supported", but
the safe path for a product UI is: draw originals that *evoke* the
project, then upgrade to official artwork after a clearance pass per
brand. Until that happens, every mark here is original art by the
Lakehouse Studio team.

## Adding or replacing a logo

1. Drop the SVG in this directory as `{component_id}.svg`. The id MUST
   match the component's `id` in `stacks/components-catalog.yaml` —
   that's what the catalog's `logo` field resolves against.
2. Stick to a 64x64 viewBox. The cart card scales it; bigger viewBoxes
   waste bytes, smaller ones blur on retina.
3. Inline all styles. No external fonts, no CSS imports — the SVG must
   render standalone, served as a static asset.
4. Run `python -c "from backend.catalog import validate_catalog; print(validate_catalog())"`
   — `validate_catalog()` warns (does not fail) if a certified
   component is missing `logo` or `tagline`.

## Attribution

| Component               | Source              | License / notes                                                    |
|-------------------------|---------------------|--------------------------------------------------------------------|
| iceberg                 | Original artwork    | Drawn for this repo. Inspired by Apache Iceberg's iceberg motif.   |
| iceberg-rest            | Original artwork    | Drawn for this repo. Mark hints at REST + Iceberg.                 |
| minio                   | Original artwork    | Drawn for this repo. Evokes the MinIO "M" flame mark.              |
| spark-iceberg           | Original artwork    | Drawn for this repo. Spark swooshes + an Iceberg cap.              |
| starrocks               | Original artwork    | Drawn for this repo. Star + orbiting dots.                         |
| All coming-soon entries | Original artwork    | Monogram template — gradient circle + initials, per palette.       |

When we replace any of these with an officially-distributed upstream
logo, update both this table and the upstream project's attribution
requirements (usually "include logo + link to project URL"; the
catalog's `url` field already carries the link).
