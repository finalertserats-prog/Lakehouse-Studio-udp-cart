# Custom image bake recipes

Two stack manifests reference `lakehousestudio/spark-*` images that don't yet exist on Docker Hub. Until they're baked + published, those stacks cannot install end-to-end.

| Image | Used by stack | Bake recipe |
|---|---|---|
| `lakehousestudio/spark-hudi:3.5.0_0.15.0` | `hudi-hms-spark-local-v0.1` | [`Dockerfile.spark-hudi`](./Dockerfile.spark-hudi) |
| `lakehousestudio/spark-delta:3.5.0_3.2.1` | `delta-hms-spark-trino-local-v0.1` | [`Dockerfile.spark-delta`](./Dockerfile.spark-delta) |

## Promotion gate

Each lock file at `stacks/compatibility/<stack>.lock.yaml` flags the image-bake as a P0 promotion gate. Until the matching image is published to a public registry, the stack stays `status: candidate` and cannot promote to `pilot-stable`.

## Build

```bash
# From the repo root:
docker build -f scripts/images/Dockerfile.spark-hudi  -t lakehousestudio/spark-hudi:3.5.0_0.15.0  .
docker build -f scripts/images/Dockerfile.spark-delta -t lakehousestudio/spark-delta:3.5.0_3.2.1 .

# Smoke-test locally (verify the bundle jars loaded):
docker run --rm lakehousestudio/spark-hudi:3.5.0_0.15.0   ls -la /opt/spark/jars/ | grep hudi
docker run --rm lakehousestudio/spark-delta:3.5.0_3.2.1   ls -la /opt/spark/jars/ | grep delta

# Publish (requires Docker Hub auth):
docker push lakehousestudio/spark-hudi:3.5.0_0.15.0
docker push lakehousestudio/spark-delta:3.5.0_3.2.1
```

## Source

Both Dockerfiles came from a Gemini research dispatch (`2026-05-17`) that verified the Maven Central jar coordinates. The base image (`tabulario/spark-iceberg:3.5.5_1.8.1`) is the same one used by `udp-local-v0.2` — bundling Hudi/Delta on top reuses the certified Spark + S3A + Hive client setup.
