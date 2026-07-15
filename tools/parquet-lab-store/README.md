# parquet-lab-store

Publish a laboratory artifact (a snapshot or variant) to a disposable object-store namespace
and verify it read-only.

```text
parquet-lab-store publish --manifest ARTIFACT.json --config STORE.toml --prefix DISPOSABLE --receipt STORE.json
parquet-lab-store verify  --receipt STORE.json --config STORE.toml
```

`STORE.toml` selects a `local` filesystem directory or an `s3`-compatible endpoint (MinIO):

```toml
kind = "local"
base_dir = "/tmp/parquet-lab-store"
# kind = "s3"
# endpoint = "http://127.0.0.1:9000"   # MinIO
# bucket = "parquet-lab"
# region = "us-east-1"
# allow_http = true
```

Publish uploads with bounded buffers under the disposable prefix, verifies each object's
HEAD/size, records its SHA-256, and writes a `ukiel-parquet-store/v1` receipt atomically. It
**refuses** an existing prefix, a root/empty/absolute/traversing prefix, an overwrite, and a
mutated artifact (bytes not matching the manifest digest). Verification is **read-only** —
only HEAD and GET, never a put/delete/copy — and there is no delete command. Credentials
come from the environment (S3) and **never** enter a receipt.
