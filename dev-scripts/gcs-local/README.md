# GCS sink — time-bucket partition reproduction

Local harness that proves why fine-grained `HHmm` partition keys create one GCS
directory per minute, and how Lenses SMTs with a rolling window collapse those
into one directory per 15-minute bucket — **without changing the connector**.

## What it proves

| Connector | SMT config | Expected GCS dirs for 6 records spanning ~30 min |
|---|---|---|
| `gcs-bad` | `TimestampConverter`, `format.to.pattern=yyyyMMddHHmm` (no rolling window) | **~6** per-minute directories under `${GCS_PREFIX}/bad/` |
| `gcs-good` | `TimestampConverter`, `rolling.window.type=minutes` + `size=15` + `yyyy-MM-dd-HH-mm` | **2–3** bucket directories under `${GCS_PREFIX}/good/` |
| `gcs-rolling` | `InsertRollingFieldTimestampHeaders`, all-digit `yyyyMMddHHmmssSSS` input, `America/Los_Angeles`, Hive-style `PARTITIONBY load_date/load_hour/load_minute` | **2–3** `load_minute=NN` buckets under `${GCS_PREFIX}/rolling/` |

The `gcs-rolling` connector covers the all-digit timestamp case: an
`yyyyMMddHHmmssSSS` field and a non-UTC timezone. It uses
`InsertRollingFieldTimestampHeaders` because `TimestampConverter` would infer an
all-digit string as a Unix epoch and ignore `format.from.pattern`. Each record
carries both `load_ts` (ISO, for bad/good) and `load_ts_digits` (all-digit, for
rolling) representing the same instant.

Flush settings are identical (`flush.count=1`). Partition routing is controlled
solely by the header value written by the SMT — flushing only controls *when*
data is written, not *where*.

### SMT plugin-discovery gotcha (important)

Kafka Connect's reflective plugin scanner only registers SMTs that implement
`Transformation` **directly**. The `Insert*Headers` family
(`InsertRollingFieldTimestampHeaders`, `InsertFieldTimestampHeaders`, …) extends a
**package-private abstract** base (`InsertTimestampHeaders`), so the scanner can't
walk the subtype chain and never registers them — referencing one as an isolated
plugin fails with `Class ... could not be found`, even though it's in the jar.
`TimestampConverter` and `RenameHeader` are unaffected (they implement
`Transformation` directly).

The harness works around this by also putting the SMT jar on the worker
**classpath** (`CLASSPATH=/connectors/kafka-connect-smt/*` in
[docker-compose.yml](docker-compose.yml)), where unregistered transform classes
resolve via `Class.forName`. Anyone using an `Insert*Headers` SMT needs the same:
put `kafka-connect-smt.jar` on the Connect worker classpath, not only in an
isolated `plugin.path` directory.

### Exactly-once / re-runs

Both connectors set `connect.gcpstorage.exactly.once.enable=false`. Exactly-once
keeps an `.indexes/<connector>` tree at the **bucket root** (outside `GCS_PREFIX`);
leaving it in place makes a re-run seek past already-committed offsets and write
nothing. Disabling it keeps the demo re-runnable. `CLEAN_GCS=1` teardown also
wipes those index dirs as a belt-and-braces measure.

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker + Compose v2 | `docker compose version` |
| JDK 17 | For building the GCS sink assembly |
| sbt | Builds `kafka-connect-gcp-storage` |
| `curl` | Downloads the SMT jar from GitHub Releases |
| `gcloud` or `gsutil` | Used by the verify script to list GCS |
| Writable GCS bucket + SA JSON | `storage.objects.create` + `storage.objects.list` |

The Lenses SMT jar is always downloaded from the published GitHub Release —
never built from source. Default:
[kafka-connect-smt v1.5.0](https://github.com/lensesio/kafka-connect-smt/releases/tag/v1.5.0).
Override with `SMT_VERSION` or `SMT_RELEASE_URL` in `.env`.

## Quick start

Host ports (chosen to avoid clashing with `opensearch-local`):

| Service | Host port |
|---|---|
| Kafka | `29092` |
| Connect REST | `28083` |

```bash
cd dev-scripts/gcs-local
cp .env.example .env
# edit .env — set GCS_BUCKET, GCS_PREFIX, GCS_CREDENTIALS_FILE

./scripts/00-run.sh
```

Or step by step:

```bash
./scripts/01-build.sh
./scripts/02-up.sh
./scripts/03-create-topic.sh
./scripts/04-deploy.sh
./scripts/05-produce.sh
./scripts/06-verify.sh
```

Force a rebuild of jars:

```bash
REBUILD=1 ./scripts/01-build.sh
```

Clean restart:

```bash
./scripts/00-run.sh --clean
# or
CLEAN_GCS=1 ./scripts/99-teardown.sh   # also deletes gs://${GCS_BUCKET}/${GCS_PREFIX}/
```

## Layout

```
dev-scripts/gcs-local/
├── docker-compose.yml          # Kafka 4.1 + Connect 8.1
├── .env.example
├── connector-bad.json.tmpl     # per-minute HHmm (no rolling window)
├── connector-good.json.tmpl    # 15-minute rolling window (TimestampConverter)
├── connector-rolling.json.tmpl # all-digit + LA tz (InsertRollingFieldTimestampHeaders)
├── connectors/
│   ├── gcp-storage/            # assembly jar (built)
│   └── kafka-connect-smt/      # SMT jar (downloaded from GitHub Releases)
├── secrets/
│   └── gcp-credentials.json    # staged from GCS_CREDENTIALS_FILE
└── scripts/
    ├── 00-run.sh
    ├── 01-build.sh
    ├── 02-up.sh
    ├── 03-create-topic.sh
    ├── 04-deploy.sh
    ├── 05-produce.sh
    ├── 06-verify.sh
    └── 99-teardown.sh
```

## Why per-minute partitions proliferate

Configs that emit a partition key with `HHmm` (or equivalent) and **no** rolling
window keep creating a distinct directory every minute. Flush interval only
controls *when* objects are written, not *where*. The fix is configuration-only:

```properties
transforms=loadTs
transforms.loadTs.type=io.lenses.connect.smt.header.TimestampConverter
transforms.loadTs.header.name=load_ts
transforms.loadTs.field=_value.load_ts
transforms.loadTs.target.type=string
transforms.loadTs.format.from.pattern=yyyy-MM-dd'T'HH:mm:ss.SSS
transforms.loadTs.format.to.pattern=yyyy-MM-dd-HH-mm
transforms.loadTs.rolling.window.type=minutes
transforms.loadTs.rolling.window.size=15
```

Use a non-all-digit `format.from.pattern` input (e.g. ISO-8601). All-digit
strings are inferred as Unix epoch millis by `TimestampConverter` and the
`format.from.pattern` is skipped. For all-digit inputs, use
`InsertRollingFieldTimestampHeaders` instead (see `connector-rolling.json.tmpl`).

KCQL stays:

```sql
INSERT INTO $bucket:prefix SELECT * FROM topic PARTITIONBY _header.load_ts ...
```

## Teardown

```bash
./scripts/99-teardown.sh
CLEAN_GCS=1 ./scripts/99-teardown.sh   # also wipe the test prefix in GCS
```
