# Cloud Source Partition Discovery

This document describes how the cloud source connectors discover the directories they read from, and how to configure the depth at which those directories live. It applies to the Amazon S3 source (`connect.s3.*`) and the Google Cloud Storage source (`connect.gcpstorage.*`), which share the discovery machinery in `kafka-connect-cloud-common`.

`<prefix>` below is the connector-specific prefix: `s3` or `gcpstorage`.

---

## What discovery does

Every discovery round lists directories beneath the prefix given in the KCQL `FROM` clause, and each directory it finds becomes a *partition*: a unit of work with its own `ReaderManager`, its own file queue, and its own stored Kafka Connect offset. Files are then read recursively beneath each discovered directory, so discovery depth decides how the work is cut up, not which files are eligible.

The one exception is a directory that holds files but sits above the configured depth: it is never discovered, so its files are never read. Choose a depth that matches where your data actually lives.

Discovery repeats on an interval (`connect.<prefix>.source.partition.search.interval`, five minutes by default) so that directories created after the connector started are picked up, unless `connect.<prefix>.source.partition.search.continuous` is set to `false`.

## The depth model

`connect.<prefix>.source.partition.search.depth` is the number of directory levels below the configured prefix at which the partition directories live. It means the same thing on every cloud.

| Depth | Discovered for a prefix of `prefix/` | Typical use |
|-------|--------------------------------------|-------------|
| `0` | `prefix/` itself | A single flat directory of files |
| `1` | `prefix/topicA/`, `prefix/topicB/` | One directory per topic |
| `2` | `prefix/topicA/0000001/`, `prefix/topicA/0000002/` | The sink's `topic/partition/` layout |
| `n` | every directory exactly `n` levels down | Deeper custom layouts |

Exclusions apply at every level, so `connect.<prefix>.source.partition.search.excludes` (`.indexes` by default) stops the walk from descending into an excluded directory rather than discarding it at the end. Directories that are already known partitions are skipped too, which is what keeps a discovery round from rebuilding readers it already has.

## Migrating from `recurse.levels`

`connect.<prefix>.source.partition.search.recurse.levels` is deprecated because the same value meant a different depth on each cloud, and on S3 it did not even mean a fixed depth. The S3 lister searched from the prefix **exactly as configured** and counted that as its first level, whereas the GCS lister searched from the directory the prefix names.

Setting it still works and still means exactly what it meant before, on both clouds and for both spellings, but it now logs a warning telling you the equivalent depth.

| Old `recurse.levels` | Equivalent `search.depth` on S3, prefix ending in `/` or bucket root | Equivalent `search.depth` on S3, prefix without a trailing slash | Equivalent `search.depth` on GCS |
|----------------------|----------------------------------------------------------------------|------------------------------------------------------------------|----------------------------------|
| unset (`0`) | `1` | `0` | `0` |
| `0` | `1` | `0` | `0` |
| `1` | `2` | `1` | `1` |
| `2` | `3` | `2` | `2` |
| `n` | `n + 1` | `n` | `n` |

There is one more difference on S3 that the table cannot express. Because the deprecated key searched from the prefix as configured, the cloud treated that prefix as a **key prefix rather than a directory**, so a prefix without a trailing slash also matched sibling directories whose names start with it: `FROM bucket:backups/bytes` discovered `backups/bytesval/` as well as `backups/bytes/`. `search.depth` always searches from the directory the prefix names, so it matches `backups/bytes/` only.

That makes the migration a two-part check for an S3 prefix without a trailing slash. Set the depth from the table, and confirm no sibling directory shares the prefix; if one does and you were relying on it, list it in its own KCQL statement. `search.depth` together with a trailing slash is the unambiguous combination.

Setting both keys is rejected at startup with a message naming the deprecated one, so a half-finished migration fails loudly rather than silently picking a winner. A negative `search.depth` is rejected too.

## Interaction with `tasks.max`

A discovered directory is assigned to a single task by hashing its path (`ConnectorTaskId.ownsDir`). The hash is applied only to the leaf directories that discovery returns, never to the intermediate levels it walks through, so a partition is not dropped because an ancestor of it hashed to a different task.

This has a consequence worth stating plainly: at depth `0` there is exactly one directory to hash, so exactly one task reads anything and the others stay idle. The connector logs a warning when it starts with depth `0` and `tasks.max` greater than one. To spread the work, set the depth to the level your partition directories live at, so that there are at least as many directories as tasks.

## Release notes

- New: `connect.<prefix>.source.partition.search.depth` sets the partition discovery depth with identical meaning on S3 and GCS, and for a prefix written with or without a trailing slash. It always searches from the directory the prefix names. Prefer it over `recurse.levels`, which is portable only if you adjust the number per cloud and per prefix spelling.
- Deprecated: `connect.<prefix>.source.partition.search.recurse.levels`. It keeps working and keeps its existing meaning on both clouds and for both prefix spellings, including matching sibling directories that share an S3 prefix without a trailing slash, so **no existing configuration changes behaviour**. It now logs a warning with the equivalent depth. Setting it together with `search.depth` is an error, as is a negative depth.
- Fixed: on GCS, a discovery depth of `0` (the default when `recurse.levels` is unset) returned the prefix to every task without an ownership check, so with `tasks.max` above one every task read every file and each discovery round created another reader for the same prefix. The prefix is now owned by exactly one task and is not rediscovered once known.
