# OpenSearch local dev environment

Spins up a full local stack — Kafka 4.1 (KRaft), Kafka Connect 4.1, and OpenSearch 2.13 with demo TLS + basic auth — so you can exercise the `kafka-connect-opensearch` sink connector from this repo end-to-end without touching any remote infrastructure.

## What it does

1. Builds the connector fat-jar from source with JDK 17.
2. Extracts the OpenSearch demo CA and creates a JKS truststore so Connect can verify the server certificate.
3. Starts Docker Compose (Kafka, Connect, OpenSearch).
4. Creates topics `elastic-dr-topic` and `elastic-dr-topic2`.
5. Deploys the connector with `_header.index_name` routing: records land in whatever OpenSearch index is named in the `index_name` message header.
6. Produces sample records to both topics.
7. Verifies documents appear in OpenSearch.

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker + Docker Compose v2 | `docker compose version` must work |
| JDK 17 | Used to build the connector via sbt. On macOS `brew install --cask temurin@17`. |
| sbt | [scala-sbt.org/download](https://www.scala-sbt.org/download/) |
| `keytool` | Ships with every JDK |
| `curl`, `jq` | For the verify script |

## Quick start

```bash
# Run everything in one go (from repo root):
./dev-scripts/opensearch-local/scripts/00-setup.sh

# Or step by step:
./dev-scripts/opensearch-local/scripts/01-build-connector.sh
docker compose -f dev-scripts/opensearch-local/docker-compose.yml up -d
./dev-scripts/opensearch-local/scripts/02-make-truststore.sh
./dev-scripts/opensearch-local/scripts/03-create-topics.sh
./dev-scripts/opensearch-local/scripts/04-deploy-connector.sh
./dev-scripts/opensearch-local/scripts/05-produce-data.sh
./dev-scripts/opensearch-local/scripts/06-verify.sh
```

## Rebuilding the connector jar

```bash
REBUILD=1 ./dev-scripts/opensearch-local/scripts/01-build-connector.sh
```

## Teardown

```bash
./dev-scripts/opensearch-local/scripts/99-teardown.sh
```

Removes the Docker Compose stack (including volumes), the generated jar, truststore, and CA cert.

## Config translation (prod → local)

The connector config is a direct translation of the production `connect.elastic.*` config to the `connect.opensearch.*` key space, with secrets replaced by plain local values:

| Production key | Local key | Local value |
|---|---|---|
| `connect.elastic.kcql` | `connect.opensearch.kcql` | _(unchanged)_ |
| `connect.elastic.hosts` | `connect.opensearch.hosts` | `opensearch` (Compose service) |
| `connect.elastic.port` | `connect.opensearch.port` | `9200` |
| `connect.elastic.protocol` | `connect.opensearch.protocol` | `https` |
| `connect.elastic.use.http.username` | `connect.opensearch.use.http.username` | `admin` |
| `connect.elastic.use.http.password` | `connect.opensearch.use.http.password` | `bY7!qX3@hV9#kM2` |
| `ssl.truststore.location` | `ssl.truststore.location` | `/etc/secrets/opensearch-truststore.jks` |
| `ssl.truststore.password` | `ssl.truststore.password` | `changeit` |

The `secret.value://` / `secret.ref://` Lenses-provider URIs are not used locally; plain values are inlined in the connector JSON instead.

## Verification

After `00-setup.sh` completes:

- `curl http://localhost:8083/connectors/opensearch-dr/status` returns `state: RUNNING`
- `curl -sk -u 'admin:bY7!qX3@hV9#kM2' 'https://localhost:9200/dr-index-a/_count'` returns `{"count":3,...}`
- `curl -sk -u 'admin:bY7!qX3@hV9#kM2' 'https://localhost:9200/dr-index-b/_count'` returns `{"count":3,...}`
