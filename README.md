# Retail Order Analytics Pipeline

A real-time + batch data engineering pipeline built to demonstrate the core
skills in a typical Data Engineer JD: Kafka (pub/sub), Spark (streaming +
batch), a relational database, containerization, and Kubernetes — all
runnable entirely on a laptop.

This README covers what was built, why each decision was made the way it
was, exactly how to run it, and a full troubleshooting log of every real
issue hit while building it (kept deliberately, since debugging a pipeline
like this is itself the skill being demonstrated).

---

## 1. What this is

```
                     ┌──────────────────┐
  order-generator ──▶│   Kafka topic:    │
  (Scala/sbt)        │   "orders"        │
                     └────────┬─────────┘
                              │
                 ┌────────────┴─────────────┐
                 ▼                           ▼
     Spark Structured Streaming     Spark Batch Job (on-demand /
     (streaming-job, Scala)          nightly CronJob in k8s)
     1-min windowed aggregation:     (batch-job, Scala)
      revenue, order count,          re-reads live_metrics,
      top SKU per window             recomputes daily_summary,
                 │                   flags anomalous hours
                 ▼                           │
           Postgres: live_metrics            ▼
                              Postgres: daily_summary, anomalies
```

Three independent Scala/sbt modules:

| Module | What it does | Run via |
|---|---|---|
| `order-generator` | Publishes synthetic JSON orders to Kafka | `sbt run`, or containerized |
| `streaming-job` | Spark Structured Streaming — windowed aggregation into `live_metrics` | `spark-submit`, or containerized |
| `batch-job` | Nightly reconciliation + statistical anomaly detection | `spark-submit --date YYYY-MM-DD`, or a k8s `CronJob` |

Everything can run three ways, in increasing order of "production-like":

1. **Bare metal** — `sbt run` / `spark-submit` directly on the host (fastest to iterate on)
2. **Docker Compose** — all five services (`kafka`, `postgres`, `order-generator`, `streaming-job`, `batch-job`) containerized and networked together
3. **Kubernetes** (`kind`, local) — the same containers deployed as a `Deployment`/`CronJob`, closer to how this would run on EKS

---

## 2. Why it's built this way (design decisions)

**Why Kafka in KRaft mode, not Zookeeper + Bitnami images.**
The original plan used Bitnami's Kafka/Zookeeper images. Partway through,
Bitnami moved versioned image tags (e.g. `bitnami/zookeeper:3.9`) behind a
paid subscription, breaking `docker compose up` with an image-not-found
error. Rather than chase Bitnami's changing distribution terms, the project
uses the **official `apache/kafka` image running in KRaft mode** — no
Zookeeper container at all, which is also the direction the Kafka project
itself has been moving.

**Why a single stateful streaming aggregation, not a two-stage one.**
An earlier version of `LiveOrderAggregator` did `groupBy(window, sku)` to
get per-SKU revenue, then a second `groupBy(window)` on top of that to roll
it up into a per-window total + top SKU. Spark's Structured Streaming
engine rejects this at query-start with `AnalysisException: Detected
pattern of possible 'correctness' issue due to global watermark` — chaining
two stateful aggregations against the same watermark risks the second one
silently dropping rows the first emits as "late." The fix: do exactly
**one** stateful aggregation (collecting per-SKU revenue into a list per
window via `collect_list`), and compute the top SKU afterward as a plain,
non-streaming DataFrame join inside `foreachBatch` — which runs on a static
micro-batch and isn't subject to that check.

**Why epoch milliseconds instead of ISO timestamp strings.**
The producer originally stamped orders with `Instant.now().toString`
(`2026-08-15T10:15:30.123456789Z`). Spark's `to_timestamp()` can silently
fail to parse that — nanosecond precision, `Z` suffix — and return `null`
for every row, which means the windowed aggregation would drop every event
and `live_metrics` would stay empty forever with **no error message
anywhere**. Epoch millis (a plain number) sidesteps the parsing entirely.

**Why connection details are environment variables, not hardcoded.**
Code originally hardcoded `localhost:9093` / `localhost:5432`. That only
works when a process runs directly on the host — inside a container,
`localhost` refers to the container itself, not the host or its sibling
containers. All three apps now read `KAFKA_BOOTSTRAP_SERVERS`, `JDBC_URL`,
`JDBC_USER`, `JDBC_PASSWORD` from the environment, defaulting to the
`localhost` values so bare-metal runs need no configuration.

**Why `imagePullPolicy: Never` on every app Deployment/CronJob in `k8s/`.**
Images built locally and loaded into `kind` via `kind load docker-image`
aren't in any registry. Without this flag, Kubernetes assumes it should
pull from Docker Hub, fails with `ErrImagePull`, and the pod never starts.
This is the single most common gotcha with `kind` + locally-built images.

**Why the Kafka Service in `k8s/kafka.yaml` exposes both port 9092 *and* 9094.**
In KRaft mode, the broker also communicates with its own controller role
over a separate port (9094 here). Docker Compose containers can reach each
other on *any* port automatically regardless of the compose file's `ports:`
list (that list only controls host exposure) — but a **Kubernetes Service
only forwards ports explicitly declared in its spec**. The first version of
this manifest only listed port 9092, which silently black-holed all 9094
traffic; the broker could never register with its own controller and
crash-looped with `Received a fatal error while waiting for the controller
to acknowledge that we are caught up`. Fixed by adding the controller port
to the Service.

**Why the anomaly detection is a plain z-score, not a fitted MLlib model.**
`DailyReconciliation` flags hours where order volume is more than 2
standard deviations from the day's mean, computed with plain Spark SQL
aggregations. At this data volume, a fitted MLlib model (KMeans distance,
GaussianMixture, etc.) would be overkill and harder to defend in an
interview than "I computed mean/stddev and flagged outliers." Worth
mentioning as a stated "next step" rather than overclaiming MLlib usage
that isn't really there.

---

## 3. Repo structure

```
retail-order-pipeline/
├── README.md                    ← this file
├── docker-compose.yml           ← kafka, postgres, + optional app services
├── rebuild-project.sh           ← recreates every file from scratch (see §6)
├── db/
│   └── init.sql                 ← schema: live_metrics, daily_summary, anomalies
├── order-generator/
│   ├── build.sbt
│   ├── project/
│   │   ├── build.properties
│   │   └── plugins.sbt          ← sbt-assembly, for the fat jar
│   ├── Dockerfile
│   └── src/main/scala/com/retailpipeline/OrderProducer.scala
├── streaming-job/
│   ├── build.sbt
│   ├── project/build.properties
│   ├── Dockerfile
│   └── src/main/scala/com/retailpipeline/LiveOrderAggregator.scala
├── batch-job/
│   ├── build.sbt
│   ├── project/build.properties
│   ├── Dockerfile
│   └── src/main/scala/com/retailpipeline/DailyReconciliation.scala
└── k8s/
    ├── namespace.yaml
    ├── kafka.yaml                ← Deployment + Service (KRaft, both ports)
    ├── postgres.yaml
    ├── order-generator.yaml
    ├── streaming-job.yaml
    └── batch-cronjob.yaml
```

---

## 4. How to run it

### Prerequisites (Windows/WSL2 — see full install steps in project history / ask if you need them again)
Docker Desktop (WSL2 backend), JDK 17, sbt, Spark 3.5.1 (standalone, for
`spark-submit`), `kubectl`, `kind`, `psql` client, Git.

### Option A — bare metal (fastest for active development)

```bash
# 1. Start Kafka + Postgres
docker compose up -d

# 2. Initialize the schema
psql -h localhost -U postgres -d retail -f db/init.sql

# 3. Create the Kafka topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# 4. Terminal 1 — order generator
cd order-generator && sbt run

# 5. Terminal 2 — streaming job
cd streaming-job && sbt package
spark-submit --class com.retailpipeline.LiveOrderAggregator \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  target/scala-2.12/streaming-job_2.12-1.0.jar

# 6. Terminal 3 — watch results land
watch -n 10 'psql -h localhost -U postgres -d retail -c "SELECT * FROM live_metrics ORDER BY window_start DESC LIMIT 5;"'

# 7. On demand — batch reconciliation (after an hour+ of streaming data)
cd batch-job && sbt package
spark-submit --class com.retailpipeline.DailyReconciliation \
  --packages org.postgresql:postgresql:42.7.3 \
  target/scala-2.12/batch-job_2.12-1.0.jar --date 2026-08-15
```

### Option B — Docker Compose (all containerized, one network)

```bash
docker compose up -d                                    # kafka + postgres
docker compose up -d --build order-generator streaming-job
docker compose ps                                       # confirm all Up
docker compose logs -f streaming-job
docker compose run --rm batch-job --date 2026-08-15      # on demand, not long-running
```

### Option C — Kubernetes (`kind`, closest to production)

```bash
kind create cluster --name retail-pipeline

docker build -t retail/order-generator:latest ./order-generator
docker build -t retail/streaming-job:latest ./streaming-job
docker build -t retail/batch-job:latest ./batch-job
kind load docker-image retail/order-generator:latest --name retail-pipeline
kind load docker-image retail/streaming-job:latest --name retail-pipeline
kind load docker-image retail/batch-job:latest --name retail-pipeline

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/postgres.yaml
kubectl get pods -n retail    # wait for kafka + postgres Running

# init schema — port-forward to a NON-5432 local port if docker-compose's
# Postgres is also running, to avoid a port collision
kubectl port-forward -n retail svc/postgres 5433:5432 &
psql -h localhost -p 5433 -U postgres -d retail -f db/init.sql

# create the topic inside the cluster
kubectl exec -it -n retail deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

kubectl apply -f k8s/order-generator.yaml
kubectl apply -f k8s/streaming-job.yaml
kubectl apply -f k8s/batch-cronjob.yaml
kubectl get pods -n retail
kubectl logs -n retail deploy/streaming-job -f

# trigger the CronJob on demand instead of waiting for its 2am schedule
kubectl create job -n retail --from=cronjob/batch-job batch-job-manual-run
```

### Stopping everything

```bash
# bare metal: Ctrl+C each terminal

kind delete cluster --name retail-pipeline
docker compose --profile app --profile batch down   # add -v to also wipe Postgres data
```

---

## 5. The database schema

Three tables (`db/init.sql`):

- **`live_metrics`** — written continuously by `streaming-job`. One row per
  1-minute window: `window_start`, `window_end`, `total_revenue`,
  `order_count`, `top_sku`. Primary key on `(window_start, window_end)` so
  the streaming job can safely upsert (`ON CONFLICT ... DO UPDATE`) rather
  than duplicate rows across micro-batches.
- **`daily_summary`** — written by `batch-job`. One row per date, the
  reconciled "source of truth" totals (streaming aggregates can drift
  slightly on restarts or late data, so a batch reconciliation pass is
  standard practice, not redundant busywork).
- **`anomalies`** — written by `batch-job` when an hour's order volume
  falls outside mean ± 2σ.

---

## 6. `rebuild-project.sh` — what it is and why it exists

While building this, several files drifted out of sync between what was
generated and what ended up on disk — mostly because `unzip` silently
skips files that already exist rather than overwriting them, so re-extracting
an updated zip over an existing project folder left stale files in place
without any warning. This caused several confusing build errors (a
Dockerfile still referencing a since-removed base image, a Scala file
missing a bug fix, etc.).

`rebuild-project.sh` is a single idempotent script that writes every
project file directly via `cat > file << 'EOF'` — no zip, no extraction,
no ambiguity about what's current. Running it always leaves every file in
the exact state described in this README. Safe to re-run any time you
suspect drift:

```bash
cd ~/projects/retail-order-pipeline
bash rebuild-project.sh
git status   # see what it changed, if anything
```

It does **not** touch `.git` history or anything not listed in §3's repo
structure.

---

## 7. Troubleshooting log (real issues hit, kept for reference)

| Symptom | Root cause | Fix |
|---|---|---|
| `docker compose up` fails: `bitnami/zookeeper:3.9: not found` | Bitnami moved versioned image tags behind a paid subscription | Switched to `apache/kafka:latest` in KRaft mode, no Zookeeper |
| `AnalysisException: possible 'correctness' issue due to global watermark` | Two chained stateful aggregations (`groupBy(window, sku)` then `groupBy(window)`) against the same watermark | Collapsed to one stateful aggregation + batch-side ranking in `foreachBatch` |
| `live_metrics` stays empty, no errors | `to_timestamp()` silently failing to parse `Instant.toString`'s ISO format | Producer sends epoch millis instead |
| `openjdk:17-slim: not found` | Docker Hub fully removed official `openjdk` images | Use `eclipse-temurin` images instead |
| `Not a valid command: assembly` | `project/plugins.sbt` (registers sbt-assembly) missing/stale on disk | Rewrote the file directly, confirmed via `grep` |
| Container `Up` but Kafka producer never actually sends | Hardcoded `localhost:9093` inside a container, where `localhost` means the container itself | Externalized to `KAFKA_BOOTSTRAP_SERVERS` env var |
| `kind`-deployed pods stuck `ErrImagePull` | Locally built images aren't in any registry; Kubernetes tries Docker Hub by default | Added `imagePullPolicy: Never` |
| Kafka pod `CrashLoopBackOff`, log shows `Received a fatal error while waiting for the controller to acknowledge that we are caught up` | K8s Service only exposed port 9092, not the KRaft controller port 9094 — unlike Compose, a K8s Service only forwards ports it explicitly declares | Added port 9094 to the Kafka Service |
| `kubectl port-forward svc/postgres 5432:5432` fails: address already in use; `psql` mysteriously shows "already exists" | Docker Compose's Postgres container already holds host port 5432; `psql` was silently hitting that database instead of the k8s one | Forward to a different local port (5433) and target it explicitly with `-p` |
| `error: unable to upgrade connection: container not found` on `kubectl exec` | Pod was mid-crash-loop; no container instance was actually running at that instant | Diagnosed via `kubectl get pods` (restart count) and `kubectl logs --previous` |

---

## 8. What I'd do differently at scale

- Swap the Postgres batch-layer sink for Snowflake or BigQuery for a real warehouse
- Add a Kafka schema registry instead of hand-parsing JSON with a hardcoded schema
- Replace `emptyDir` Postgres storage in `k8s/postgres.yaml` with a real `PersistentVolumeClaim`
- Add Prometheus/Grafana for pipeline observability instead of reading `psql` by hand
- Swap the z-score anomaly check for a proper Spark MLlib pipeline if the data volume justified it
- Add a schema-registry-aware dead-letter topic instead of printing malformed records to console
- Reimplement `streaming-job` in Flink as a second implementation, to compare latency/checkpointing tradeoffs directly

---

## 9. One-line summary (for a resume or interview)

"Built a lambda-style pipeline — Kafka (KRaft mode) for pub/sub ingestion,
Spark Structured Streaming for real-time revenue/order metrics, and a
nightly Spark batch job for reconciliation and statistical anomaly
detection — all deployed on Kubernetes locally via `kind`, with Docker
Compose and bare-metal run modes for faster iteration."
