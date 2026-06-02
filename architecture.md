# MLS Platform — System Design

**Real-Estate Analytics & Reporting Platform**

A data-intensive platform ingesting **~100k records/day** from **10 MLS scrapers**. It is deliberately split into a **strongly-consistent transactional core** and an **availability-first analytics pipeline**.

---

## 1. High-Level Design

### Data Flow

```
EventBridge → Scrapers (Fargate) → SQS (+ DLQ) → Batch consumer → ClickHouse + S3 (Parquet)
Dashboard API → reads Postgres + ClickHouse → Users
```

### Architecture Diagram

```mermaid
flowchart TB
    subgraph Trigger["⏰ Scheduling"]
        EB["EventBridge Scheduler"]
    end

    subgraph Ingestion["🕷️ Ingestion — Fargate"]
        S9["9× Short Scrapers<br/>(Spot)"]
        S1["1× Long Scraper ~1.5h<br/>(On-Demand)<br/>heartbeat + overlap guard"]
    end

    subgraph Queue["📬 Durable Buffer"]
        SQS["SQS Queue"]
        DLQ["Dead Letter Queue<br/>(poison isolation / replay)"]
    end

    subgraph Processing["⚙️ Processing — Fargate"]
        BC["Batch Consumer<br/>enrich · bulk-insert · archive"]
    end

    subgraph Stores["🗄️ Data Stores"]
        PG[("Postgres / Aurora<br/>CP — users, invoicing,<br/>scraper state, dimensions")]
        CH[("ClickHouse — EC2<br/>AP — ReplacingMergeTree")]
        S3[("S3<br/>Parquet + PDF reports")]
    end

    subgraph Serving["🌐 Serving"]
        API["Dashboard API<br/>Bun/Hono — Fargate / Lambda"]
    end

    subgraph Async["📨 Async Workers"]
        PDF["PDF Worker<br/>Lambda → S3"]
        NOTIF["Notifier<br/>SES / SNS"]
    end

    Users(["👤 Users / Clients"])

    EB --> S9 & S1
    S9 --> SQS
    S1 --> SQS
    SQS -. poison .-> DLQ
    DLQ -. replay .-> SQS
    SQS --> BC
    S9 -. run-state .-> PG
    S1 -. run-state .-> PG

    BC -- "1. archive" --> S3
    BC -- "2. bulk insert" --> CH
    BC -- "enrich (dimensions)" --> PG

    API --> PG
    API --> CH
    API --> Users
    API --> PDF
    PDF --> S3
    NOTIF --> Users

    style PG fill:#1f6feb,color:#fff
    style CH fill:#d29922,color:#fff
    style S3 fill:#238636,color:#fff
    style DLQ fill:#da3633,color:#fff
```

### Components

| Component | Role |
|-----------|------|
| **Ingestion** | 10 Fargate scrapers (9 short on **Spot**, 1 long ~1.5h **on-demand**). Triggered by EventBridge Scheduler. Stream rows to SQS; report run-state to Postgres. |
| **Queue** | SQS + DLQ. Durable buffer; poison messages isolated for replay. |
| **Processing** | Batch consumer (Fargate). Enriches rows from Postgres dimensions, bulk-inserts to ClickHouse, archives Parquet to S3. |
| **Transactional store** | Postgres (RDS/Aurora). Users, properties, invoicing, scraper state, dimension data. |
| **Analytics store** | ClickHouse (self-hosted EC2). Hot query store; `ReplacingMergeTree` for dedup. |
| **Archive** | S3. Parquet (verification + replay) and generated PDF reports. |
| **Serving** | Bun/Hono Dashboard API (Fargate, or Lambda + RDS Proxy). Reads both stores. |
| **Async** | PDF worker (Lambda → S3); Notifier (SES/SNS) for client reminders. |
| **Ops** | Secrets Manager; VPC endpoints (no NAT); CloudWatch alarms (DLQ depth, merge lag); Postgres↔ClickHouse count reconciliation. |

---

## 2. Key Architecture Decisions

```mermaid
flowchart LR
    A["Scrapers never insert<br/>into ClickHouse directly"] --> A1["Batched inserts via consumer<br/>avoid 'too many parts' failure"]
    B["ReplacingMergeTree dedup"] --> B1["Pipeline is idempotent<br/>& safely retryable"]
    C["Dual-write order:<br/>S3 → ClickHouse → delete SQS msg"] --> C1["Durable copy exists<br/>before anything can fail"]
    D["Long scraper on-demand<br/>(not Spot)"] --> D1["Incremental stream +<br/>overlap guard + heartbeat"]
    E["ClickHouse justified by<br/>query latency only"] --> E1["~36M rows/year —<br/>Postgres alone could serve<br/>analytics for years"]

    style A fill:#0d1117,color:#fff
    style B fill:#0d1117,color:#fff
    style C fill:#0d1117,color:#fff
    style D fill:#0d1117,color:#fff
    style E fill:#0d1117,color:#fff
```

- **Scrapers never insert into ClickHouse directly** — batched inserts via the consumer avoid the *"too many parts"* failure.
- **`ReplacingMergeTree` dedup** makes the whole pipeline idempotent and safely retryable.
- **Dual-write order: S3 → ClickHouse → delete SQS message** — a durable copy exists before anything can fail.
- **Long scraper runs on-demand (not Spot)** — streams incrementally, with an overlap guard and heartbeat.
- **ClickHouse is justified only by query latency** — at ~36M rows/year Postgres alone could serve analytics for years.

### Dual-Write Sequence

```mermaid
sequenceDiagram
    participant SQS
    participant C as Batch Consumer
    participant S3
    participant CH as ClickHouse

    SQS->>C: receive batch
    C->>C: enrich from Postgres dimensions
    C->>S3: 1. write Parquet (durable copy)
    S3-->>C: ack
    C->>CH: 2. bulk insert (ReplacingMergeTree)
    CH-->>C: ack
    C->>SQS: 3. delete message
    Note over C,SQS: Crash before step 3 → message redelivered →<br/>dedup at merge time makes retry safe
```

---

## 3. CAP Theorem

Under a network partition you choose **Consistency OR Availability**. This system runs two subsystems with deliberately opposite choices.

```mermaid
flowchart TB
    P{{"🌐 Network Partition"}}

    subgraph CP["Transactional Core — CP"]
        direction TB
        PGN["Postgres single primary<br/>ACID for money & identity<br/>favors consistency over<br/>write availability"]
    end

    subgraph AP1["Analytics Pipeline — AP"]
        direction TB
        APN["SQS → Consumer → ClickHouse<br/>prioritizes ingest throughput<br/>& read availability<br/>eventual consistency OK"]
    end

    subgraph AP2["Object Storage — AP + Durable"]
        direction TB
        S3N["S3<br/>highly available<br/>strong read-after-write<br/>11 nines durability"]
    end

    P --> CP
    P --> AP1
    P --> AP2

    style CP fill:#1f6feb,color:#fff
    style AP1 fill:#d29922,color:#fff
    style AP2 fill:#238636,color:#fff
```

| Subsystem | Choice | Rationale |
|-----------|--------|-----------|
| **Transactional core (Postgres)** | **CP** | ACID for money and identity; on partition the single primary favors consistency over write availability. |
| **Analytics pipeline (SQS → consumer → ClickHouse)** | **AP** | Prioritizes ingest throughput and read availability; eventual consistency is acceptable (dashboards may lag seconds to minutes; dedup resolves at merge time). |
| **Object storage (S3)** | **AP + Durable** | Highly available, strong read-after-write, 11 nines durability. |

**Net:** consistency where correctness is non-negotiable (invoicing, users), availability and eventual consistency where freshness can safely lag (analytics).
