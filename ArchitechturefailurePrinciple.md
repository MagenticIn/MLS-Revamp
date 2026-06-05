

# 1. SQS Backlog Explosion

### Scenario

Your scrapers produce:

```text
100,000 messages/minute
```

But consumers process:

```text
20,000 messages/minute
```

After a few hours:

```text
Queue = millions of messages
```

Now:

* Data becomes stale
* Dashboard shows old information
* Users complain

### Fix

Monitor:

```text
ApproximateAgeOfOldestMessage
Queue Depth
```

Auto-scale consumers:

```text
Queue > 100k
→ Start 5 more consumers

Queue > 1M
→ Start 20 more consumers
```

---

# 2. Duplicate Scraper Runs

You already have:

```text
heartbeat + overlap guard
```

Good.

But what if:

```text
Scraper starts
↓
Writes "running"
↓
Crashes
↓
Never clears status
```

Now EventBridge sees:

```text
already running
```

Forever.

### Fix

Store:

```sql
status
started_at
heartbeat_at
```

If:

```sql
NOW() - heartbeat_at > 10 min
```

Mark job:

```text
dead
```

Allow restart.

---

# 3. SQS Duplicate Messages

Many engineers forget this.

SQS Standard guarantees:

```text
At least once delivery
```

Not exactly once.

Example:

```text
Message processed
Consumer crashes before ACK
```

AWS sends again.

Now:

```text
Same property inserted twice
```

### Fix

Everything must be idempotent.

Use:

```sql
property_id
source
snapshot_date
```

as unique keys.

---

# 4. ClickHouse Duplicate Records

You're using:

```text
ReplacingMergeTree
```

Good.

But people misunderstand it.

Example:

```text
Listing A
```

Inserted 10 times.

Until merge runs:

```sql
SELECT *
```

returns 10 copies.

### Fix

Always:

```sql
FINAL
```

when correctness matters.

Or maintain version columns.

---

# 5. Scraper Flood Attack

Example:

Bug:

```javascript
while(true){
 sendToSQS()
}
```

Suddenly:

```text
10 million messages
```

SQS bill explodes.

ClickHouse explodes.

Aurora explodes.

### Fix

Rate limits.

Per scraper:

```text
Max records per run
Max records per minute
```

Kill switch.

---

# 6. Poison Message Infinite Loop

Current flow:

```text
SQS
 ↓
Consumer
 ↓
Fail
 ↓
Retry
 ↓
DLQ
```

Good.

But what if replay blindly sends it back?

```text
DLQ
 ↓
Replay
 ↓
Fail
 ↓
DLQ
 ↓
Replay
```

Infinite loop.

### Fix

Store:

```text
retry_count
root_cause
```

Block replay after threshold.

---

# 7. Aurora Connection Exhaustion

This is very common.

Imagine:

```text
20 consumers
```

Each opens:

```text
50 DB connections
```

Total:

```text
1000 connections
```

Aurora dies.

### Fix

Use:

* PgBouncer
* RDS Proxy

Never allow direct uncontrolled connections.

---

# 8. ClickHouse Becomes Write Bottleneck

Suppose:

```text
50 consumers
```

All inserting:

```text
100 rows
```

every second.

Thousands of tiny inserts.

ClickHouse hates small inserts.

### Fix

Batch:

```text
5000–50000 rows
```

before insert.

---

# 9. S3 Archive Mismatch

Current flow:

```text
Archive to S3
Insert to ClickHouse
```

What if:

```text
Archive succeeds
Insert fails
```

Now:

```text
Raw exists
Analytics missing
```

Users see incomplete data.

### Fix

Maintain ingestion ledger:

```sql
job_id
status
```

Track:

```text
archived
inserted
completed
```

---

# 10. Regional AWS Outage

Everything appears to be in one region.

If:

```text
us-east-1
```

has issues:

* EventBridge fails
* Fargate fails
* SQS fails

System stops.

### Fix

At least:

```text
Cross-region S3 replication
```

And disaster recovery plan.

---

# 11. Dashboard Query Kills ClickHouse

User requests:

```sql
SELECT *
FROM listings
```

on:

```text
500 million rows
```

Now ClickHouse CPU hits 100%.

Everyone's dashboard slows.

### Fix

Query guardrails:

```text
max_execution_time
max_rows
```

Pre-aggregated tables.

---

# 12. Missing Data Freshness Visibility

Current dashboard may show:

```text
Dallas Listings
```

But user doesn't know:

```text
Updated 5 mins ago
```

or

```text
Updated 2 days ago
```

### Fix

Track:

```sql
last_scraped_at
last_processed_at
```

Expose freshness everywhere.

---

# 13. Spot Instance Mass Eviction

You have:

```text
9x short scrapers (Spot)
```

AWS can terminate all simultaneously.

Example:

```text
Prime Day
Black Friday
Regional capacity issue
```

You lose scraping coverage.

### Fix

Mix:

```text
70% Spot
30% On-Demand
```

---

# 14. PDF Worker Cost Explosion

User exports:

```text
100k reports
```

Lambda starts:

```text
100k executions
```

SES sends:

```text
100k emails
```

Unexpected bill.

### Fix

Per-user quotas.

Rate limits.

---

# 15. Silent Data Corruption (Biggest Risk)

The most dangerous failure isn't downtime.

It's:

```text
Wrong data
```

Example:

Scraper parser changes.

Price:

```text
$1,250,000
```

becomes:

```text
12500
```

System continues working.

No alarms.

Dashboard shows garbage.

### Fix

Validation layer:

```text
Price range checks
Square footage checks
Duplicate detection
Outlier detection
```

Reject suspicious records.

---

# Additional Improvements I Would Add

### Redis Locking

For scraper coordination:

```text
Redis
  ↓
Distributed Lock
```

instead of relying only on Postgres.

---

### OpenTelemetry

Track:

```text
Scraper
 → SQS
 → Consumer
 → ClickHouse
```

with one trace ID.

Debugging becomes much easier.

---

### CDC Instead of Direct Reads

For future scale:

```text
Aurora
 ↓
Debezium
 ↓
Kafka/Redpanda
 ↓
Consumers
```

instead of polling tables.

---

### Data Quality Service

A dedicated service that checks:

* Missing regions
* Sudden record drops
* Price anomalies
* Scraper health

before data reaches customers.

