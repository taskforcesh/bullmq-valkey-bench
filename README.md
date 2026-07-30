# BullMQ Valkey / PostgreSQL Benchmark

Benchmarks BullMQ (Node.js) across two suites:

- **`valkey`** (default) — Valkey 7.2, 8.1, and 9.0 over the Redis protocol,
  measuring how server-side improvements affect job-queue throughput.
- **`pg`** — the **BullMQ v6 PostgreSQL backend** (durable, and with
  `synchronous_commit=off`) compared against **Redis**, on identical hardware.

The same `bench.js` harness runs both; a `target` abstraction hides whether the
backend is Redis or PostgreSQL, so every test body is identical.

## Quick Start (Local)

### Valkey suite (default)

```bash
# 1. Start all three Valkey versions
docker compose up -d

# 2. Install dependencies
npm install

# 3. Run the benchmark (single-threaded, default config)
node bench.js

# 4. (Optional) Run with io-threads=4
docker compose --profile io-threads up -d
node bench.js --io-threads

# 5. Cleanup
docker compose --profile io-threads down -v
```

### PostgreSQL vs Redis suite (BullMQ v6)

```bash
# 1. Start Redis + PostgreSQL (durable) + PostgreSQL (synchronous_commit=off)
docker compose --profile pg up -d --wait

# 2. Install dependencies (pulls in bullmq v6 and the pg driver)
npm install

# 3. Run the PostgreSQL vs Redis comparison
npm run bench:pg          # or: node bench.js --suite=pg

# 4. Cleanup
docker compose --profile pg down -v
```

Requires **BullMQ v6+** (PostgreSQL backend) and the **`pg`** peer dependency —
both are declared in `package.json`.

## Running on AWS (GitHub Actions)

For reproducible, production-representative results on real EC2 hardware, use the
GitHub Actions workflows. Each provisions an ephemeral EC2 instance, runs the
benchmark, downloads results, renders a Job Summary, and tears everything down
automatically (even on failure).

- **Valkey Benchmark** (`.github/workflows/bench-valkey.yml`) — Valkey 7.2/8.1/9.0.
- **PostgreSQL vs Redis Benchmark** (`.github/workflows/bench-postgres.yml`) —
  the v6 PostgreSQL backend vs Redis. Uses the `pg` docker-compose profile and
  runs `node bench.js --suite=pg`.

Both share the same AWS/OIDC setup below.

### Setup

1. **Create a GitHub OIDC identity provider in AWS** (one-time, account-level):
   - Go to IAM → Identity providers → Add provider
   - Provider type: **OpenID Connect**
   - Provider URL: `https://token.actions.githubusercontent.com`
   - Audience: `sts.amazonaws.com`

2. **Create an IAM role** with:
   - Trust policy allowing your repo to assume it:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Principal": {
           "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
         },
         "Action": "sts:AssumeRoleWithWebIdentity",
         "Condition": {
           "StringEquals": {
             "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
           },
           "StringLike": {
             "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/bullmq-valkey-bench:*"
           }
         }
       }]
     }
     ```
   - Permission policy with:
     ```
     ec2:RunInstances, ec2:TerminateInstances, ec2:DescribeInstances,
     ec2:DescribeVpcs, ec2:CreateTags,
     ec2:ImportKeyPair, ec2:DeleteKeyPair,
     ec2:CreateSecurityGroup, ec2:DeleteSecurityGroup,
     ec2:AuthorizeSecurityGroupIngress,
     ssm:GetParameters
     ```

3. **Add the role ARN as a repository secret:**
   - Name: `AWS_BENCHMARK_ROLE_ARN`
   - Value: `arn:aws:iam::ACCOUNT_ID:role/your-bench-role`

4. Go to **Actions → Valkey Benchmark → Run workflow** and configure:

| Input | Default | Description |
|-------|---------|-------------|
| `instance_type` | `c6i.xlarge` | EC2 instance type (4 vCPU, 8GB) |
| `region` | `us-east-1` | AWS region |
| `runs` | `5` | Runs per test |
| `bulk_jobs` | `50000` | Jobs for bulk insert |
| `process_jobs` | `10000` | Jobs for processing tests |
| `run_io_threads` | `true` | Also run io-threads=4 benchmark |

### Cost

A `c6i.xlarge` run takes ~20 minutes and costs under $0.10. The instance
is terminated automatically even if the workflow fails.

### Output

- **Job Summary** — Markdown table with all results directly in the Actions UI
- **Artifact** — `results.json`, `results-mt.json`, and `system-info.json`

## Port Map

### Valkey suite

| Version     | Default Port | io-threads Port |
|-------------|-------------|-----------------|
| Valkey 7.2  | 6380        | 6390            |
| Valkey 8.1  | 6381        | 6391            |
| Valkey 9.0  | 6382        | 6392            |

### PostgreSQL vs Redis suite (`--profile pg`)

| Target                            | Port | Notes                                        |
|-----------------------------------|------|----------------------------------------------|
| Redis 7.4                         | 6379 | Redis baseline                               |
| PostgreSQL 17 (default)           | 5432 | Out-of-the-box durable, `synchronous_commit=on` |
| PostgreSQL 17 (sync_commit=off)   | 5433 | Tuning variant, fewer fsync waits            |

The **default** PostgreSQL target is the headline comparison against Redis (what
you get with no tuning). The **`sync_commit=off`** target is a secondary "what a
bit of tuning buys you" data point — not the default, and it trades some crash
durability for throughput.

## Tests

| Test | Description |
|------|-------------|
| **Raw round-trip** | Baseline latency (`PING` for Redis, `SELECT 1` for PostgreSQL) |
| **Bulk Insert** | `addBulk()` with 50,000 jobs |
| **Single Insert** | Concurrent `add()` calls (concurrency=10) |
| **Pure Overhead** | No-op jobs at c=1, c=10, c=50 |
| **10ms I/O Work** | Simulated async I/O at c=10, c=50 |
| **CPU Work** | 1,000 sin/cos per job at c=10 |

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SUITE` | `valkey` | Which suite to run: `valkey` or `pg` (also `--suite=pg`) |
| `RUNS` | 5 | Runs per test (mean ± stddev) |
| `BULK_JOBS` | 50000 | Jobs for bulk insert |
| `PROCESS_JOBS` | 50000 | Jobs for processing tests |
| `PG_POOL_MAX` | 64 | node-postgres pool size per backend (`pg` suite) |
| `PGUSER` / `PGPASSWORD` / `PGDATABASE` | `postgres` / `postgres` / `bullmq_bench` | PostgreSQL credentials (`pg` suite) |

## Output

Results are printed as a summary table and saved to JSON:

| Suite / mode | File |
|--------------|------|
| Valkey, single-threaded | `results.json` |
| Valkey, io-threads | `results-mt.json` |
| PostgreSQL vs Redis | `results-pg.json` |
