# BullMQ Valkey Benchmark

Benchmarks BullMQ (Node.js) across Valkey 7.2, 8.1, and 9.0 to measure
how server-side improvements affect job queue throughput.

## Quick Start (Local)

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

## Running on AWS (GitHub Actions)

For reproducible, production-representative results on Linux/Intel hardware,
use the GitHub Actions workflow. It provisions an ephemeral EC2 instance,
runs all benchmarks, and tears everything down automatically.

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

| Version     | Default Port | io-threads Port |
|-------------|-------------|-----------------|
| Valkey 7.2  | 6380        | 6390            |
| Valkey 8.1  | 6381        | 6391            |
| Valkey 9.0  | 6382        | 6392            |

## Tests

| Test | Description |
|------|-------------|
| **Raw PING** | Baseline Redis round-trip latency |
| **Bulk Insert** | `addBulk()` with 50,000 jobs |
| **Single Insert** | Concurrent `add()` calls (concurrency=10) |
| **Pure Overhead** | No-op jobs at c=1, c=10, c=50 |
| **10ms I/O Work** | Simulated async I/O at c=10, c=50 |
| **CPU Work** | 1,000 sin/cos per job at c=10 |

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `RUNS` | 5 | Runs per test (mean ± stddev) |
| `BULK_JOBS` | 50000 | Jobs for bulk insert |
| `PROCESS_JOBS` | 10000 | Jobs for processing tests |

## Output

Results are printed as a summary table and saved to `results.json`
(or `results-mt.json` for io-threads mode).
