#!/usr/bin/env node
/**
 * BullMQ Valkey Benchmark
 *
 * Benchmarks BullMQ across Valkey 7.2, 8.1, and 9.0 to measure
 * how server-side improvements affect job queue throughput.
 *
 * Tests:
 *   1. Bulk Insert       – addBulk() with 50,000 jobs
 *   2. Single Insert     – concurrent add() calls
 *   3. Pure Overhead     – no-op job processing (c=1, c=10, c=50)
 *   4. 10ms I/O Work     – simulated async I/O (c=10, c=50)
 *   5. CPU Work          – 1000 sin/cos per job (c=10)
 *   6. Raw PING          – baseline Redis round-trip latency
 *
 * Usage:
 *   docker compose up -d
 *   npm install
 *   node bench.js                # default (single-threaded) ports
 *   node bench.js --io-threads   # multi-threaded ports (6390-6392)
 *
 * Environment:
 *   RUNS=5          Number of runs per test (default: 5)
 *   BULK_JOBS=50000 Jobs for bulk insert test
 *   PROCESS_JOBS=10000 Jobs for processing tests
 */

import { Queue, Worker } from "bullmq";

// ── Configuration ────────────────────────────────────────────────────

const useIOThreads = process.argv.includes("--io-threads");

const TARGETS = [
  { name: "Valkey 7.2", port: useIOThreads ? 6390 : 6380 },
  { name: "Valkey 8.1", port: useIOThreads ? 6391 : 6381 },
  { name: "Valkey 9.0", port: useIOThreads ? 6392 : 6382 },
];

const RUNS = parseInt(process.env.RUNS ?? "5", 10);
const BULK_JOBS = parseInt(process.env.BULK_JOBS ?? "50000", 10);
const PROCESS_JOBS = parseInt(process.env.PROCESS_JOBS ?? "10000", 10);

// ── Helpers ──────────────────────────────────────────────────────────

function mean(arr) {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function stddev(arr) {
  const m = mean(arr);
  return Math.sqrt(arr.reduce((s, v) => s + (v - m) ** 2, 0) / arr.length);
}

function fmt(n) {
  return new Intl.NumberFormat("en-US").format(Math.round(n));
}

function connOpts(port) {
  return { connection: { host: "localhost", port } };
}

async function flush(port) {
  const { Redis } = await import("ioredis");
  const r = new Redis({ host: "localhost", port, maxRetriesPerRequest: null });
  await r.flushall();
  await r.quit();
}

async function pingLatency(port, count = 5000) {
  const { Redis } = await import("ioredis");
  const r = new Redis({ host: "localhost", port, maxRetriesPerRequest: null });
  const start = performance.now();
  for (let i = 0; i < count; i++) {
    await r.ping();
  }
  const elapsed = performance.now() - start;
  await r.quit();
  return elapsed / count; // ms per ping
}

// ── Benchmark: Bulk Insert ───────────────────────────────────────────

async function benchBulkInsert(port, numJobs) {
  await flush(port);
  const queue = new Queue("bench-bulk", connOpts(port));

  const jobs = Array.from({ length: numJobs }, (_, i) => ({
    name: `job-${i}`,
    data: { index: i, payload: "x".repeat(64) },
  }));

  const start = performance.now();
  await queue.addBulk(jobs);
  const elapsed = (performance.now() - start) / 1000; // seconds

  const rate = numJobs / elapsed;
  await queue.close();
  await flush(port);
  return rate;
}

// ── Benchmark: Single Insert (concurrent) ────────────────────────────

async function benchSingleInsert(port, numJobs, concurrency = 10) {
  await flush(port);
  const queue = new Queue("bench-single", connOpts(port));

  let idx = 0;
  const start = performance.now();

  // Process in batches of `concurrency`
  while (idx < numJobs) {
    const batch = [];
    for (let i = 0; i < concurrency && idx < numJobs; i++, idx++) {
      batch.push(
        queue.add(`job-${idx}`, { index: idx, payload: "x".repeat(64) })
      );
    }
    await Promise.all(batch);
  }
  const elapsed = (performance.now() - start) / 1000;

  const rate = numJobs / elapsed;
  await queue.close();
  await flush(port);
  return rate;
}

// ── Benchmark: Processing ────────────────────────────────────────────

async function benchProcessing(port, numJobs, concurrency, workFn) {
  await flush(port);
  const queueName = `bench-proc-${concurrency}-${Date.now()}`;
  const queue = new Queue(queueName, connOpts(port));

  // Pre-load jobs
  const batchSize = 5000;
  for (let i = 0; i < numJobs; i += batchSize) {
    const batch = Array.from(
      { length: Math.min(batchSize, numJobs - i) },
      (_, j) => ({
        name: `job-${i + j}`,
        data: { index: i + j },
      })
    );
    await queue.addBulk(batch);
  }

  return new Promise((resolve, reject) => {
    let completed = 0;
    let startTime;

    const worker = new Worker(
      queueName,
      async (job) => {
        if (workFn) await workFn(job);
      },
      {
        ...connOpts(port),
        concurrency,
        autorun: false,
      }
    );

    worker.on("completed", () => {
      if (completed === 0) {
        startTime = performance.now();
      }
      completed++;
      if (completed >= numJobs) {
        const elapsed = (performance.now() - startTime) / 1000;
        const rate = numJobs / elapsed;
        worker.close().then(() => {
          queue.close().then(() => {
            flush(port).then(() => resolve(rate));
          });
        });
      }
    });

    worker.on("error", reject);
    worker.run();
  });
}

// ── Work functions ───────────────────────────────────────────────────

function noopWork() {
  return undefined;
}

function cpuWork() {
  // 1000 sin/cos operations (matches Elixir/Python benchmarks)
  let acc = 0;
  for (let i = 0; i < 1000; i++) {
    acc += Math.sin(i) * Math.cos(i);
  }
  return acc;
}

async function ioWork() {
  // 10ms simulated I/O
  return new Promise((resolve) => setTimeout(resolve, 10));
}

// ── Runner ───────────────────────────────────────────────────────────

async function runTest(label, fn, runs = RUNS) {
  const results = [];
  for (let r = 0; r < runs; r++) {
    process.stdout.write(`  Run ${r + 1}/${runs}...`);
    const rate = await fn();
    results.push(rate);
    process.stdout.write(` ${fmt(rate)} j/s\n`);
  }
  const m = mean(results);
  const s = stddev(results);
  console.log(`  → ${label}: ${fmt(m)} ± ${fmt(s)} j/s\n`);
  return { mean: m, stddev: s, runs: results };
}

// ── Main ─────────────────────────────────────────────────────────────

async function main() {
  const mode = useIOThreads ? "io-threads=4" : "single-threaded (default)";
  console.log(`\n╔══════════════════════════════════════════════════════╗`);
  console.log(`║   BullMQ Valkey Benchmark — ${mode.padEnd(24)}║`);
  console.log(`║   ${RUNS} runs · ${fmt(BULK_JOBS)} bulk · ${fmt(PROCESS_JOBS)} process jobs     ║`);
  console.log(`╚══════════════════════════════════════════════════════╝\n`);

  const allResults = {};

  for (const target of TARGETS) {
    console.log(`\n━━━ ${target.name} (port ${target.port}) ━━━\n`);
    const results = {};

    // 0. Ping latency
    console.log("▸ Raw PING latency");
    const pingMs = await pingLatency(target.port);
    console.log(`  → ${pingMs.toFixed(3)} ms/ping\n`);
    results.ping = pingMs;

    // 1. Bulk insert
    console.log("▸ Bulk Insert");
    results.bulkInsert = await runTest(
      "Bulk Insert",
      () => benchBulkInsert(target.port, BULK_JOBS)
    );

    // 2. Single insert
    console.log("▸ Single Insert (concurrency=10)");
    results.singleInsert = await runTest(
      "Single Insert",
      () => benchSingleInsert(target.port, 5000, 10)
    );

    // 3. Pure overhead
    for (const c of [1, 10, 50]) {
      console.log(`▸ Pure Overhead (c=${c})`);
      results[`overhead_c${c}`] = await runTest(
        `Overhead c=${c}`,
        () => benchProcessing(target.port, PROCESS_JOBS, c, null)
      );
    }

    // 4. I/O work
    for (const c of [10, 50]) {
      console.log(`▸ 10ms I/O Work (c=${c})`);
      results[`io_c${c}`] = await runTest(
        `I/O c=${c}`,
        () => benchProcessing(target.port, Math.min(PROCESS_JOBS, 5000), c, ioWork)
      );
    }

    // 5. CPU work
    console.log("▸ CPU Work — 1000 sin/cos (c=10)");
    results.cpu_c10 = await runTest(
      "CPU c=10",
      () => benchProcessing(target.port, PROCESS_JOBS, 10, cpuWork)
    );

    allResults[target.name] = results;
  }

  // ── Summary table ────────────────────────────────────────────────

  console.log("\n\n╔══════════════════════════════════════════════════════════════════════╗");
  console.log("║                         SUMMARY TABLE                              ║");
  console.log("╚══════════════════════════════════════════════════════════════════════╝\n");

  const tests = [
    ["PING latency", (r) => r.ping ? `${r.ping.toFixed(3)} ms` : "—"],
    ["Bulk Insert", (r) => r.bulkInsert ? `${fmt(r.bulkInsert.mean)} j/s` : "—"],
    ["Single Insert", (r) => r.singleInsert ? `${fmt(r.singleInsert.mean)} j/s` : "—"],
    ["Overhead c=1", (r) => r.overhead_c1 ? `${fmt(r.overhead_c1.mean)} j/s` : "—"],
    ["Overhead c=10", (r) => r.overhead_c10 ? `${fmt(r.overhead_c10.mean)} j/s` : "—"],
    ["Overhead c=50", (r) => r.overhead_c50 ? `${fmt(r.overhead_c50.mean)} j/s` : "—"],
    ["I/O c=10", (r) => r.io_c10 ? `${fmt(r.io_c10.mean)} j/s` : "—"],
    ["I/O c=50", (r) => r.io_c50 ? `${fmt(r.io_c50.mean)} j/s` : "—"],
    ["CPU c=10", (r) => r.cpu_c10 ? `${fmt(r.cpu_c10.mean)} j/s` : "—"],
  ];

  // Header
  const col1 = 20;
  const colW = 18;
  let header = "Test".padEnd(col1);
  for (const t of TARGETS) header += t.name.padStart(colW);
  console.log(header);
  console.log("─".repeat(col1 + TARGETS.length * colW));

  for (const [label, extractor] of tests) {
    let row = label.padEnd(col1);
    for (const t of TARGETS) {
      const val = extractor(allResults[t.name] || {});
      row += val.padStart(colW);
    }
    console.log(row);
  }

  // ── JSON output for article data ─────────────────────────────────
  const jsonOut = {};
  for (const t of TARGETS) {
    const r = allResults[t.name];
    jsonOut[t.name] = {
      ping_ms: r.ping,
      bulk_insert: r.bulkInsert?.mean,
      single_insert: r.singleInsert?.mean,
      overhead_c1: r.overhead_c1?.mean,
      overhead_c10: r.overhead_c10?.mean,
      overhead_c50: r.overhead_c50?.mean,
      io_c10: r.io_c10?.mean,
      io_c50: r.io_c50?.mean,
      cpu_c10: r.cpu_c10?.mean,
    };
  }

  const jsonPath = `results${useIOThreads ? "-mt" : ""}.json`;
  const fs = await import("fs");
  fs.writeFileSync(jsonPath, JSON.stringify(jsonOut, null, 2));
  console.log(`\nResults saved to ${jsonPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
