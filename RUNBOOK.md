# RUNBOOK — running the Temporal LenderFlow POC

Operational companion to `README.md`. Everything here was verified on a
macOS/darwin box with Java 11.0.15, Maven 3.9.16, Temporal CLI 1.8.2.

> **Mocks:** this project ships its own mock server at `mock/mock_lender.py`.
> `lender-config.json` points at it (`http://localhost:9099`) out of the box.
> The Beeceptor route table is kept in section 8 as an alternative.

---

## 1. Files — what is actually needed

**Exactly one file is read at runtime.** The only file/env access anywhere in
`src/main/java` is:

```
StageActivityImpl.java:38   getResourceAsStream("lender-config.json")
```

No `.env`, no `application.yml`, no properties file, no env vars, no database.

| File | Needed | Notes |
|---|---|---|
| `src/main/resources/lender-config.json` | **Required** | The only runtime input. Maven copies it into `target/classes/`. **Always edit the `src/` copy** — edits to `target/` survive until the next `mvn compile` silently overwrites them. |
| `pom.xml` | **Required** | Dependencies resolve from `~/.m2`; already cached, so `-o` works. |
| `mock/mock_lender.py` | Required for live demo | Local mock for all 17 routes, both lenders. |
| `RUNBOOK.md` | Reference | This file. |
| `target/` | Generated | Safe to `mvn clean`. |

### Config state to be aware of

`lender-config.json` is currently tuned for **live demos**, not production values:

| Field | Current | Original |
|---|---|---|
| `federal.transport.baseUrl` | `http://localhost:9099` | `https://fed-mock.free.beeceptor.com` |
| `demobank.transport.baseUrl` | `http://localhost:9099` | `https://demobank-mock.free.beeceptor.com` |
| `pollIntervalSeconds` (×3 federal) | `10` | `120` |
| `pollIntervalSeconds` (×1 demobank) | `10` | `30` |

Keep `10` for demos — at 120s a poll demo takes an hour. Restore the 120/30
values before treating the config as representative of production timings.

---

## 2. Prerequisites

```bash
java -version      # 11.x — the pom targets release 11
mvn -v             # 3.x
temporal --version # Temporal CLI
python3 -V         # 3.7+ — stdlib only, no pip install needed
```

Install the Temporal CLI if missing:

```bash
brew install temporal
```

---

## 3. Quick start — tests only (no server, no mocks)

```bash
mvn test
```

6 tests, ~7 seconds. Runs entirely in-process on `TestWorkflowEnvironment` —
**no Temporal server, no mock, no network.**

This is also the single most persuasive demo in the repo:
`loanStatusPoll_exhausts_thenParksForManualRetrySignal` skips 30 × 120s of wall
clock (one hour) and asserts in milliseconds.

---

## 4. Full live demo — startup order

Four processes. Each must be up before the next.

### Terminal 1 — Temporal server

```bash
temporal server start-dev --ui-port 8233
```
Single binary. In-memory: **all workflow state is lost on restart**, and
default retention is 24h. UI: <http://localhost:8233>

### Terminal 2 — mock lender APIs

```bash
python3 mock/mock_lender.py
```
Serves all 17 routes for both lenders on `http://localhost:9099`.

### Terminal 3 — worker (this is the one that blocks)

```bash
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker federal"
```
**Nothing executes without a worker.** `start` only enqueues; the flow runs
inside this process. For the second lender, in another terminal:

```bash
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker demobank"
```

One worker per lender — task queue `lih-<slug>-work`. That is the per-lender
isolation story: separate queue, separate scaling, no new infra.

### Terminal 4 — drive it

```bash
R="mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="

$R"start federal LR-1001"
$R"status LR-1001"
$R"signal LR-1001 pledgeCardReady"
$R"start demobank LR-2001"
```
$R"signal LR-1001 pledgeCardReady"

temporal workflow signal -w lender-flow-RB-2 --name pledgeCardReady --input '{}'

**Always pass `-o`** (offline). It skips dependency resolution — several seconds
off every invocation, which matters when running these live.

### What each command replaces in production

| Command | Production equivalent |
|---|---|
| `start federal LR-1001` | `POST /lenderSanctionRequest` |
| `signal LR-1001 pledgeCardReady` | `PUT /v1/disbursement` (was a Zeebe message publish) |
| `signal LR-1001 retryLoanStatus` | `GET /workflow/publish-event?eventName=...` |
| `status LR-1001` | a Mongo read + Camunda Operate |

### Workflow IDs are single-use

`Runner.java:92` sets `WorkflowIdReusePolicy.REJECT_DUPLICATE`, so re-running
`start federal LR-1001` fails until retention expires. **That is the idempotency
proof** (it replaces `exec_lock` in Redis) — use a fresh `LR-` id each run.

---

## 5. Mock server — commands and scenario control

Start it:

```bash
python3 mock/mock_lender.py          # foreground, logs every request
python3 mock/mock_lender.py &        # background
```

Control endpoints — **no restart needed, the workflow reacts because branch
conditions live in config, not compiled code**:

```bash
curl localhost:9099/_state           # current knobs + live poll counters
curl localhost:9099/_reset           # reset poll counters only
curl "localhost:9099/_set?KEY=VALUE" # flip a knob (also resets counters)
```

### Scenario knobs

| Knob | Default | Effect |
|---|---|---|
| `panStatusCode` | `E` | `F`/`I` → **business reject**; any other value (e.g. `X`) → `successWhen` miss → **technical failure** |
| `panHttp` | `200` | `500` → technical failure on the pan branch |
| `unNameStatus` | `S` | anything else → `successWhen` miss → technical failure |
| `unNameHttp` | `200` | `500` → technical failure on the un-name branch |
| `panAadhaarHttp` | `200` | `500` → technical failure on that branch |
| `mobileAadhaarHttp` | `200` | `500` → technical failure on that branch |
| `aadhaarChecksNeeded` | `true` | `false` → parallel fan-out runs 2 branches, not 4 |
| `precheckDedupeFlag` | `N` | precheck dedupe result |
| `dedupeFlag` | `N` | `Y` → `customerCreation` + `dedupeRetry` both `SKIPPED` |
| `lenderCustomerId` | `""` | non-blank → `dedupeRetry` skipped (condition is `BLANK`) |
| `loanStatusSucceedsAfter` | `2` | polls before `/stp/loan-status` flips to SUCCESS. **`-1` = never** |
| `ftStatusSucceedsAfter` | `2` | same for `/ft/status` |
| `dedupeHttp` | `200` | `500` → technical failure, 2s→4s→8s backoff, then FAILED |
| `accountInfoStatus` | `200` | `406` → business reject at `accountInfo` |
| `sanctionApprovesAfter` | `2` | demobank sanction poll |

### Parallel-branch failure semantics

`Promise.allOf(...).get()` is **fail-fast**, and `ctx.putAll(...)` runs *after* it
(`LenderFlowWorkflowImpl.java:171-176`). Consequences:

- The first branch to fail aborts the whole step.
- **No branch's `extract` values reach the context — not even from branches that
  succeeded.** A partial fan-out yields nothing.
- `failedAt` records `PRECHECK/parallelChecks` only; the failing branch name
  appears in `reason`, not `failedAt`.
- Sibling activities are not proactively cancelled; the workflow returns and the
  server discards their results.
- There is no "optional branch" / continue-on-failure option in config. Parallel
  groups are all-or-nothing.

| | Technical failure | Business rejection |
|---|---|---|
| Failure type | `TECHNICAL_FAILURE` | `BUSINESS_REJECT` |
| Retries | 3 attempts, 2s → 4s backoff | **none** (non-retryable) |
| Time to fail | ~6s+ | immediate |
| `outcome` | `FAILED` | `REJECTED` |
| History | 3 × `ACTIVITY_TASK_STARTED` | 1 × `ACTIVITY_TASK_STARTED` |

### The scenario matrix

```bash
# Happy path
curl "localhost:9099/_set?panStatusCode=E&dedupeFlag=N&loanStatusSucceedsAfter=2"

# Business rejection — REJECTED, ZERO retries (non-retryable)
curl "localhost:9099/_set?panStatusCode=F"

# Technical failure — 3 retries with backoff, then FAILED
curl "localhost:9099/_set?dedupeHttp=500"

# Existing customer — customerCreation + dedupeRetry both SKIPPED
curl "localhost:9099/_set?dedupeFlag=Y&lenderCustomerId=CIF-9"

# Narrower fan-out — parallel group runs 2 branches instead of 4
curl "localhost:9099/_set?aadhaarChecksNeeded=false"

# Poll then succeed — advances on the 3rd poll
curl "localhost:9099/_set?loanStatusSucceedsAfter=3"

# Poll exhaustion — 30 polls, then parks on retryLoanStatus
curl "localhost:9099/_set?loanStatusSucceedsAfter=-1"

# Account rejected
curl "localhost:9099/_set?accountInfoStatus=406"
```

**For the durability demo, set `loanStatusSucceedsAfter=-1` first.** With the
default of 2 the whole federal flow reaches `eSignWait` in under 12 seconds —
there is no mid-flow left to catch when you kill the worker.

---

## 6. Temporal CLI — the commands that matter here

```bash
# --- health ---
temporal operator cluster health                        # -> SERVING

# --- is my worker connected?  (diagnoses most "it's stuck" cases) ---
temporal task-queue describe --task-queue lih-federal-work
#   Pollers section EMPTY = no worker attached.

# --- inspect ---
temporal workflow list
temporal workflow describe -w lender-flow-LR-1001       # Status, CloseTime
temporal workflow result   -w lender-flow-LR-1001       # blocks until done, prints FlowResult
temporal workflow show     -w lender-flow-LR-1001       # FULL event history

# --- query (same data as `status`, without the JVM boot) ---
temporal workflow query -w lender-flow-LR-1001 --type status

# --- signal (much faster than the Maven wrapper for live demos) ---
temporal workflow signal -w lender-flow-LR-1001 --name pledgeCardReady --input '{}'
temporal workflow signal -w lender-flow-LR-1001 --name retryLoanStatus
temporal workflow signal -w lender-flow-LR-1001 --name retryFundTransferStatus

# --- recovery ---
temporal workflow terminate -w lender-flow-LR-1001 --reason "demo reset"
temporal workflow reset     -w lender-flow-LR-1001 --event-id 3 --reason "replay"
```

Three of these carry weight in a review:

- **`workflow show`** — the answer to "we lose Operate's token view." Prints every
  activity scheduled/started/failed/retried with timestamps. On a PAN-rejection
  run, `panValidation` appears **exactly once** — visual proof that
  `BUSINESS_REJECT` bypassed the retry policy.
- **`workflow reset`** — the "DLQ + replay admin" row on the deletion slide. A
  failed flow stays queryable and can be rewound to any event and re-run.
- **`workflow signal`** — instant, where `mvn exec:java` spends ~3s booting a JVM.

---

## 7. Suggested demo order

1. **Durability.** Set `loanStatusSucceedsAfter=-1`, start a flow, let it enter
   the poll, `ctrl-C` the worker, restart it. Show the UI resuming mid-poll.
   Nothing else lands as hard — lead with it.
2. **Config-only lender.** `start demobank LR-2001`. A completely different stage
   graph on the same JAR. No deploy, no BPMN, no code.
3. **Time-skipped tests.** `mvn test` — a one-hour poll loop asserted in
   milliseconds. Then ask how that flow is tested today.
4. **Business vs technical failure.** `panStatusCode=F` (no retries, `REJECTED`),
   then `dedupeHttp=500` (backoff retries, `FAILED`). Same config surface,
   correctly different behaviour.
5. **The `status` query.** Live stage history, no Mongo read, no Operate.
6. **The deletion slide** (see README).

---

## 8. Alternative — Beeceptor instead of the local mock

Derived by walking `lender-config.json`, so every field below is one the
interpreter actually reads. **17 rules** — federal 14 + demobank 3. (README
says "the 11 below fit" and then lists 14; both are wrong.)

**One endpoint hosts both lenders** — no path collisions. Point both `baseUrl`s
at the same host.

> ⚠️ **Use exact-path matching, not prefix.** demobank's `/precheck` is a prefix
> of federal's `/precheck/name-dob`. A prefix rule for `/precheck` swallows all
> four federal precheck routes.

All rules return **HTTP 200** — the workflow branches on body, not status code.

### Federal — 14 rules

| # | Method | Path | Response body |
|---|---|---|---|
| 1 | POST | `/precheck/name-dob` | `{"status":"SUCCESS"}` |
| 2 | POST | `/precheck/dedupe` | `{"status":"SUCCESS","data":{"ddupe_flag":"N","customer_id":"","aadhaar_checks_needed":"true"}}` |
| 3 | POST | `/precheck/pan` | `{"panStatusCode":"E","status":"SUCCESS"}` |
| 4 | POST | `/precheck/un-name` | `{"status":"S"}` |
| 5 | POST | `/precheck/pan-aadhaar` | `{"status":"SUCCESS"}` |
| 6 | POST | `/precheck/mobile-aadhaar` | `{"status":"SUCCESS"}` |
| 7 | POST | `/stp/dedupe` | `{"ddupeFlag":"N","partialKycFlag":"N","lenderCustomerId":""}` |
| 8 | POST | `/stp/customer-creation` | `{"responseCode":"00","customerId":"CIF-1001","partialKycFlag":"N"}` |
| 9 | POST | `/stp/dedupe-retry` | `{"lenderCustomerId":"CIF-1001"}` |
| 10 | POST | `/stp/customer-info` | `{"status":"SUCCESS","referenceNumber":"REF-1"}` |
| 11 | POST | `/stp/account-info` | `{"statusCode":"200","status":"SUCCESS","loanAccountNumber":"LA-77001"}` |
| 12 | **GET** | `/stp/loan-status` | `{"loanStatus":"PENDING"}` — flip to `SUCCESS` mid-demo |
| 13 | POST | `/ft/pledge-card` | `{"statusCode":"200","bpmNumber":"BPM-9001"}` |
| 14 | **GET** | `/ft/status` | `{"status":"PENDING"}` — flip to `{"status":"SUCCESS","utr":"UTR123456789"}` |

### DemoBank — 3 rules

| # | Method | Path | Response body |
|---|---|---|---|
| 15 | POST | `/precheck` | `{"verdict":"PASS","custRef":"DEMO-CUST-1"}` |
| 16 | **GET** | `/sanction/status` | `{"state":"PENDING"}` — flip to `APPROVED` |
| 17 | POST | `/disburse` | `{"ok":"true","utr":"UTR-DEMO-999"}` |

Rule 17: `successWhen` compares `$.ok EQUALS "true"` as a **string** — quote it.

### Beeceptor scenario matrix

| Scenario | Edit | Expected |
|---|---|---|
| Business rejection | #3 → `{"panStatusCode":"F"}` | `REJECTED`, zero retries |
| Technical failure | #7 → HTTP **500** | 3 retries, 2s→4s→8s, then `FAILED` |
| Existing customer | #7 → `"ddupeFlag":"Y"`, `"lenderCustomerId":"CIF-9"` | `customerCreation` + `dedupeRetry` `SKIPPED` |
| Narrower fan-out | #2 → `"aadhaar_checks_needed":"false"` | 2 branches instead of 4 |
| Poll → park | leave #12 on `PENDING` | after 30 polls → `AWAITING_MANUAL_RETRY` |
| Account rejected | #11 → `"statusCode":"406"` | `REJECTED` at `accountInfo` |

### Switching the config over

```bash
sed -i '' 's#"baseUrl": "http://localhost:9099"#"baseUrl": "https://YOUR-ENDPOINT.free.beeceptor.com"#g' \
  src/main/resources/lender-config.json
```

**What Beeceptor cannot do:** rules 12, 14, 16 are poll endpoints, and a static
rule returns the same body forever — so a poll either succeeds immediately or
never. To demo "polls three times, then succeeds" you edit the rule body in the
web UI *while the flow is mid-poll*. The local mock flips on a counter instead,
which is why it is the default here.

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `start` returns but nothing happens | No worker attached | `temporal task-queue describe --task-queue lih-federal-work` — Pollers empty? Start the worker. |
| Flow fails instantly at `nameDobCheck` | Mock not running, or unparseable body | `curl localhost:9099/_state`. A non-JSON body → `unparseable response` → retryable → 3 retries → `FAILED`. |
| `start` rejected as duplicate | `WorkflowIdReusePolicy.REJECT_DUPLICATE` | Use a new `LR-` id. This is correct behaviour, not a bug. |
| Config edit had no effect | Edited `target/classes/` | Edit `src/main/resources/`, then `mvn -o compile`. |
| Workflows vanished | `start-dev` is in-memory | Expected — state is lost on server restart. |
| Poll demo too slow | `pollIntervalSeconds` back at 120 | Set to 10 for demos. |
| `Address already in use` on 9099 | Mock already running | `pkill -f mock_lender.py` |

### Full teardown

```bash
pkill -f "com.rupeek.poc.temporal.Runner"   # workers
pkill -f mock_lender.py                     # mock
pkill -f "temporal server start-dev"        # server
```

---

## 10. Architecture — how Temporal is working here

Three tiers, and one inversion that explains most of the operational properties.

```
┌─────────────────────────────────────────────────────────┐
│  TEMPORAL SERVER (4 services + persistence)             │
│                                                          │
│   Frontend ── gRPC API (start, signal, query)           │
│   History  ── owns event history, shard-partitioned     │
│   Matching ── hosts the task queues                     │
│   Worker   ── internal housekeeping (not yours)         │
│                                                          │
│   Persistence: Cassandra / Postgres / MySQL             │
│   Visibility:  Elasticsearch (search attributes)        │
└────────────────────────┬────────────────────────────────┘
                         │  workers LONG-POLL outbound
                         │  (server never dials in)
        ┌────────────────┴────────────────┐
        │                                 │
┌───────▼─────────┐              ┌────────▼────────┐
│ lih-federal-work│              │lih-demobank-work│
│  ├ workflow q   │              │  ├ workflow q   │
│  └ activity q   │              │  └ activity q   │
└───────┬─────────┘              └────────┬────────┘
        │                                 │
┌───────▼─────────────────┐      ┌────────▼────────────────┐
│ YOUR JAR × N replicas   │      │ YOUR JAR × M replicas   │
│  LenderFlowWorkflowImpl │      │  (identical code)       │
│  StageActivityImpl      │      │                         │
└─────────────────────────┘      └─────────────────────────┘
```

**The inversion:** workers long-poll *outbound*. The server never opens a
connection to a worker. No inbound ports, no load balancer in front of workers,
no service discovery — workers can sit behind NAT. This is why "add a worker" is
genuinely just "run another pod": there is nothing to register.

**Task queues are not a broker.** No SQS, no Kafka, no RabbitMQ.
`lih-federal-work` is a logical name inside the Matching service. Creating one is
free and implicit — it is never provisioned. `Runner.taskQueue()` (`:33`) just
builds a string. That is the "per-lender isolation with no new infra" claim, and
it is literally true.

Each queue name hosts **two** independent queues:

```
TaskQueueType  workflow   <- "run the workflow code forward"
TaskQueueType  activity   <- "execute this activity"
```

### The loop, traced through real event history

Using `loanStatus` poll #1 (events 59–66 of an actual run):

| Step | Who | What |
|---|---|---|
| 1 | Worker (workflow poller) | Picks up a Workflow Task, **replays `run()` from the top** against history, reaches the poll loop |
| 2 | Worker | Returns **commands**, not results: `ScheduleActivityTask` |
| 3 | History svc | Writes event 59 `ACTIVITY_TASK_SCHEDULED`, enqueues on the activity queue |
| 4 | Worker (activity poller) | Runs `StageActivityImpl.execute()` — the **only** place a socket opens |
| 5 | History svc | Events 60–61: started, completed with `{fedLoanStatus: PENDING, terminal: false}` |
| 6 | Worker | New Workflow Task → replay → `terminal` false → `Workflow.sleep(10s)` → returns `StartTimer` |
| 7 | History svc | Event 65 `TIMER_STARTED`; 10s later event 66 `TIMER_FIRED` |
| 8 | | back to step 1 |

**Workflow code produces commands; it never performs I/O.** That is the
determinism contract, and why `Workflow.sleep` survives a pod kill — the timer
lives in the History service, not in the JVM.

### Sticky execution

After the first workflow task, history shows:

```json
"taskQueue": {
  "name": "89252@HOST:3d6ba125-c5c2-48a4-b2df-e7ff058857d2",
  "kind": "TASK_QUEUE_KIND_STICKY",
  "normalName": "lih-federal-work"
}
```

The server routes subsequent workflow tasks to *that specific worker*, which has
the workflow state cached, and sends only the **new** events instead of the full
history. If that worker dies, the sticky task times out
(`stickyQueueScheduleToStartTimeout`), the task falls back to the normal queue,
and **any** worker replays from event 1. State lives in the server; the cache is
only a cache. That is the durability demo, mechanically.

---

### Scaling by layer

#### Application code — flat

One `LenderFlowWorkflowImpl`, one `StageActivityImpl`, zero lender names. Code
volume is **O(1) in lender count**; only `lender-config.json` grows. `demobank`
running a different stage graph on the same JAR is the proof.

What *does* force a code change: a new **execution mode** (a fifth case in the
`switch` at `:96`), a new **predicate op**, or a new **signal name** (the
hardcoded switch at `:238`). Interpreter changes — rare. Everything else is
config or a T3 handler.

#### Worker — horizontal and stateless

N replicas of the identical JAR on the same queue. No coordination, no leader
election, no partition assignment, no rebalancing.

Per-worker knobs (all verified present in SDK 1.25.2):

| Knob | Controls |
|---|---|
| `setMaxConcurrentActivityExecutionSize` | activities in flight per worker |
| `setMaxConcurrentWorkflowTaskExecutionSize` | workflow tasks in flight |
| `setMaxConcurrentActivityTaskPollers` / `WorkflowTaskPollers` | poller threads (observed: 5 each by default) |
| `setMaxWorkerActivitiesPerSecond` | rate limit, **this worker** |
| `setMaxTaskQueueActivitiesPerSecond` | rate limit, **whole fleet on this queue** |

`Runner.worker()` (`:70`) passes no `WorkerOptions` — pure defaults. Fine for a
POC; production wants these tuned.

**Critical property for this workload:** it is I/O-bound on bank endpoints, not
CPU-bound. And a *parked* workflow costs a worker **nothing** — `eSignWait` holds
no thread, no memory, no connection. Ten thousand loans sitting at e-sign for
three days consume zero worker capacity. Sizing is driven by *active* activity
concurrency, not by how many loans are open. That is the biggest difference from
a thread-per-request or poll-loop-service design.

#### Task queue — the isolation boundary

`lih-<slug>-work` per lender buys four things:

1. **Blast radius.** If Federal's endpoint goes to 30s latency, Federal's
   activity slots fill up. On a *shared* queue those slow activities would occupy
   every worker's slots and starve other lenders — head-of-line blocking.
   Separate queues make that structurally impossible.
2. **Independent scale.** Federal 20 pods, demobank 1.
3. **Independent deploy.** Roll interpreter changes to one lender's fleet first.
   With Worker Versioning, that is the safe-migration story.
4. **Per-lender rate limiting.** `setMaxTaskQueueActivitiesPerSecond` is a
   **fleet-wide** limit across all workers on that queue. Banks impose TPS caps;
   this honours them with no distributed rate limiter and no Redis token bucket.
   Given the design being replaced is SQS-FIFO-per-lender + KEDA, this is the
   strongest single line in the comparison.

#### Server — the part that needs planning

- `numHistoryShards` is **immutable after cluster creation**. Pick 512+. It is
  the one decision that cannot be walked back.
- History service scales with shards; Frontend and Matching scale horizontally.
- **Persistence is almost always the bottleneck**, not Temporal itself.
- Elasticsearch is required for search attributes — i.e. required for the ops
  queries that replace Operate.

Temporal Cloud removes all of this operationally (ap-south-1 available), at the
cost of per-action billing.

---

### Scaling per lender — three topologies

**A. One deployment per lender.** Max isolation: separate pods, JVMs, scaling
policies, deploy cadence. Cost: 15 deployments for 15 lenders.

**B. One deployment, many queues in one process.** A single worker process can
host multiple task queues — call `factory.newWorker()` once per slug before
`factory.start()`:

```java
for (String slug : slugs) {
    Worker w = factory.newWorker(taskQueue(slug));
    w.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
    w.registerActivitiesImplementations(new StageActivityImpl());
}
```

Fewer pods, queue-level slot isolation preserved. But JVM-level failures (OOM, GC
pause, bad deploy) hit every lender in that process.

**C. Tiered — recommended.** High-volume lenders (Federal) get their own
deployment; the long tail shares one multi-queue deployment. Real isolation where
volume justifies it, pod economy where it does not. Promoting a lender from the
shared pool to its own deployment is a config change plus a deploy — no code.

`Runner` supports A and C today (`worker <slug>`); B is the ~6-line loop above.

---

### What will actually limit this, in order of likelihood

**1. Poll loops dominate the event budget.** A real run produced **76 events for
a flow that polled twice**. Each poll cycle costs ~8 events (activity
scheduled/started/completed + workflow task scheduled/started/completed + timer
started/fired). At `maxAttempts: 30`, one `loanStatus` loop is ~240 events; with
`fundTransferStatus` too, a single slow loan approaches ~500. On **Temporal Cloud
actions are the billing unit**, so poll interval and `maxAttempts` are commercial
decisions, not just latency ones. Measure this before quoting a cost.

**2. The manual-retry loop is unbounded.** `runPoll` (`:212`) does
`attempt = 0; continue;` after the retry signal. Nothing caps how many times ops
can hit retry, and each cycle adds ~240 events. Temporal's history limits
(defaults on the order of ~50K events / ~50MB, configurable) would eventually
terminate the workflow. Unlikely in practice, but unbounded *by design*; the
standard fix is `Workflow.continueAsNew` to start a fresh history carrying
context forward. Name it as known-and-mitigable.

**3. Activity input duplication.** Every activity input re-sends the full `call`
config, `transport`, and accumulated `context` — history grew 473 → 34,325 bytes
across a 14-step flow. Scales as `steps × context size`, and context only grows.
Harmless now; it is the mechanism that meets the ~2MB payload ceiling once T3
stages carry image references.

None are blockers. They are the three numbers a skeptical reviewer will probe:
*what does one loan cost, and what stops it growing unbounded?*
