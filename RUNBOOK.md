# RUNBOOK — Temporal LenderFlow POC

## 1. What this is

One generic workflow that runs **every** lender's loan flow. The lender-specific
parts — stage order, endpoints, payloads, response parsing, retry policy — live in
an **external config store**, not in the jar.

Adding a lender is a set of config documents. Changing one does not need a
redeploy, and a change can repair a loan that is **already running**.

What the POC is built to demonstrate, in order of how much it matters:

| # | Claim | Scenario |
|---|---|---|
| 1 | A bad config change can be fixed **while a loan is stuck**, with no redeploy and no Temporal Reset | [S7](#s7), [S8](#s8) |
| 2 | Config changes are **replay-safe** — they change a flow's future, never its past | [S9](#s9), `ReplayCompatibilityTest` |
| 3 | Durable execution — kill the worker mid-flow, it resumes where it was | [S10](#s10) |
| 4 | A new lender is **config only**, zero code | [S11](#s11) |
| 5 | Business rejection vs technical failure are handled differently, and a failed loan is **red in the UI** | [S2](#s2), [S3](#s3) |
| 6 | A failed fan-out branch parks, and only **it** re-runs — successful siblings are never re-fired | [S4](#s4) |
| 7 | An operator can **write a value into a running loan** that config cannot reach | [S13](#s13) |
| 8 | A one-hour poll loop is asserted in milliseconds in tests | [S12](#s12) |

The flow itself:

| Stage | Steps | Temporal primitive |
|---|---|---|
| PRECHECK | `nameDobCheck` → `dedupeCheck` → 4 parallel checks | async fan-out, settle-all join, config-driven go/no-go gate |
| STP | `dedupe` → `customerCreation`* → `dedupeRetry`* → `customerInfo` → `accountInfo` → `loanStatus` | conditional steps, durable poll via `Workflow.sleep` |
| FUND_TRANSFER | `eSignWait` → `pledgeCardSend` → `fundTransferStatus` | `Workflow.await` on a signal (days-long human wait), poll → park → retry signal |

`*` conditional — governed by `condition` in config, evaluated against workflow context.

Design rationale lives in **[CONFIG-DESIGN.md](CONFIG-DESIGN.md)**. Deeper
background is kept in the [appendix](#appendix) at the bottom of this file.

---

## 2. Architecture, scale and UI

### The three tiers

```
   callers
        |  start, signal, query           (gRPC :7233)
        v
  Temporal Server  ──────────────  UI :8233
   history · matching · frontend        SQLite (dev) / Cassandra+ES (prod)
        ^
        |  workers LONG-POLL OUTBOUND for work
        |
  Worker pods  (one task queue per lender: lih-<slug>-work)
   LenderFlowWorkflowImpl   +   StageActivityImpl
        |                              |
        |                              v  HTTP to the lender
        |                        Config store (files or Mongo)
```

**The inversion that explains most of the operational properties:** workers
long-poll *outbound*. The server never opens a connection to a pod. So pods can
be behind a firewall, scaled to zero, or restarted mid-flow without the server
caring — and "is my worker connected" is answered by
`temporal task-queue describe`, not by network debugging.

### Where the state lives

Workflow state is **event history on the server**, not in the pod. A pod holds a
cache; if it dies, any other pod replays the history and continues. That is why
killing the worker mid-flow ([S10](#s10)) is a non-event, and why there is no
`PendingCorrelation` collection, no `poll_lock` in Redis, and no re-enqueue path.

### Scale, in one line each

- **One workflow class, N lenders.** Isolation is one task queue per lender
  (`lih-<slug>-work`), so a slow lender cannot starve another.
- **Parked loans cost nothing.** `eSignWait` for 72h and a 30-poll status loop
  consume no thread and no pod — they are timers on the server.
- **The real limits**, in order of likelihood: history size per workflow (keep
  under ~10k events; continue-as-new for long flows), Temporal Actions billing on
  chatty retry policies, and visibility-store load if you over-index search
  attributes. Detail in [A5](#a5-architecture--how-temporal-is-working-here).

### UI

`temporal server start-dev` serves the UI on **<http://localhost:8233>**.

What to look at during a demo:
- **Workflows list** → click a loan → **History** tab shows every activity,
  timer and signal, with inputs and results.
- Activity names are config-derived (`PRECHECK.panValidation`,
  `FUND_TRANSFER.disburse`), so the timeline reads like the flow instead of a
  stack of identical boxes. That choice has a production caveat — see
  [A6.1](#a61-ui-naming--dynamic-activity-vs-setsummary).
- **Pending Activities** on a running workflow shows the current attempt number
  and the next retry time — this is where you watch a retry ladder burn down in
  [S7](#s7).

---

## 3. Config — where it lives, what is in it, how to change it

### Where

`lender-config.json` is **only a seed source**. It is not read while a flow runs.
`Runner seed` normalises it into **seven collections** of small documents:

| collection | id | holds | when it is read |
|---|---|---|---|
| `pointers` | `federal` | **the only mutable doc** — which versions are live | every lookup |
| `flows` | `federal:v1` | stage names, in order | once, at workflow start |
| `stages` | `federal:v1:PRECHECK` | one doc **per stage**: steps, modes, conditions, gates | once, at workflow start (**pinned**) |
| `stage_knobs` | `federal:t1:PRECHECK` | one doc **per stage**: retry policy, timeouts | **once per stage boundary** |
| `bindings` | `federal:b1:PRECHECK.nameDobCheck` | one doc **per call**: method, path, request template, extract, predicates | **every activity attempt** (hot) |
| `transports` | `federal:t1` | baseUrl, static headers | every activity attempt |
| `lenders` | `federal` | display metadata | — |

**The three read points are the whole design.** Flow *shape* is pinned at start
so it cannot shift under a running loan. Retry/timeout *knobs* are re-read at each
stage boundary. The *wire contract* is resolved inside the activity on every
attempt — which is why editing a payload repairs a loan that is mid-retry.

### Two backends, same seven collections

| | selected by | layout |
|---|---|---|
| **files** (default) | `CONFIG_STORE=file`, `CONFIG_DIR=./config-store` | each `:` becomes a directory |
| **Mongo** | `CONFIG_STORE=mongo`, `MONGO_URI`, `MONGO_DB` | real BSON, indexed, queryable |

```
config-store/pointers/federal.json
config-store/stages/federal/v1/PRECHECK.json
config-store/bindings/federal/b1/PRECHECK.nameDobCheck.json
config-store/stage_knobs/federal/t1/FUND_TRANSFER.json
```

Neither backend caches — deliberately, so an edit lands on the next attempt and
you can *see* it. Production would cache immutable versions forever and re-read
only the pointer.

### How to change it

```bash
# see everything the store currently serves, stage by stage
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="show federal"
```

**File store** — edit one document:
```bash
python3 -c "
import json; p='config-store/bindings/federal/b1/STP.customerInfo.json'
d=json.load(open(p)); d['path']='/stp/v2/customer-info'; json.dump(d,open(p,'w'),indent=2)"
```

**Mongo** — one `updateOne`:
```bash
docker exec mongodb mongosh lender_config --quiet --eval \
  'db.bindings.updateOne({_id:"federal:b1:STP.customerInfo"},
                         {$set:{path:"/stp/v2/customer-info"}})'
```

Useful Mongo queries:
```js
db.bindings.find({lenderSlug:"federal", stage:"FUND_TRANSFER"})  // per-stage view
db.stage_knobs.findOne({_id:"federal:t1:PRECHECK"})              // one stage's knobs
db.pointers.findOne({_id:"federal"})                             // what is live
```

**Publishing a version** is a pointer flip; **rollback** is flipping it back.
Every other document is immutable and version-named.

Payload templates support nested JSON, typed leaves, defaults and optional keys:

```json
"request": {
  "applicant": { "name": "${ctx.customerName | upper}", "pan": "${ctx.pan}" },
  "amount":       "${num:ctx.loanAmount ?: 150000}",
  "consent":      "${bool:ctx.consentGiven ?: \"true\"}",
  "?middleName":  "${ctx.middleName}"
}
```

An unresolved `${ctx.x}` with no `?:` default **fails loudly** rather than sending
an empty string. Response side supports dot paths, array indexing
(`$.data.items[0].ref`) and regex capture (`regex:$.message:/REF-(\d+)/`).

---

## 4. Running the POC

### Prerequisites

```bash
java -version      # 11.x — the pom targets release 11
mvn -version
temporal --version # brew install temporal
python3 --version  # for the mock
docker --version   # only if using CONFIG_STORE=mongo
```

Dependencies are cached in `~/.m2`, so every command below uses `-o` (offline).

### Tests only — no server, no mocks

```bash
mvn -o test
```
36 tests. The 1 skip is a deliberate fixture-capture test gated behind
`-Dregen.histories=true`. A one-hour poll loop runs in milliseconds because
`TestWorkflowEnvironment` skips time.

### Full live demo — four terminals, in this order

```bash
# Terminal 0 — seed the config store (once; no server needed)
mvn -o -q compile
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="seed"

#   ...or Mongo-backed:
#   docker run -d --name mongodb -p 27017:27017 mongo:7.0
#   export CONFIG_STORE=mongo      # export in EVERY terminal below
#   mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="seed"

# Terminal 1 — Temporal server (UI at http://localhost:8233)
temporal server start-dev

# Terminal 2 — mock lender APIs (all routes, both lenders, port 9099)
python3 mock/mock_lender.py

# Terminal 3 — worker. THIS ONE BLOCKS. Do NOT use -q, or you get no logs.
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker federal"
```

> **`-q` silences all logging.** `exec:java` runs inside Maven's JVM, so slf4j
> binds to Maven's logger and `-q` suppresses every INFO line. The flow still
> runs; you just cannot watch it. Drop `-q` on the worker terminal.

Terminal 4 is where you drive the scenarios below.

---

## 5. Reset — run this after EVERY scenario

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

```
1/3 config  wiped and re-seeded into file:/.../config-store (6 stages, 17 bindings)
2/3 mock    scenario knobs restored via http://localhost:9099/_reset (HTTP 200)
3/3 temporal deleted 1 execution(s) (all lender-flow-*)
ready — workflow IDs are free to reuse
```

Three stores drift during a demo and **all three must move together**, or the
next scenario silently inherits the last one's state:

1. **config store** — a scenario edited a binding or a knob
2. **mock** — a scenario flipped `panStatusCode`, `dedupeFlag`, a poll counter
3. **Temporal** — workflow IDs are single-use, so a finished loan blocks its own
   ID from being started again

Two details that make it actually clean:

- It **wipes** the config store before re-seeding, rather than overwriting it.
  Re-seeding alone would leave behind whatever a scenario published — a `b2`
  binding version, an extra stage — so the next scenario would start from a store
  that only looks pristine.
- It **deletes** (not just terminates) the executions, which is what frees the
  workflow ID — so every scenario below reuses `LR-1001` and stays copy-paste.

`reset <loanRequestId>` targets one loan instead of all of them. The worker keeps
running throughout; reset never touches it.

---

## 6. Test scenarios

Every scenario is four blocks in this order — **db/config**, **mock**,
**project**, **temporal** — then reset. Skip a block when it says *(none)*.
Terminal 3 (worker) stays up the whole time.

Shorthand used below:
```bash
alias runner='mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args'
# then:  runner "start federal LR-1001"
```

---

### S1 — Happy path <a id="s1"></a>

Proves the whole flow runs from config: 3 stages, a 4-way fan-out, two poll
loops, one human signal.

```bash
# 1. db/config   (none — seeded defaults)
# 2. mock        (none — defaults are the happy path)

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 30
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 pledgeCardReady"

# 4. temporal
temporal workflow show -w lender-flow-LR-1001 | grep -E "^  Status|Result"
```
**Expect** `Status COMPLETED`, and `"outcome":"COMPLETED"` with `utr` in the
result context. In the worker log, every step logs its `binding=b1` provenance.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S2 — Business rejection: no retries <a id="s2"></a>

A lender decline is an **answer, not a fault**. It must not be retried and must
not park.

```bash
# 1. db/config   (none)

# 2. mock
curl -s "localhost:9099/_set?panStatusCode=F" >/dev/null

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 20

# 4. temporal
temporal workflow list | head -3
temporal workflow show -w lender-flow-LR-1001 | grep -E "^  Status"
```
**Expect** `Status  Failed` — **red in the UI**, not green "Completed". A loan that
did not disburse is never shown as a success. The failure carries the type so a
decline stays distinguishable from an outage:

```bash
temporal workflow show -w lender-flow-LR-1001 -o json | grep -o '"type":"BUSINESS_REJECT"'
```
```
message : REJECTED at PRECHECK/parallelChecks — parallelChecks rejected by lender: panValidation
type    : BUSINESS_REJECT
```

Reached in **one attempt** — the worker log says
`BUSINESS REJECT — lender declined; no retry, no park`. The sibling branches still
ran and recorded results (settle-all, not fail-fast).

> Because declines and outages both show as `Failed`, "failed workflow count" on
> its own is not an alerting signal — filter by failure type, or a busy day of
> declines looks like an incident.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S3 — Technical failure: retries with backoff <a id="s3"></a>

Same config surface, correctly different behaviour from S2.

```bash
# 1. db/config   (none)

# 2. mock
curl -s "localhost:9099/_set?dedupeHttp=500" >/dev/null

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"

# 4. temporal — watch the retry ladder live
temporal workflow describe -w lender-flow-LR-1001 | grep -A6 "Pending Activities"
```
**Expect** 3 attempts at 2s → 4s backoff, then the step **PARKS** for up to 72h
rather than failing the loan. The worker log shows `attempt 1/2/3 ... HTTP 500`
then `PARKED`, with the exact command to rescue it. Every mode that can suffer a
technical failure now parks — SYNC, POLL and PARALLEL ([S4](#s4)); only
`AWAIT_SIGNAL` does not, because it is already a wait.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S4 — A failed PRECHECK branch parks, and only it re-runs <a id="s4"></a>

A technical failure inside the parallel fan-out parks the step, exactly like a
SYNC step. On release **only the failed branch re-runs** — a branch that already
succeeded is never re-fired, which for a lender call with a side effect would be
a duplicate request.

```bash
# 1. db/config   (none)

# 2. mock — the PAN endpoint starts 500ing
curl -s "localhost:9099/_set?panHttp=500" >/dev/null

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 35
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"

# 4. temporal   (none)
```
**Expect** `"parkedAt": "PRECHECK/parallelChecks"`, `waitingOnSignal: retryStep`,
and the branch tally showing three SUCCESS and one FAILED:
```
parallelChecks.panValidation.outcome     FAILED
parallelChecks.unNameCheck.outcome       SUCCESS
parallelChecks.panAadhaarLink.outcome    SUCCESS
parallelChecks.mobileAadhaarLink.outcome SUCCESS
```

```bash
# 2. mock — fix the endpoint
curl -s "localhost:9099/_set?panHttp=200" >/dev/null

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 retryStep"
sleep 20
```
**Expect** in the worker log — note `[panValidation] only`:
```
FANOUT PRECHECK/parallelChecks RELEASED by retryStep — re-resolving config (transport=t1) and re-running [panValidation] only
BRANCH PRECHECK/parallelChecks/panValidation LAUNCH (round 2) mandatory=true retry=3x
GATE PRECHECK/parallelChecks -> PROCEED
```
Proof from the mock's own request log: `/precheck/pan` was hit **4** times
(3 failed attempts + 1 after the fix) while `/precheck/un-name` was hit exactly
**1** time.

The park waits `parkTimeoutHours` — **72h by default, the same three days as the
e-sign wait**. If nobody signals in that window the loan fails with
`TECHNICAL_FAILURE` and goes red.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S5 — Conditional steps: existing customer <a id="s5"></a>

Branching lives in config, evaluated against workflow context.

```bash
# 1. db/config   (none)

# 2. mock
curl -s "localhost:9099/_set?dedupeFlag=Y&lenderCustomerId=CIF-9" >/dev/null

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 25
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 pledgeCardReady"
sleep 15
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"

# 4. temporal   (none)
```
**Expect** `customerCreation` and `dedupeRetry` both `SKIPPED`. The worker log
says exactly why:
`STEP STP/customerCreation SKIPPED — ctx.dedupeFlag EQUALS N [actual=Y]`

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S6 — Fan-out width decided by config <a id="s6"></a>

The parallel group runs 2 branches instead of 4, with no code change.

```bash
# 1. db/config   (none)

# 2. mock
curl -s "localhost:9099/_set?aadhaarChecksNeeded=false" >/dev/null

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 20
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"

# 4. temporal   (none)
```
**Expect** `panAadhaarLink` and `mobileAadhaarLink` `SKIPPED`, gate reports
`2/2 ok`. Worker log: `FANOUT PRECHECK/parallelChecks awaiting 2 branch(es)`.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S7 — THE HEADLINE: fix a stuck loan by editing config <a id="s7"></a>

A bad endpoint breaks a live loan. Fix the config; the loan repairs itself. **The
worker is never restarted and nothing is redeployed.**

```bash
# 1. db/config — break one binding while the loan is running
python3 -c "
import json; p='config-store/bindings/federal/b1/FUND_TRANSFER.pledgeCardSend.json'
d=json.load(open(p)); d['path']='/broken/pledge-card'; json.dump(d,open(p,'w'),indent=2)"
#    Mongo equivalent:
#    docker exec mongodb mongosh lender_config --quiet --eval \
#      'db.bindings.updateOne({_id:"federal:b1:FUND_TRANSFER.pledgeCardSend"},
#                             {$set:{path:"/broken/pledge-card"}})'

# 2. mock   (none)

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 30
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 pledgeCardReady"
sleep 30
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"
```
**Expect** `"parkedAt": "FUND_TRANSFER/pledgeCardSend"`, `waitingOnSignal: retryStep`.
The worker log shows `RESOLVED ... binding=b1` on **every attempt** — that is the
hot-config mechanism, visible.

```bash
# 1. db/config — fix it
python3 -c "
import json; p='config-store/bindings/federal/b1/FUND_TRANSFER.pledgeCardSend.json'
d=json.load(open(p)); d['path']='/ft/pledge-card'; json.dump(d,open(p,'w'),indent=2)"

# 3. project — release the parked step
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 retryStep"
sleep 25

# 4. temporal
temporal workflow show -w lender-flow-LR-1001 | grep -E "^  Status"
```
**Expect** `COMPLETED`. Worker log: `RELEASED by retryStep — re-resolving config`.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S8 — Change the API payload shape at runtime <a id="s8"></a>

Turn a flat payload into a nested, typed one — the kind of change that used to be
a code change. Includes a deliberately missing field to show strict mode.

```bash
# 1. db/config
python3 - <<'EOF'
import json
p = 'config-store/bindings/federal/b1/PRECHECK.nameDobCheck.json'
d = json.load(open(p))
d['request'] = {
  "requestId":    "${ctx._requestId}",
  "applicant":    {"name": "${ctx.customerName | upper}",
                   "dob":  "${ctx.dob}",
                   "pan":  "${ctx.pan}"},
  "amount":       "${num:ctx.loanAmount ?: 150000}",
  "consent":      "${bool:ctx.consentGiven ?: \"true\"}",
  "branchCode":   "${ctx.branchCode}",
  "?middleName":  "${ctx.middleName}"
}
json.dump(d, open(p, 'w'), indent=2)
EOF

# 2. mock   (none)

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 25
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"
```
**Expect** parked at `PRECHECK/nameDobCheck`. The worker log names the exact
problem and every key that *was* available:
```
TEMPLATE ERROR [binding b1] unresolved template ${ctx.branchCode}
  — context has no value for 'ctx.branchCode' and no ?: default was given
  | ctx keys available: [customerName, pan, mobile, dob, ...]
```
The old renderer would have sent `"branchCode": ""` to the bank without a word.

```bash
# 1. db/config — add a default
python3 -c "
import json; p='config-store/bindings/federal/b1/PRECHECK.nameDobCheck.json'
d=json.load(open(p)); d['request']['branchCode']='\${ctx.branchCode ?: \"BLR-001\"}'
json.dump(d,open(p,'w'),indent=2)"

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 retryStep"
sleep 25
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 pledgeCardReady"
sleep 25

# 4. temporal
temporal workflow show -w lender-flow-LR-1001 | grep -E "^  Status"
```
**Expect** `COMPLETED`, and in the log the rendered nested body with
`"branchCode":"BLR-001"`.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S9 — Config changes the future, never the past <a id="s9"></a>

Publish a new binding version while a loan is parked. The loan picks it up for
work **not yet done**; what already happened is untouched, and the recorded
history still replays cleanly.

```bash
# 3. project — start a loan and let it park on the e-sign wait
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 30

# 1. db/config — publish b2 and flip the pointer
python3 - <<'EOF'
import json, pathlib, shutil
src = pathlib.Path('config-store/bindings/federal/b1')
dst = pathlib.Path('config-store/bindings/federal/b2')
shutil.copytree(src, dst, dirs_exist_ok=True)
for f in dst.glob('*.json'):
    d = json.load(open(f)); d['bindingVersion'] = 'b2'; json.dump(d, open(f,'w'), indent=2)
p = 'config-store/pointers/federal.json'
d = json.load(open(p)); d['bindingVersion'] = 'b2'; json.dump(d, open(p,'w'), indent=2)
EOF

# 3. project — release it and check provenance
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 pledgeCardReady"
sleep 25
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"
```
**Expect** `lastBindingVersion: b2`; steps completed *before* the flip still show
`b1` in their history records. `skeletonVersion` is unchanged — flow shape is
pinned for the life of the loan.

```bash
# 4. temporal — the proof: the recorded history still replays against current code
mvn -o test -Dtest=ReplayCompatibilityTest
```
**Expect** 7/7 green. Rollback is flipping `bindingVersion` back to `b1`.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S10 — Durability: kill the worker mid-flow <a id="s10"></a>

Lead a demo with this one. Nothing else lands as hard.

```bash
# 2. mock — make loan-status poll for a while so there is time to kill the worker
curl -s "localhost:9099/_set?loanStatusSucceedsAfter=8" >/dev/null

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 30
#    >>> now ctrl-C Terminal 3 (the worker), wait 10s, and start it again:
#    mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker federal"
sleep 40
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"

# 4. temporal
temporal workflow describe -w lender-flow-LR-1001 | grep -E "Status|HistoryLength"
```
**Expect** the poll loop resumes at the same attempt. No state was lost, because
none of it was in the pod. The restarted worker **replays** history — and the
replay-aware logger means the log does not re-print the earlier stages.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

### S11 — A new lender, config only <a id="s11"></a>

A completely different stage graph on the same jar.

```bash
# 3. project — needs its own worker on its own task queue (new terminal)
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker demobank"
#    then, in terminal 4:
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="show demobank"
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start demobank LR-2001"
sleep 40

# 4. temporal
temporal workflow show -w lender-flow-LR-2001 | grep -E "^  Status"
```
**Expect** `COMPLETED`. DemoBank has 3 stages with entirely different steps
(`combinedPrecheck` → `sanction` poll → `disburse`) and zero code was written.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset LR-2001"
```

---

### S12 — Time-skipped tests <a id="s12"></a>

```bash
# 3. project only — no server, no mock, no worker
mvn -o test
```
**Expect** 36 tests, ~15s. A 30-poll loop at 120s intervals is an hour of wall
clock and runs in milliseconds. Then ask how that flow is tested today.

Worth naming individually:
- `bindingEditedMidRetry_healsTheLoan_withNoRedeploy` — S6, automated
- `replayIsClean_afterTheBindingChangedMidFlight` — S8, automated
- `skeletonChange_doesNotLeakIntoARunningExecution` — shape pinning
- `ReplayCompatibilityTest` — replays 6 committed histories against current code
- **`ConfigDeterminismTest`** — the counterfactual: two workflows, the same config
  edit, one breaks on replay and one does not. See below.

**Why `ConfigDeterminismTest` is worth reading.** It is the only test that proves
the *reason* for the design rather than the behaviour. It runs two workflows
against the same store and makes the same edit under both:

| workflow | reads config | result after the edit |
|---|---|---|
| `DriftingWorkflowImpl` (test-only, deliberately wrong) | from **workflow code** | `NonDeterministicException` — `Event 71 of type EVENT_TYPE_ACTIVITY_TASK_SCHEDULED does not match command type COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION` |
| `LenderFlowWorkflowImpl` (real) | inside an **activity** | replays clean |

That is the whole argument for the pointer design. A mutable pointer is safe
*precisely because the workflow never dereferences it* — the activity does, and
activity results are immutable history. Note the failure mode it prevents: the
drifting workflow would not "pick up new config", it would **stall**, retrying the
same workflow task forever while ops sees a loan that simply stops.

*(no reset needed — tests are hermetic)*

---

### S13 — Patch a running loan's context <a id="s13"></a>

The one case config **cannot** fix: the lender now demands a value that nobody
ever captured. No binding edit helps, because the value does not exist anywhere
in the loan. Write it into the running workflow by hand.

```bash
# 1. db/config — make a binding demand a key nothing produces
python3 -c "
import json; p='config-store/bindings/federal/b1/STP.customerInfo.json'
d=json.load(open(p)); d['request']['branchCode']='\${ctx.branchCode}'
json.dump(d,open(p,'w'),indent=2)"

# 2. mock   (none)

# 3. project
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"
sleep 40
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"
```
**Expect** `"parkedAt": "STP/customerInfo"` — strict rendering refused to send a
blank, so the step parked instead of shipping `"branchCode": ""` to the bank.

```bash
# 3. project — write the missing value straight into the live loan
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner \
    -Dexec.args="patch LR-1001 branchCode=BLR-042 agentId=AG-9"

# identity keys are refused — try it and see
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner \
    -Dexec.args="patch LR-1001 loanRequestId=LR-HIJACK"

mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"
```
**Expect** in `status`:
```
branchCode    BLR-042
agentId       AG-9
loanRequestId LR-1001          <- unchanged, the patch was refused
history       CONTEXT_PATCHED        operator set [branchCode, agentId] (was {branchCode=null, agentId=null})
              CONTEXT_PATCH_REFUSED  all keys protected: [loanRequestId]
```

`loanRequestId`, `lenderSlug` and `_requestId` are refused outright. Rewriting
`loanRequestId` mid-flight would re-point every downstream idempotency key at a
different loan — a double-disbursal path dressed up as an ops convenience.

Every patch is written into the step history, so it is auditable in `status` and
in the Temporal UI. The signal is fire-and-forget, which is why the CLI says
"sent" and tells you to confirm with `status` rather than claiming success.

```bash
# 3. project — release the parked step; the re-run renders the patched value
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 retryStep"
sleep 25
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 pledgeCardReady"
sleep 25

# 4. temporal
temporal workflow show -w lender-flow-LR-1001 | grep -E "^  Status"
```
**Expect** `COMPLETED`, with `branchCode` and `agentId` in the final context and
the outgoing payload carrying `"branchCode":"BLR-042"`.

> This is the most dangerous command in the API — it edits live loan state with no
> schema and no validation, and the change is permanent in history. It exists
> because the alternative (terminate and re-run a part-disbursed loan) is worse.

```bash
mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="reset"
```

---

## 7. Suggested demo order

1. **S10 durability** — kill the worker. Lead with it.
2. **S7 hot config fix** — the loan repairs itself from a config edit.
3. **S11 new lender** — different flow, same jar.
4. **S12 tests** — an hour-long poll asserted in milliseconds.
5. **S2 vs S3** — rejection and failure, correctly different, both from config.
6. **The deletion slide** — what this removes:

| Component today | Replaced by |
|---|---|
| `PendingCorrelation` store | workflow state |
| `contextSnapshot` in Mongo | `loadFlowSkeleton` activity result in event history |
| `poll_lock` in Redis | `Workflow.sleep` in the poll loop |
| `ASYNC_RESUME` → re-enqueue | the loop never stopped |
| `exec_lock:{slug}:{lr}:{stage}` | workflow ID + `WorkflowIdReusePolicy` |
| SQS FIFO work queue per lender | task queue per lender |
| DLQ + replay admin | failed workflow stays queryable; park + `retryStep`, or reset |
| `lender-async-service` (whole service) | — |

---

## 8. What this POC does NOT prove

- **Not load tested.** No throughput, latency or cost numbers under real volume.
- **No production config governance** — no schema validation, no dataflow
  validator, no approvals, no RBAC, no audit. See
  [CONFIG-DESIGN.md §7](CONFIG-DESIGN.md#7-what-was-not-built-and-why) for the
  full list of 18 things deliberately left out, and why each matters.
- **Step identity is still config step NAMES.** Renaming a step changes the
  idempotency key — a double-disbursal path. Immutable UIDs must land before any
  of this is editable in production. This is the #1 outstanding risk.
- **No security work** — mTLS, namespace isolation, payload encryption, PII
  redaction in logs (`LOG_RAW=false` is the only control today).
- **Real lender APIs are mocked.** Auth, pagination, partial responses and
  rate limits are all unexplored.

---

<a id="appendix"></a>
# Appendix — background and detail

Everything below is reference material: the reasoning behind the choices above,
kept so it is not lost. None of it is needed to run the POC.

---

## A1. Logs — reading a flow without opening the UI

### Getting them to appear

```bash
# RIGHT — logs at INFO
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker federal"

# WRONG — silent
mvn -o -q exec:java ...
```

`exec:java` runs in **Maven's own JVM**, so slf4j binds to Maven's logger rather
than to `slf4j-simple`, and `-q` then suppresses every INFO line the app emits.
The flow still runs; you just cannot see it. Drop `-q` on the worker terminal.

Levels and toggles live in `src/main/resources/simplelogger.properties`:

| | |
|---|---|
| `LOG_RAW=false` | suppress full request/response bodies (they carry PAN, Aadhaar ref, mobile) |
| `-Dorg.slf4j.simpleLogger.log.com.rupeek.poc.temporal.config.store=debug` | trace every config document read |

### What a healthy step looks like

```
[INFO] LenderFlowWorkflowImpl - --- STAGE PRECHECK [3 steps] knobs re-read from transport=t1 (retry 3x initial 2s x2.0 backoff, timeout 10s)
[INFO] LenderFlowWorkflowImpl - STEP PRECHECK/nameDobCheck mode=SYNC onFailure=PARK
[INFO] StageActivityImpl      - [PRECHECK/nameDobCheck] attempt 1 RESOLVED callKey=PRECHECK.nameDobCheck binding=b1 transport=t1 (re-read from the store on THIS attempt)
[INFO] StageActivityImpl      - [PRECHECK/nameDobCheck] attempt 1 --> POST http://localhost:9099/precheck/name-dob
[INFO] StageActivityImpl      - [PRECHECK/nameDobCheck] --> headers {X-Client-Id=rupeek-los-poc, Content-Type=application/json}
[INFO] StageActivityImpl      - [PRECHECK/nameDobCheck] --> body {"requestId":"RPK-LR-4002-6f0bec48","name":"TEST CUSTOMER","dob":"1990-01-15"}
[INFO] StageActivityImpl      - [PRECHECK/nameDobCheck] attempt 1 <-- HTTP 200
[INFO] StageActivityImpl      - [PRECHECK/nameDobCheck] <-- body {"status": "SUCCESS"}
[INFO] StageActivityImpl      - [PRECHECK/nameDobCheck] attempt 1 EXTRACTED {nameDobStatus=SUCCESS} (terminal=false)
[INFO] StageActivityImpl      -   successWhen: $.status EQUALS SUCCESS | actual=SUCCESS -> true
[INFO] LenderFlowWorkflowImpl - STEP PRECHECK/nameDobCheck SUCCESS http=200 binding=b1 extracted={nameDobStatus=SUCCESS}
```

Every decision the config drove is on one line with the value it saw:

```
CONDITION PRECHECK/parallelChecks/panAadhaarLink : ctx.aadhaarChecksNeeded EQUALS true [actual=true] -> true
STEP STP/dedupeRetry SKIPPED — ctx.customerId BLANK [actual=CIF-1001]
BRANCH PRECHECK/parallelChecks/mobileAadhaarLink LAUNCH mandatory=false retry=4x timeout=10s
FANOUT PRECHECK/parallelChecks awaiting 4 branch(es) — settle-all, not fail-fast
GATE PRECHECK/parallelChecks 4/4 ok | quorumMet=true (minSuccess=0) onBranchFailure=FAIL
GATE PRECHECK/parallelChecks -> PROCEED
POLL STP/loanStatus attempt 1/30 http=200 terminal=true binding=b1 extracted={fedLoanStatus=SUCCESS}
WAIT FUND_TRANSFER/eSignWait awaiting signal 'pledgeCardReady' for up to 72h (zero resource cost)
```

### What a broken binding looks like

Note `RESOLVED` on **every attempt** — that is the hot-config mechanism visible in
the log, and the window in which an edit lands:

```
[INFO ] [STP/customerInfo] attempt 1 RESOLVED callKey=STP.customerInfo binding=b1 transport=t1
[ERROR] [STP/customerInfo] attempt 1 TEMPLATE ERROR [binding b1] unresolved template ${ctx.missingThing}
        — context has no value for 'ctx.missingThing' and no ?: default was given
        | ctx keys available: [customerName, pan, mobile, ... customerId, _requestId]
[INFO ] [STP/customerInfo] attempt 2 RESOLVED ...
[INFO ] [STP/customerInfo] attempt 3 RESOLVED ...
[WARN ] STEP STP/customerInfo PARKED — retries exhausted: ... type='CONFIG_ERROR'
[WARN ] STEP STP/customerInfo waiting for retryStep. Fix the binding document in the
        config store, then: Runner signal LR-4003 retryStep
```

The error names the template, the binding version, and every context key that
*was* available — which is usually enough to write the fix without opening
anything else. After the fix:

```
[INFO] SIGNAL received: retryStep
[INFO] STEP STP/customerInfo RELEASED by retryStep — re-resolving config (transport=t1, retry 3x) and re-running the step
[INFO] [STP/customerInfo] --> body {"requestId":"...","customerId":"CIF-1001","branchCode":"BLR-001"}
[INFO] STEP STP/customerInfo SUCCESS http=200 binding=b1 extracted={customerInfoStatus=SUCCESS}
```

### Why the workflow uses a different logger

`LenderFlowWorkflowImpl` logs through `Workflow.getLogger(...)`, not
`LoggerFactory.getLogger(...)`. A workflow method re-executes from the top on
every replay, so a plain logger would re-emit every line each time a worker picks
the workflow up — a loan parked for a day would print its PRECHECK log dozens of
times and the log would read as though the flow ran dozens of times.
`Workflow.getLogger` is replay-aware and stays quiet while replaying.

Activities do not replay, so `StageActivityImpl` uses a normal slf4j logger and
stamps each line with `Activity.getExecutionContext().getInfo().getAttempt()`.

---

## A2. Mock server — every scenario knob

Start it:

```bash
python3 mock/mock_lender.py          # foreground, logs every request
python3 mock/mock_lender.py &        # background
```

Control endpoints — **no restart needed, the workflow reacts because branch
conditions live in config, not compiled code**:

```bash
curl localhost:9099/_state           # current knobs + live poll counters
curl localhost:9099/_reset           # restore ALL knobs to defaults + zero counters
curl "localhost:9099/_set?KEY=VALUE" # flip a knob (also zeroes counters)
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

The join is **settle-all**, not fail-fast (`LenderFlowWorkflowImpl.runParallel`).
Every launched branch is awaited even after a sibling has already failed —
`Promise.get()` parks the workflow coroutine, the branches keep running
concurrently, so the step still costs the slowest branch, not the sum.

Order of events inside one PARALLEL step:

1. **Launch.** Branches whose `condition` fails are skipped and recorded as
   `SKIPPED` before anything is scheduled.
2. **Retry, per branch.** Each branch exhausts its own retry policy before its
   promise settles — the call's `retry` block if present, else the lender's
   `defaultRetry`. `BUSINESS_REJECT` is never retried, whatever config says.
3. **Merge.** Every *successful* branch's `extract` is merged into ctx, in
   declared order (deterministic on replay). A partial fan-out now contributes
   its successful branches instead of contributing nothing.
4. **Decide, once.** The gate below runs after every branch has settled.

| Config | Where | Meaning |
|---|---|---|
| `mandatory` | call | `false` → a technical failure on this branch does not by itself stop the flow. Default `true`. |
| `retry` | call | Per-branch retry override (`initialIntervalSeconds`, `backoffCoefficient`, `maximumAttempts`, `nonRetryableErrorTypes`). Falls back to `defaultRetry`. |
| `minSuccess` | step | Quorum: minimum successful branches required. `0` = no quorum rule. |
| `onBranchFailure` | step | `FAIL` (default) or `CONTINUE` when the gate is not met. |

Gate precedence:

1. **Any branch `REJECTED`** → outcome `REJECTED`, flow stops. A decline is an
   answer, not a fault: neither `mandatory:false` nor `onBranchFailure:CONTINUE`
   overrides it.
2. **All mandatory branches OK and quorum met** → `PROCEED`, or
   `PROCEED_DEGRADED` if a tolerated branch failed.
3. **Gate not met** → `onBranchFailure`: `FAIL` gives outcome `FAILED`,
   `CONTINUE` gives `PROCEED_DEGRADED`.

Keys written into ctx, queryable and usable in any downstream step `condition`:

| Key | Example |
|---|---|
| `<step>.<call>.outcome` | `parallelChecks.panValidation.outcome` = `SUCCESS` / `FAILED` / `REJECTED` / `SKIPPED` |
| `<step>.decision` | `parallelChecks.decision` = `PROCEED` / `PROCEED_DEGRADED` / `STOP` |
| `<step>.successCount`, `<step>.branchCount` | `3`, `4` |
| `<step>.failedCalls`, `<step>.rejectedCalls` | `mobileAadhaarLink`, `` |

`failedAt` still records `PRECHECK/parallelChecks`; the per-branch verdicts are
in the history (`PRECHECK/parallelChecks/panValidation`) and in ctx.
Sibling activities are not proactively cancelled — that is deliberate, since
their results are now kept.

In the shipped federal config, `mobileAadhaarLink` is `mandatory: false` with its
own 4-attempt retry, so it is the branch to poke for a degraded run:

```bash
# Tolerated branch dies -> 4 attempts, then COMPLETED with decision PROCEED_DEGRADED
curl "localhost:9099/_set?mobileAadhaarHttp=500"
```

| | Technical failure | Business rejection |
|---|---|---|
| Failure type | `TECHNICAL_FAILURE` | `BUSINESS_REJECT` |
| Retries | per-call `retry`, else `defaultRetry` (3 attempts, 2s → 4s) | **none** (non-retryable) |
| Time to fail | ~6s+ | immediate |
| Tolerable? | yes, via `mandatory:false` / `onBranchFailure:CONTINUE` | never |
| `outcome` | `FAILED` (or flow continues, degraded) | `REJECTED` |

### Knob combinations (reference)

The runnable scenarios live in [section 6](#6-test-scenarios) — these are just
the mock settings each one uses, in case you want to combine them by hand.

```bash
# Happy path
curl "localhost:9099/_set?panStatusCode=E&dedupeFlag=N&loanStatusSucceedsAfter=2"

# Business rejection — REJECTED, ZERO retries (non-retryable)
curl "localhost:9099/_set?panStatusCode=F"

# Technical failure — 3 retries with backoff, then the step PARKS
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

## A3. Temporal CLI — the commands that matter here

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

## A4. Troubleshooting

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

## A5. Architecture — how Temporal is working here

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
│  DynamicStageActivity…  │      │                         │
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
| 4 | Worker (activity poller) | Activity type `STP.loanStatus` misses the typed map → `DynamicStageActivityImpl` → `StageActivityImpl.execute()` — the **only** place a socket opens (see §11.1) |
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

---

## A6. Design decisions — the two a reviewer will push on

Both of these are places where the POC deliberately picked one side of a
tradeoff. Neither choice is obviously right; both need a different answer in
production, and the reasoning is more useful than the verdict.

### A6.1 UI naming — dynamic activity vs `setSummary`

**The problem.** The Temporal UI labels every box in the timeline graph from
`ActivityTaskScheduled.activityType.name`. With one generic `execute` method
serving every configured call, a 14-step flow rendered as `LoadConfig` followed
by thirteen identical `Execute` boxes. The config-driven design was invisible in
the exact tool you use to demo it.

**What the POC does — dynamic activity.** `Workflow.newUntypedActivityStub`
(`:70`) takes the activity type as a *string argument* instead of deriving it
from a Java method, so `activityName()` (`:300`) supplies it from config:

```
acts.execute("PRECHECK.nameDobCheck", CallResult.class, req)   // :122
acts.executeAsync("PRECHECK.panValidation", ...)               // :150
```

That string lands verbatim in the scheduling command and therefore in history,
so the graph now reads `PRECHECK.nameDobCheck`, `PRECHECK.panValidation`,
`FUND_TRANSFER.disburse`. On the worker side,
`registerActivitiesImplementations` builds a `Map<String, executor>` keyed by
`@ActivityMethod` name with the first letter capitalised (`LoadConfig`,
`Execute`). Dispatch per task:

```
task.activityType.name  ->  map lookup
  hit   -> POJOActivityImplementation      (reflective typed call)
  miss  -> DynamicActivityImplementation   (if registered)
  miss  -> FAIL: Activity Type "X" is not registered with a worker
```

`DynamicStageActivityImpl` receives `EncodedValues` — a lazy wrapper over the
raw payloads — deserializes arg 0 to a `CallRequest` via the DataConverter, and
delegates to the single `StageActivityImpl.execute`. One HTTP executor, N names.
`loadFlowSkeleton` / `loadStageKnobs` stay typed: few calls, already meaningful,
and typed keeps the
`LenderConfig` return compile-checked.

Two SDK constraints, confirmed in the 1.25.2 bytecode:

- **Exactly one** `DynamicActivity` per worker —
  *"An implementation of DynamicActivity is already registered with the worker"*.
- The lookup miss is a real activity-task failure that **burns a retry attempt**,
  not a no-op.

**The alternative — `setSummary` (server/SDK 1.27+).** A `UserMetadata` message
rides alongside the command rather than inside its identity fields:

```
ActivityTaskScheduled {
  activityType  = { name: "Execute" }          <- UNCHANGED
  user_metadata = { summary: "PRECHECK / parallelChecks / panValidation" }
}
```

UI >= 2.34 labels the timeline from the summary. Same release adds
`WorkflowOptions.setStaticSummary/setStaticDetails` (labels the row in the
workflow *list*) and `Workflow.setCurrentDetails(markdown)`, which could be fed
from `currentStage`/`currentStep` so the workflow page shows live progress with
no query call. Note: summaries are Payloads, so a payload codec encrypts them
too and the UI then needs a codec server to render them.

**Prod verdict: typed activity + `setSummary`.** Three reasons, worst first.

1. **Rolling-deploy safety.** Task queue dispatch is not type-aware — any worker
   polling `lih-federal-work` can receive any activity task. Mid-rollout, an old
   pod without `DynamicStageActivityImpl` picks up `PRECHECK.panValidation` and
   fails it as unregistered, burning attempts against the configured
   `maximumAttempts`. A slow rollout can fail real loans. A constant `Execute`
   type is handled by every worker version and cannot hit this.
2. **Every change to the naming scheme is replay-breaking.** Activity type is
   part of the command the SDK compares on replay, so it is history shape.

   *Not* at risk: renaming a call in the config store. `loadFlowSkeleton` is
   pinned at `:98` and `activityName()` derives from it, so an in-flight loan
   replays its own recorded skeleton and regenerates its own old names. Store
   renames reach only loans that start afterwards.

   At risk: **code** changes to how the name is built — the typed -> dynamic
   migration itself, and any later reformat (`PRECHECK.nameDobCheck` ->
   `PRECHECK/nameDobCheck`, dropping the stage prefix). A loan that scheduled
   `Execute` on Monday and replays on Thursday's worker hits
   `NonDeterministicException`. It does not fail the loan — the workflow task
   fails and retries forever, so the loan **stalls** with repeating
   `WorkflowTaskFailed` until the worker is rolled back or the run is reset.
   Quieter and worse than a clean failure.

   The amplifier: 72h e-sign and 7-day park waits mean a loan's lifetime
   routinely spans several deploys, so "in-flight during a naming change" is the
   normal case, not an edge case. Migrate behind
   `Workflow.getVersion("dynamic-activity-names", DEFAULT_VERSION, 1)` —
   `DEFAULT_VERSION` keeps old runs on `Execute` — since draining only works for
   short flows. A `setSummary` label is metadata, so none of this applies to it.
3. **Metric cardinality.** The worker tags `temporal_activity_execution_latency`,
   `temporal_activity_schedule_to_start_latency`,
   `temporal_activity_execution_failed` and friends with `activity_type`.
   Dynamic naming gives ~30 lenders x ~15 calls = ~450 types x ~8 metrics x
   histogram buckets — survivable, but it grows *whenever someone edits config*
   and per-type alert rules silently miss the new names.

Mitigating factor for the current code: `activityName` is
`stage.name + "." + callName` — config-derived, **no per-instance data**. That
keeps the set bounded. The fatal version of this pattern puts `loanRequestId` or
similar in the type name; do not let anyone "improve" it that way.

**Dynamic is still correct when** you are below server 1.27 with no upgrade
scheduled — which is where this POC sits, and where the demo value of a
self-documenting timeline outweighs the three risks above.

**The prod shape:**

```java
ActivityOptions.newBuilder(base)
    .setSummary(stage.name + "/" + step.name + "/" + callName)   // UI label
    // attempt suffix required — activity IDs must be unique within a workflow
    .setActivityId(stage.name + "/" + callName + "#" + attempt)
    .build();
```

plus one custom metric emitted from inside `StageActivityImpl` with a curated
label set (`lender`, `call`, `outcome`) — so you own the cardinality budget
instead of inheriting it from config edits.

### A6.2 Config snapshot — loaded once per execution

> **Resolved — see [CONFIG-DESIGN.md](CONFIG-DESIGN.md).** Everything this
> section argued for is now implemented: the two-tier split became a three-plane
> one (skeleton pinned at start / knobs per stage / **binding resolved inside the
> activity on every attempt**), config moved to an external store, and the
> "explicit escape hatch" became `onFailure: PARK` plus the `retryStep` signal.
> The analysis below is kept because it is still the reasoning, and §11.2's
> "where the snapshot hurts" cases are exactly what scenarios S6 and S7 now fix live.

**Correcting a comment in the code.** The original `StageActivity.loadConfig`'s
javadoc said
config is loaded once "so a replay months later uses the SAME config". Replay
safety does not come from loading once — it comes from config being read **inside
an activity**. Activity results are recorded in history and never re-executed on
replay; ten config-loading calls would each replay their own recorded bytes.
Determinism holds either way.

What loading once at `:65` actually buys is **semantic coherence**: one loan
follows exactly one version of the flow, start to finish. That is a business
decision, not a determinism constraint. The hard invariant underneath: **config
can only affect a workflow's future, never its past.**

**Where the snapshot hurts.**

- *Endpoint rotation while loans are parked.* Loan parked on `eSignWait` for
  days. Lender moves `/v1/loan-status` -> `/v2/loan-status`; config is fixed.
  The loan resumes, calls v1, 404s, exhausts retries, `FAILED` — and so does
  every other in-flight loan. Fleet-wide manual remediation from a change that
  should have been a non-event.
- *Timeout tuning cannot reach live flows.* Bump
  `transport.defaultTimeoutSeconds` and in-flight loans ignore it: the stub from
  `activityOptions(lender)` (`:304`) is built once, so `startToClose` **and** the
  retry policy are frozen at start. This is the most common real "change a
  running workflow" request and it is pure knob-turning with no shape risk.

**Where the snapshot saves you.**

- *A config edit breaks a data dependency mid-flight.* Insert `panVerify` before
  `pledgeCardSend` and make the latter use `${ctx.panRefId}`. A loan already past
  PRECHECK reloads at the FUND_TRANSFER boundary and renders an unresolved
  template into a money-moving call. `ctx` is a flat accumulator with no schema
  check; nothing in the code catches this.
- *A rename becomes a double disbursal.* Rename step `disburse` -> `fundTransfer`.
  Cosmetic — except `_requestId` and the `history` records key off step names,
  and so does anything the lender dedupes on. A loan that already disbursed under
  the old name no longer matches. Renaming a JSON field is not obviously a code
  change; that is precisely what makes it dangerous.
- *Audit.* "What flow did loan X follow?" is one activity result at the top of
  history. Per-stage reload means stitching N reads and hoping each recorded a
  version.
- *New failure surface.* Every reload point is a dependency on the config store.
  It retries, so nothing corrupts — but an outage stalls flows at every stage
  boundary and burns actions.

**Recommendation — two tiers, which the schema almost already has.**

| tier | fields | lifetime |
|---|---|---|
| flow **shape** | `lender.stages` — order, conditions, extraction, step names | pinned at start |
| operational **knobs** | `lender.transport`, `lender.defaultRetry` — base URL, headers, timeouts, retry, poll interval | re-read per stage, stub rebuilt |

That split kills both "where it hurts" cases without opening either "where it
saves you" case. Cheap to implement because `LenderConfig` already separates
`transport`/`defaultRetry` from `stages`: add a `loadTransport(lenderSlug)`
activity called at each stage boundary and move stub construction inside the
stage loop.

Then add an **explicit escape hatch** for the rare shape change — an
`@UpdateMethod adoptConfigVersion(String version)` that swaps the pinned config
for remaining steps only. Properties that matter: an operator opts in per
workflow, it is recorded in history so the audit trail shows where the version
switched, and nobody triggers it by saving a file. Have it reject the update
unless every `${ctx.*}` referenced by the *remaining* steps already resolves —
that check is what stops the data-dependency scenario above.

**For a loan already on the wrong path, the tool is not config — it is Reset.**
Reset to an event before `loadFlowSkeleton`; activities re-execute from there, so the
reset run picks up current config. Do not build reload semantics to approximate
this.

**If flows run for days, revisit Continue-As-New at stage boundaries.** You get a
fresh config snapshot per stage *and* bounded history (see limit #2 above), with
the version switch visible as a run boundary instead of buried mid-history.
Bigger change — `ctx` and `history` become carry-over input — but it is the shape
this converges on.


---

## A7. Alternative — Beeceptor instead of the local mock

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
