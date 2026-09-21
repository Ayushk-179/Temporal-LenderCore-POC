# Temporal LenderCore POC

A **config-driven workflow engine for lender integrations**, built on
[Temporal](https://temporal.io). One generic workflow runs *every* lender's flow.
Everything lender-specific — stage order, step types, endpoints, request
payloads, response parsing, retry policy, branch conditions — lives in an
**external config store**, not in the JAR.

The consequences this POC is built to prove:

| # | Claim | Where it is demonstrated |
|---|---|---|
| 1 | Onboarding a new lender/flow is **config only** — zero code, zero deploy | [RUNBOOK S11](RUNBOOK.md#s11) |
| 2 | A bad config change can be repaired **while a request is stuck**, with no redeploy and no Temporal reset | [RUNBOOK S7](RUNBOOK.md#s7), [S8](RUNBOOK.md#s8) |
| 3 | Config changes are **replay-safe** — they change a flow's future, never its past | [RUNBOOK S9](RUNBOOK.md#s9), `ReplayCompatibilityTest` |
| 4 | Durable execution — kill the worker mid-flow and it resumes exactly where it was | [RUNBOOK S10](RUNBOOK.md#s10) |
| 5 | Business rejection and technical failure are handled differently and are both visible in the UI | [RUNBOOK S2](RUNBOOK.md#s2), [S3](RUNBOOK.md#s3) |
| 6 | A failed parallel branch parks and only **it** re-runs — successful siblings are never re-fired | [RUNBOOK S4](RUNBOOK.md#s4) |
| 7 | Long-running waits (human e-sign, status polls) cost no thread and no pod | [RUNBOOK S1](RUNBOOK.md#s1) |

> **Status: proof of concept.** It builds and runs — `mvn -o test` is green
> (41 tests, 1 deliberate skip), and the full flow plus the live
> hot-config-edit demo have been run end to end against `temporal server
> start-dev` and the bundled mock. Production gaps are listed deliberately and
> in full in [CONFIG-DESIGN.md §7](CONFIG-DESIGN.md#7-what-was-not-built-and-why).

---

## Currently onboarded

**One real flow is onboarded today: `federal`** — a three-stage lender
disbursement flow.

| Stage | Steps | Temporal primitive exercised |
|---|---|---|
| `PRECHECK` | `nameDobCheck` → `dedupeCheck` → `parallelChecks` (4 branches) | async fan-out with a settle-all join, per-branch retry, config-driven go/no-go gate; 2 branches are conditionally skipped by config |
| `STP` | `dedupe` → `customerCreation`\* → `dedupeRetry`\* → `customerInfo` → `accountInfo` → `loanStatus` | conditional steps, durable poll loop via `Workflow.sleep` |
| `FUND_TRANSFER` | `eSignWait` → `pledgeCardSend` → `fundTransferStatus` | `Workflow.await` on a signal (days-long human wait), poll → park → manual-retry signal |

`*` conditional — governed by a `condition` predicate in config, evaluated
against workflow context.

A second lender, **`demobank`**, ships alongside it as a deliberately minimal
config-only example (`combinedPrecheck` → `sanction` → `disburse`). It exists to
show that a completely different stage graph runs on the same JAR with no code
change. Any further flow is onboarded the same way — see
[Onboarding a new flow](#onboarding-a-new-flow).

---

## Prerequisites

| Tool | Version | Needed for |
|---|---|---|
| JDK | 11 (the POM targets `release 11`) | build + run |
| Maven | 3.6+ | build + run |
| [Temporal CLI](https://docs.temporal.io/cli) | any recent | local server (`brew install temporal`) |
| Python | 3.8+ | the bundled mock lender API |
| Docker | any | **optional** — only for `CONFIG_STORE=mongo` |

```bash
java -version && mvn -version && temporal --version && python3 --version
```

Dependencies live in `~/.m2` after the first build, so every command below uses
`-o` (offline) to keep runs fast and reproducible.

---

## Quick start

### 1. Tests only — no server, no mocks

The fastest proof that the repo works. A one-hour poll loop is asserted in
milliseconds because `TestWorkflowEnvironment` skips time.

```bash
mvn -o test
```

### 2. Full local run — four terminals, in this order

```bash
# --- Terminal 0: build + seed the config store (once; no server needed) ---
mvn -o -q compile
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="seed"
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="show federal"

# --- Terminal 1: Temporal dev server (UI at http://localhost:8233) ---
temporal server start-dev

# --- Terminal 2: mock lender APIs (all routes, both lenders, port 9099) ---
python3 mock/mock_lender.py

# --- Terminal 3: worker. THIS ONE BLOCKS. Do NOT add -q, or you get no logs ---
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker federal"
```

> **Do not pass `-q` to the worker.** `exec:java` runs inside Maven's JVM, so
> SLF4J binds to Maven's logger and `-q` suppresses every INFO line. The flow
> still runs; you just cannot watch it.

### 3. Drive a flow (Terminal 4)

```bash
alias runner='mvn -o -q exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args'

runner "start federal LR-1001"              # start a request
runner "status LR-1001"                     # live stage history + config versions in use
runner "signal LR-1001 pledgeCardReady"     # release the human e-sign wait
runner "reset"                              # back to scenario zero (run after every scenario)
```

Watch it in the Temporal UI at <http://localhost:8233> — activity names are
config-derived (`PRECHECK.panValidation`, `FUND_TRANSFER.disburse`), so the
timeline reads like the flow instead of a stack of identical boxes.

---

## CLI reference

All commands are `mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="…"`.

| Command | What it does | Needs Temporal? |
|---|---|---|
| `seed` | Normalise `src/main/resources/lender-config.json` into the external store (7 collections) | no |
| `show <slug>` | Print everything the store currently serves for a lender, stage by stage | no |
| `worker <slug>` | Start a worker on that lender's task queue `lih-<slug>-work` (blocks) | yes |
| `start <slug> <requestId>` | Start a workflow (`lender-flow-<requestId>`) | yes |
| `status <requestId>` | Query live stage history and the config versions it is pinned to | yes |
| `signal <requestId> <name>` | Send a signal — `pledgeCardReady`, `retryStep`, `retryLoanStatus`, `retryFundTransferStatus` | yes |
| `patch <requestId> k=v [k=v …]` | Write values into a **running** workflow's context that config cannot reach | yes |
| `reset [requestId]` | Wipe + re-seed config, reset the mock, delete executions so IDs are reusable | yes |

**`reset` is not optional between scenarios.** Three stores drift during a demo —
the config store, the mock's scenario knobs, and Temporal's workflow IDs — and
all three must move together or the next scenario silently inherits the last
one's state.

---

## Configuration

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `CONFIG_STORE` | `file` | `file` or `mongo` |
| `CONFIG_DIR` | `./config-store` | root directory for the file-backed store |
| `MONGO_URI` | `mongodb://localhost:27017` | used when `CONFIG_STORE=mongo` |
| `MONGO_DB` | `lender_config` | used when `CONFIG_STORE=mongo` |

```bash
# Mongo-backed instead of file-backed — export in EVERY terminal
docker run -d --name mongodb -p 27017:27017 mongo:7.0
export CONFIG_STORE=mongo
mvn -o exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="seed"
```

### The store — seven collections

`lender-config.json` is **only a seed source**; it is never read while a flow
runs. `seed` normalises it into small, individually editable documents:

| Collection | Example id | Holds | Read when |
|---|---|---|---|
| `pointers` | `federal` | **the only mutable doc** — which versions are live | every lookup |
| `flows` | `federal:v1` | stage names, in order | once, at workflow start |
| `stages` | `federal:v1:PRECHECK` | one doc **per stage**: steps, modes, conditions, gates | once, at workflow start (**pinned**) |
| `stage_knobs` | `federal:t1:PRECHECK` | one doc **per stage**: retry policy, timeouts | **once per stage boundary** |
| `bindings` | `federal:b1:PRECHECK.nameDobCheck` | one doc **per call**: method, path, request template, extract rules, predicates | **every activity attempt** (hot) |
| `transports` | `federal:t1` | base URL, static headers | every activity attempt |
| `lenders` | `federal` | display metadata | — |

**Those three read points are the whole design.** Flow *shape* is pinned at
start so it cannot shift under a running request. Retry/timeout *knobs* re-read
at each stage boundary. The *wire contract* resolves inside the activity on
every attempt — which is why editing a payload repairs a request that is
mid-retry. Full rationale in [CONFIG-DESIGN.md](CONFIG-DESIGN.md).

Publishing a version is a **pointer flip**; rollback is flipping it back. Every
other document is immutable and version-named.

---

## Onboarding a new flow

No Java changes. Add documents to the config store (or add a lender block to
`lender-config.json` and re-`seed`), then run a worker on its task queue.

1. **`lenders/<slug>`** — display metadata.
2. **`transports/<slug>/t1`** — base URL, static headers, default timeout.
3. **`flows/<slug>/v1`** — the ordered list of stage names.
4. **`stages/<slug>/v1/<STAGE>`** — one document per stage, listing its steps.
5. **`stage_knobs/<slug>/t1/<STAGE>`** — retry policy and timeouts, plus
   per-call overrides.
6. **`bindings/<slug>/b1/<STAGE>.<step>`** — one document per call: method,
   path, request template, `extract`, `successWhen`, `businessRejectWhen`.
7. **`pointers/<slug>`** — point at `v1` / `b1` / `t1` to make it live.
8. Start a worker: `runner "worker <slug>"`, then `runner "start <slug> REQ-1"`.

### Step modes available to a flow

| `mode` | Behaviour |
|---|---|
| `SYNC` | one call, retried per the stage's retry policy |
| `POLL` | durable poll loop (`Workflow.sleep`) until a terminal predicate matches, `maxAttempts`, then `onExhausted` |
| `PARALLEL` | fan-out over `branches` with a settle-all join, `minSuccess` gate and per-branch conditions |
| `AWAIT_SIGNAL` | `Workflow.await` on a named signal for up to `timeoutHours` — costs nothing while waiting |

Every step also takes `condition` (skip when false), `onFailure` (`FAIL` or
`PARK`) and `parkTimeoutHours`. `PARK` suspends the step and waits for a
`retryStep` signal — that is the no-redeploy repair path.

### Request/response templating

```json
"request": {
  "applicant": { "name": "${ctx.customerName | upper}", "pan": "${ctx.pan}" },
  "amount":       "${num:ctx.loanAmount ?: 150000}",
  "consent":      "${bool:ctx.consentGiven ?: \"true\"}",
  "?middleName":  "${ctx.middleName}"
}
```

Nested JSON, typed leaves (`num:`, `bool:`), defaults (`?:`) and optional keys
(`?key`). An unresolved `${ctx.x}` with no default **fails loudly** rather than
silently sending an empty string. Response side supports dot paths, array
indexing (`$.data.items[0].ref`) and regex capture
(`regex:$.message:/REF-(\d+)/`).

---

## Repository layout

```
src/main/java/com/rupeek/poc/temporal/
  Runner.java                  CLI: seed | show | worker | start | signal | status | patch | reset
  workflow/                    the ONE generic workflow — interprets the config
  activity/                    HTTP activity; resolves the binding on every attempt
  config/model/                skeleton, knobs, binding, transport, predicates
  config/store/                FileConfigStore + MongoConfigStore behind one interface
  config/template/             request/response template renderer
  config/seed/                 lender-config.json → 7 collections
src/main/resources/
  lender-config.json           SEED SOURCE ONLY — not read at runtime
src/test/                      41 tests, incl. replay-compatibility + determinism
config-store/                  the file-backed store (generated by `seed`)
mock/mock_lender.py            local lender API stand-in with live scenario knobs
```

The mock serves both lenders on port 9099 and can be driven live:
`GET /_state` shows current knobs, `GET /_set?key=val` flips one, `GET /_reset`
restores the happy path — no restart needed, because the branch conditions live
in config rather than compiled code.

---

## Documentation map

| Document | Read it for |
|---|---|
| **README.md** (this file) | what the project is, setup, CLI, how to onboard a flow |
| **[RUNBOOK.md](RUNBOOK.md)** | operational guide: architecture, UI, and 13 numbered scenarios (S1–S13) with copy-paste commands and a one-command reset |
| **[CONFIG-DESIGN.md](CONFIG-DESIGN.md)** | design rationale: the skeleton/knobs/binding split, why it is replay-safe, and §7 — what was deliberately not built |

Start with the RUNBOOK if you want to *see* it work; start with CONFIG-DESIGN if
you want to know *why* it is shaped this way.

---

## Scope — what this POC does not prove

Stated up front, because the rest is only credible with these on the table:

- **No Tier-2 transform handlers.** Every call here is declarative field
  mapping. Real integrations need XML/JAXB, PGP, SOAP, document generation and
  rules engines. Those stay code under any engine. The honest claim is
  *"the orchestration and transport layer become config; the transforms stay code."*
- **Payload sizes are unrealistic.** Temporal's per-payload limit is ~2 MB;
  production payloads carrying base64 images will exceed it. A real design
  passes object-store keys, not bytes.
- **No versioning strategy demonstrated.** Changing the interpreter while flows
  are parked for days needs Worker Versioning or patching, plus replay tests in
  CI. Required work, not solved work.
- **Not a load test.** Nothing here says anything about throughput.
- **No caching, auth, RBAC or config governance.** Neither store caches, by
  design, so an edit lands on the next attempt and you can *see* it.

The full list, with reasoning, is
[CONFIG-DESIGN.md §7](CONFIG-DESIGN.md#7-what-was-not-built-and-why).
