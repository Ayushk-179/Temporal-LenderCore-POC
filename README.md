# Fed Mendix flow on Temporal — POC

One generic workflow. All lender specifics in `src/main/resources/lender-config.json`.
Adding a lender is a config document, not a BPMN file plus 11 Java files.

> **Not compiled.** This skeleton was written without a build environment. Expect
> to fix import/signature nits on first `mvn compile`. The design is the deliverable;
> the code is a head start.

---

## What the flow does

| Stage | Steps | Temporal primitive being demonstrated |
|---|---|---|
| PRECHECK | `nameDobCheck` → `dedupeCheck` → 4 parallel checks | async fan-out with a settle-all join: every branch retries per config, successful branches merge their extracts, then one config-driven go/no-go gate. 2 of the 4 branches are conditionally skipped by config |
| STP | `dedupe` → `customerCreation`* → `dedupeRetry`* → `customerInfo` → `accountInfo` → `loanStatus` | conditional steps, durable poll loop via `Workflow.sleep` |
| FUND_TRANSFER | `eSignWait` → `pledgeCardSend` → `fundTransferStatus` | `Workflow.await` on a signal (days-long human wait), poll → park → manual-retry signal |

`*` conditional — governed by `condition` in config, evaluated against workflow context.

---

## Run it

```bash
# 1. Temporal dev server (single binary, no Cassandra, no Elasticsearch)
brew install temporal          # or: https://docs.temporal.io/cli
temporal server start-dev      # UI at http://localhost:8233

# 2. Worker on the Federal task queue
mvn -q compile
mvn exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="worker federal"

# 3. Start a flow
mvn exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="start federal LR-1001"

# 4. Inspect live progress without touching a DB
mvn exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="status LR-1001"

# 5. Release the e-sign wait (this is what PUT /v1/disbursement becomes)
mvn exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="signal LR-1001 pledgeCardReady"

# Tests — the 1-hour poll loop runs in milliseconds
mvn test
```

---

## Beeceptor mocks

Create endpoint `fed-mock` → base URL `https://fed-mock.free.beeceptor.com`.
Free tier allows a limited number of rules; the 11 below fit. All are `200 OK`
unless the scenario says otherwise — the workflow branches on **body**, not
status code, except where `businessRejectWhen` names a `statusCode` field.

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
| 12 | GET | `/stp/loan-status` | `{"loanStatus":"PENDING"}` — flip to `SUCCESS` mid-demo |
| 13 | POST | `/ft/pledge-card` | `{"statusCode":"200","bpmNumber":"BPM-9001"}` |
| 14 | GET | `/ft/status` | `{"status":"PENDING"}` — flip to `{"status":"SUCCESS","utr":"UTR123456789"}` |

### Scenario matrix

Edit the mock body, restart nothing. The workflow reacts because the branch
condition lives in config, not in compiled code.

| Scenario | Change | Expected |
|---|---|---|
| Happy path | defaults | `COMPLETED`, UTR in context |
| Business rejection | `/precheck/pan` → `{"panStatusCode":"F"}` | `REJECTED`, **zero retries** (non-retryable) |
| Technical failure | `/stp/dedupe` → HTTP 500 | 3 retries with 2s→4s→8s backoff, then `FAILED` |
| Existing customer | `/stp/dedupe` → `"ddupeFlag":"Y"`, `"lenderCustomerId":"CIF-9"` | `customerCreation` and `dedupeRetry` both `SKIPPED` |
| Aadhaar checks not needed | `/precheck/dedupe` → `"aadhaar_checks_needed":"false"` | parallel group runs 2 branches instead of 4 |
| Poll then succeed | `/stp/loan-status` `PENDING`, flip to `SUCCESS` after 2 polls | advances on the poll after the flip |
| Poll exhaustion | leave `/stp/loan-status` on `PENDING` | after 30 polls parks on `retryLoanStatus`; `status` shows `AWAITING_MANUAL_RETRY` |
| Durability | kill the worker mid-flow, restart | resumes at the same step, no state lost |
| Idempotency | run `start federal LR-1001` twice | second is rejected by `WorkflowIdReusePolicy` |
| New lender | `start demobank LR-2001` (worker on `lih-demobank-work`) | different stage graph, zero code change |

For the poll demos, temporarily drop `pollIntervalSeconds` to `10` and
`maxAttempts` to `4` in config so it fits in a live demo.

---

## Demo order (what to show, in this sequence)

1. **Durability.** Start a flow, let it enter the loan-status poll, `ctrl-C` the
   worker, restart it. Show the Temporal UI resuming mid-poll. Nothing else lands
   as hard — lead with it.
2. **Config-only lender.** `start demobank LR-2001`. A completely different stage
   graph runs on the same JAR. No deploy, no BPMN, no code.
3. **Time-skipped tests.** `mvn test` — a one-hour poll loop asserted in
   milliseconds. Then ask how that flow is tested today.
4. **Business vs technical failure.** Flip PAN to `F` (no retries, `REJECTED`)
   then dedupe to HTTP 500 (backoff retries, `FAILED`). Same config surface,
   correctly different behaviour.
5. **The `status` query.** Live stage history with no Mongo read and no Operate.
6. **The deletion slide.** What we no longer build:

| New-arch component | Replaced by |
|---|---|
| `PendingCorrelation` store | workflow state |
| `contextSnapshot` in Mongo | `loadConfig` activity result in event history |
| `poll_lock` in Redis | `Workflow.sleep` in the poll loop |
| `ASYNC_RESUME` → re-enqueue | the loop never stopped |
| `exec_lock:{slug}:{lr}:{stage}` | workflow ID + `WorkflowIdReusePolicy` |
| SQS FIFO work queue per lender | task queue per lender |
| DLQ + replay admin | failed workflow stays queryable; reset and retry |
| `lender-async-service` (whole service) | — |

---

## What this POC does NOT prove — say this out loud

- **No Tier-2 handler is implemented.** Every call here is declarative field
  mapping. Production Federal needs XML/JAXB customer creation, PGP for the
  Aadhaar checks, SOAP for UN-name, image collage generation, and Drools. Those
  are handler code under any engine. The honest claim is *"the orchestration and
  the transport layer become config; the transforms stay code."*
- **Payload sizes are unrealistic.** Temporal's per-payload limit is ~2 MB.
  Pledge-card and account-info payloads carry base64 images in production and
  will exceed it. Real design passes S3 keys, not bytes.
- **No versioning strategy demonstrated.** Changing this interpreter while flows
  are parked at `eSignWait` for days needs Worker Versioning or patching, plus
  replay tests in CI. Flag it as required work, not solved work.
- **Not a load test.** Nothing here says anything about throughput.
- **Fed CLM and fed-driver are out of scope entirely.**

Volunteering these four is what makes the rest credible.
