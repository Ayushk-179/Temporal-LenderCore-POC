# Externalised, per-stage, hot-editable lender config

**Status: POC.** This demonstrates what is possible. It is deliberately not
production machinery — see [§7 What was NOT built](#7-what-was-not-built-and-why)
for the list of things a production version needs and this does not have.

---

## 1. The problem

Before this change, the entire lender contract was one classpath blob
(`src/main/resources/lender-config.json`) loaded once per execution by
`StageActivity.loadConfig()`.

- Any change — a rotated endpoint, a timeout tune, one extra payload field —
  was a jar rebuild and a worker redeploy.
- In-flight loans could never benefit from a fix. A loan parked on `eSignWait`
  for days resumed against a frozen snapshot; if the lender had moved `/v1` to
  `/v2`, it 404'd, exhausted retries and `FAILED` — fleet-wide, from a change
  that should have been a non-event.
- Recovering a stuck loan meant a Temporal Reset or manual compensation. There
  was no way to fix the broken thing and resume.

Goal: config in an external store, editable without redeploy, where an edit can
change a workflow's **future but never its past**, and where fixing a bad payload
template is enough to un-stick a live loan.

---

## 2. The core idea

> **Replay safety does not come from *pinning* config. It comes from config being
> read inside an *activity*.** Activity results are recorded in event history and
> are never re-executed on replay, so what the store says later is irrelevant to a
> replay of what already happened.

That reframes the whole problem. The work is not to add `Workflow.getVersion()`
gymnastics — it is to **shrink the replay-visible config surface** to the smallest
thing the workflow actually needs, pin that, and let everything else move freely.

Config splits into two planes by a single question: *can this field change the
number, order or identity of the commands the workflow emits?*

| plane | contents | read by | when | replay risk |
|---|---|---|---|---|
| **Skeleton** (shape) | stage list, step list, `mode`, `condition`, signal names, poll params, parallel gate (`mandatory`, `minSuccess`, `onBranchFailure`), `onFailure` | workflow | **once**, at start | pinned in history — cannot shift under a loan |
| **Knobs** | retry policy, activity timeouts | workflow | **per stage** | recorded per stage; a fixed call sequence |
| **Binding** (contract) | `method`, `path`, `queryParams`, `headers`, `request` template, `extract`, `successWhen`/`terminalWhen`/`businessRejectWhen`, `baseUrl`, `staticHeaders`, HTTP timeout | **activity** | **every attempt** | **none** — never enters the workflow |

The skeleton is what replay compares. The binding never crosses into the
workflow, which is *why* payload, URL, header, extraction and predicate changes
are safe on a running loan — not by careful sequencing, but structurally.

**The payoff:** an activity sitting in retry backoff resolves its binding again on
the next attempt. Editing a payload template repairs a loan mid-flight, with no
redeploy, no signal and no reset.

Knobs are the awkward middle. `RetryOptions` and `startToCloseTimeout` are
`ActivityOptions` — the *workflow* sets them when scheduling, so they cannot be
resolved activity-side. Re-reading them once per stage is the compromise: a knob
change reaches every step not yet scheduled. A step already retrying keeps the
policy it was scheduled with; the escape hatch for that is PARK + `retryStep`.

---

## 3. The store — normalised by stage

Seven collections of small documents, addressed by a colon-separated id.
`ConfigStore` is a dumb key/value interface (`read` / `readAll` / `write`), so a
backend is ~60 lines and all the meaning lives in `ConfigResolver`.

| collection | id | holds |
|---|---|---|
| `lenders` | `federal` | display metadata |
| `pointers` | `federal` | **the one mutable document** — which versions are live |
| `flows` | `federal:v1` | stage NAMES, in order |
| `stages` | `federal:v1:PRECHECK` | **one document per stage** — its steps, modes, conditions, gates |
| `bindings` | `federal:b1:PRECHECK.nameDobCheck` | **one document per call** — method, path, request template, extract, predicates |
| `transports` | `federal:t1` | baseUrl, static headers |
| `stage_knobs` | `federal:t1:PRECHECK` | **one document per stage** — retry policy, timeouts, per-call overrides |

### Why normalise by stage

1. **Editing.** Changing one lender call rewrites one small document instead of a
   9 KB blob, so two people editing two different calls cannot clobber each
   other. `editBindingPath` in the test suite is a one-document update, and that
   is not a test convenience — it is the production edit.
2. **Reading.** The three resolution points map 1:1 onto documents.
   `loadStageKnobs` is exactly one `stage_knobs` read; a call attempt is exactly
   one `bindings` read. No sub-document extraction, no over-fetching a lender's
   entire contract to make one call.
3. **Browsing and maintenance.** `db.bindings.find({stage: "FUND_TRANSFER"})`
   shows precisely what money movement touches. As a blob you cannot ask that.
4. **It fixed a real Mongo problem.** callKeys contain dots
   (`PRECHECK.nameDobCheck`). As a blob those were *field names*, which is why
   the first cut had to store opaque JSON strings. Normalised, a callKey is an
   `_id` *value*, so documents are genuine BSON — queryable, indexable, and
   editable in `mongosh` like anything else.

Two design points worth defending:

- **Stage order lives in exactly one place** — the `stages` array of the `flows`
  document. Reordering is an edit to one array, not a risky reshuffle of nested
  structure.
- **Stage knobs are materialised, not inherited.** The seeder folds the lender
  default into each stage document, so `FUND_TRANSFER`'s retry policy means
  exactly what it says with no hunt for an inherited default. The cost is that
  changing the default for every stage is N edits — the right trade for something
  humans read and maintain. Per-call exceptions are a **list** of
  `{callKey, retry, timeoutSeconds}`, not a map, for the same dotted-field-name
  reason as above.

### Two backends, one shape

| | | |
|---|---|---|
| `FileConfigStore` | default | `CONFIG_STORE=file`, `CONFIG_DIR=./config-store` |
| `MongoConfigStore` | opt-in | `CONFIG_STORE=mongo`, `MONGO_URI`, `MONGO_DB` |

Each `:` in an id becomes a directory level, so the file layout reads like the
collections:

```
config-store/pointers/federal.json
config-store/flows/federal/v1.json
config-store/stages/federal/v1/PRECHECK.json
config-store/bindings/federal/b1/PRECHECK.nameDobCheck.json
config-store/stage_knobs/federal/t1/FUND_TRANSFER.json
```

Neither backend caches — deliberately. That is what lets you edit a document
while a loan is in retry backoff and *see* the next attempt pick it up.
Production caches immutable versions forever and re-reads only the pointer.

The Mongo seeder creates indexes on `{lenderSlug, bindingVersion, stage}` and
friends, so the per-stage queries above stay cheap.

`Runner seed` normalises the legacy `lender-config.json` into these collections
(6 stage docs, 17 binding docs, 6 stage_knobs docs across two lenders). After
seeding, that file is only a seed source and offline fallback. The split was
mechanical — every field landed in exactly one plane, and nothing in the existing
config resisted classification. That is itself a finding: the blob was already
two planes wearing one hat.

`Runner show <slug>` prints the whole flow stage by stage, with each step's mode,
gate, and resolved endpoint — the visualisation the normalised shape makes possible.

---

## 4. Template rendering

`request` is now a `JsonNode` tree instead of `Map<String,String>`, so lender
payloads no longer have to be flat strings.

```json
"request": {
  "requestId": "${ctx._requestId}",
  "applicant": {
    "name": "${ctx.customerName | upper}",
    "pan":  "${ctx.pan}"
  },
  "amount":       "${num:ctx.loanAmount ?: 150000}",
  "consent":      "${bool:ctx.consentGiven ?: \"true\"}",
  "items":        "${json:ctx.itemsJson}",
  "?middleName":  "${ctx.middleName}"
}
```

- string interpolation in any leaf (previous behaviour, preserved)
- typed leaves `${num:…}`, `${bool:…}`, `${json:…}` produce real JSON types
- defaults `?:`, function pipeline `| upper | lower | trim | substr(a,b)`
- `"?key"` is dropped entirely when unresolved
- the `path` is templated too, so `/loan/${ctx.loanId}/status` is expressible

**Strict mode is on by default and fixes a real defect.** The previous renderer
did `ctx.getOrDefault(key, "")` — an absent key silently became an empty string
in the request body. On a disbursal call that reaches the lender. Now an
unresolved reference with no default throws, is retried (giving an operator a
window to fix it), and then parks with a diagnostic naming the exact template and
binding version.

**This is not an expression engine, on purpose.** No evaluation, no reflection,
no method invocation. The function list above is the entire vocabulary. Adopting
SpEL/MVEL here would turn hot-editable config into a remote-code-execution path
into the worker pods.

Response side (`JsonPaths`): dot paths, array indexing (`$.data.items[0].ref`),
and a regex extractor (`regex:$.message:/REF-(\d+)/`). Regex is confined to
**extraction** — the right tool for scraping a reference id out of a lender's
free-text message, the wrong tool for building a payload.

---

## 5. Recovery: PARK + `retryStep`

`onFailure: PARK` (seeded for all SYNC and POLL steps) means: when an activity
exhausts its retry ladder, don't fail the execution — record
`AWAITING_INTERVENTION` and wait on the `retryStep` signal. The operator fixes the
document and signals; the step re-runs and resolves the corrected config.

The full ladder, cheapest first:

| # | Symptom | Fix | Reaches |
|---|---|---|---|
| 1 | wrong `baseUrl` / header / HTTP timeout | edit `transport-t1.json` | next attempt — self-heals mid-backoff |
| 2 | payload wrong; lender changed contract | edit `bindings-b1.json` | next attempt of the **same in-flight activity** |
| 3 | `extract` path or `successWhen` wrong → false failure | edit binding | next attempt |
| 4 | retries exhausted, step parked | fix binding, signal `retryStep` | current run, no reset |
| 5 | retry policy itself too tight | edit knobs, then #4 | current run |
| 6 | shape wrong (missing/reordered step) | publish new skeleton → applies to **new** loans. Stuck loan: Temporal **Reset** | past is not editable |
| 7 | unrecoverable | terminate + manual compensation | — |

**PARALLEL steps park too**, and re-run only the branches that failed. The earlier
objection — re-running a fan-out would re-fire branches that already succeeded —
is answered by tracking per-branch outcomes across rounds, so a branch that
succeeded is never called twice however many times the step parks. Verified by
`parkedFanout_reRunsOnlyTheFailedBranch_onRetryStep` and, live, by the mock's own
request counts.

Every mode that can suffer a *technical* failure now parks: SYNC, POLL and
PARALLEL. `AWAIT_SIGNAL` does not, because it is already a wait with its own
timeout.

**`BUSINESS_REJECT` never parks** — a lender decline is an answer, and no config
edit turns it into a yes.

The park window is `parkTimeoutHours`, **72h by default — the same three days as
the e-sign wait**. There is no reason for an operator to get less time to rescue a
loan than a customer gets to sign one. On timeout the loan fails with
`TECHNICAL_FAILURE`.

### A loan that did not complete FAILS the execution

Returning a `FlowResult` with `outcome=FAILED` left the workflow green and
"Completed" in the Temporal UI, so a broken loan was indistinguishable from a
disbursed one in the list — which is where anyone looks first. A non-COMPLETED
outcome now throws, and the loan shows **red**.

The failure type keeps the two cases apart:

| outcome | UI status | failure type |
|---|---|---|
| COMPLETED | Completed | — |
| REJECTED | **Failed** | `BUSINESS_REJECT` |
| FAILED | **Failed** | `TECHNICAL_FAILURE` |

The whole `FlowResult` rides along as failure details, so the final context and
step history stay visible on the UI's failure panel and `status()` still answers.

**The trade-off, stated plainly:** with declines and outages both showing as
`Failed`, "failed workflow count" is no longer an alerting signal on its own — it
has to be filtered by failure type, or a busy day of rejections looks like an
incident. That was a deliberate choice for UI clarity over metric purity.

---

## 6. What is proven, and where

`mvn test` — 25 tests, all green.

| claim | test |
|---|---|
| a binding edit mid-backoff heals a live loan, no redeploy | `bindingEditedMidRetry_healsTheLoan_withNoRedeploy` |
| retries exhaust → park → fix → `retryStep` → complete | `stepParks_whenRetriesExhaust_thenRetryStepResumesWithFixedConfig` |
| a decline never parks, even inside a fan-out | `businessReject_neverParks` |
| a failed fan-out branch parks, and only it re-runs | `parkedFanout_reRunsOnlyTheFailedBranch_onRetryStep` |
| a fan-out keeps sibling results while parked | `parallelBranchFailure_keepsSuccessfulSiblingResults_thenParks` |
| **history replays clean after config changed mid-flight** | `replayIsClean_afterTheBindingChangedMidFlight` |
| a shape change does not leak into a running loan, but does apply to the next one | `skeletonChange_doesNotLeakIntoARunningExecution` |
| an operator can supply a value config cannot reach | `patchContext_suppliesAValueConfigCannotReach_thenTheParkedStepCompletes` |
| identity keys cannot be patched | `patchContext_refusesIdentityKeys` |
| **reading config from workflow code breaks replay** | `ConfigDeterminismTest#readingConfigFromWorkflowCode_breaksReplayWhenTheConfigChanges` |
| **reading it through an activity does not** | `ConfigDeterminismTest#readingConfigThroughAnActivityReplaysCleanlyUnderTheSameChange` |
| nested/typed payloads, defaults, optional keys | `TemplateRendererTest` |
| strict mode fails loudly instead of sending `""` | `strictMode_failsLoudly_insteadOfSendingAnEmptyString` |
| array indexing + regex extraction | `extraction_supports*` |

The tests use a **real** `FileConfigStore` in a temp directory, seeded by the
**real** `ConfigSeeder` and read through the **real** `ConfigResolver`. Only the
HTTP call is faked, so the seeder, the two-plane split and the per-attempt
resolution are genuinely under test rather than stubbed around.

The replay test is the important one: it publishes a new binding version while a
loan is parked, lets the loan finish, then feeds the recorded history to
`WorkflowReplayer`. A clean replay is evidence, not assertion.

### Verified live

Against `temporal server start-dev` + `mock/mock_lender.py`, with the worker
started **once and never restarted**:

1. A loan ran all three stages; every step recorded its `bindingVersion`.
2. `FUND_TRANSFER.pledgeCardSend.path` was edited to a broken path while the loan
   was parked on `eSignWait` → the step retried, exhausted, and **parked** instead
   of failing.
3. The path was fixed and `retryStep` sent → the loan **completed**.
4. `PRECHECK.nameDobCheck.request` was rewritten flat → nested with typed leaves
   and a deliberately missing field → strict mode caught it and parked with
   `unresolved template ${ctx.branchCode} … [binding b1]`.
5. A `?:` default was added and `retryStep` sent → the loan **completed**.

---

## 6b. Code review findings

A review pass over the implementation found two real defects and two robustness
gaps. All four are fixed; they are recorded here because each is a class of bug
this architecture makes *easier* to introduce, not just a one-off.

### Fixed — a typo'd context reference was shipped as a literal

`${cts.pan}` (note the typo) resolved to the **literal string `"cts.pan"`** and
was sent to the lender in the payload. `lookup()` treated any unrecognised bare
token as a literal, so there was no way to misspell a context key loudly.

Head position now requires `ctx.<key>` or a quoted `"literal"`; a bare token
throws and names the fix. A bare token *after* `?:` is still a literal, which is
what keeps `${num:ctx.loanAmount ?: 150000}` readable. This is the same class of
bug as the old `getOrDefault(key, "")` — a silently wrong value in a money-moving
request — and hot-editable config makes it more likely, not less, because nobody
compiles a config edit.

### Fixed — a config typo could hang a loan instead of failing it

`Predicates.evaluate` throws `IllegalArgumentException` on an unknown `op`, and
`step.mode` could be null. Both were raw exceptions thrown from **workflow**
code, and the Java SDK retries a failed *workflow task* forever — so a typo in a
condition produced a loan that hung silently with no outcome and a repeating
`WorkflowTaskFailed`, which is far worse than a clean failure.

Both are now converted to non-retryable `CONFIG_ERROR` `ApplicationFailure`s, and
a workflow-side predicate carrying a response `path` instead of `ctx` is rejected
with a message saying where it belongs.

### Documented — a stray `retryStep` short-circuits the next park

`retryStepSignal` is not cleared when a step parks, only when the wait completes.
That is deliberate: it means a `retryStep` sent while the step was still on its
last retry is honoured a moment later when it parks, which is the realistic ops
sequence. The cost is that a stray signal sent when nothing is parked will
release the *next* park immediately. It is self-correcting — the step re-runs and
re-parks — but confusing in a log, so it now logs a warning saying exactly that.

### Deliberate — the command sequence DID move, once

Making a non-COMPLETED loan fail the execution changes the final command from
`CompleteWorkflowExecution` to `FailWorkflowExecution`. That is a
**replay-breaking change**: deploying it while a loan is mid-flight and heading
for a rejection would stall that loan on a non-determinism error.

The fixture net caught it — `business-reject.json` was the one history whose
terminal event changed, and it was regenerated deliberately. For a POC this is
fine. In production this change needs `Workflow.getVersion` or a drain before it
reaches workers serving in-flight loans. The other five fixtures were unaffected,
which is the useful signal: adding park support to PARALLEL did **not** move the
command sequence for flows that never hit that path.

### Verified — nothing else moved

All six committed history fixtures in `src/test/resources/histories/` replay
cleanly against the reviewed code (`ReplayCompatibilityTest`). Logging, guards
and the renderer fix changed no commands, which is the property that matters:
a change that moved the sequence would stall every in-flight loan on the next
deploy.

---

## 6c. Logging

The POC logs every config-driven decision with the value it was based on, because
"why did this step do that" is the question a config-driven flow gets asked most.

- **Request/response**: method, URL, headers, and full raw bodies both ways, per
  attempt. `LOG_RAW=false` suppresses bodies — they carry PAN, Aadhaar reference
  and mobile, and raw-body logging must not ship (§7.16).
- **Config provenance**: every attempt logs `RESOLVED callKey=... binding=...`,
  which makes the hot-config mechanism visible — you can watch an edit land
  between attempt 2 and attempt 3.
- **Conditions and predicates**: `ctx.customerId BLANK [actual=CIF-1001] -> false`,
  and `successWhen: $.status EQUALS SUCCESS | actual=SUCCESS -> true`.
- **Gate decisions**: branch launches with their effective retry, the settle-all
  join, and the single go/no-go verdict.
- **Recovery**: park, the exact `Runner signal <id> retryStep` command to run, and
  the release.

One implementation detail that is not optional: the workflow logs through
`Workflow.getLogger(...)`, which is **replay-aware**. A plain slf4j logger in
workflow code re-emits every line on every replay, so a parked loan's log would
read as though the flow ran dozens of times. Activities do not replay and use a
normal logger, stamped with the Temporal attempt number.

See `RUNBOOK.md` §13 for annotated output, including the `mvn -q` gotcha that
silences all of it.

---

## 7. What was NOT built, and why

Everything below is deliberately absent. This is a POC; each item is real work
and most of it is where a production schedule actually goes.

### Correctness and safety

1. **Stable identity. This is the #1 real risk.** `_requestId` and every history
   record key off config step *names*. Rename a step in a hot-editable store and
   the lender's dedupe key changes — that is a **double-disbursal path**.
   Production needs immutable `uid`s (ULIDs) assigned at creation, with `name` as
   a mutable label. Normalisation exists mostly to make this possible. Ship it
   **before** anything becomes editable.
2. **No publish-time validation.** No JSON Schema, no approval, no audit trail.
   "Editable without redeploy" removes the compiler, code review and the test
   suite from the change path; a production version has to rebuild them in a
   publish pipeline.
3. **No dataflow validator.** Nothing checks that a `${ctx.x}` referenced by step
   *N* is actually produced by a step < *N* on every reachable path. Strict mode
   catches it at runtime (and parks); a validator would catch it at publish time.
   This is the highest-value missing gate.
4. **Interpreter drift.** Adding a new `mode`, predicate `op` or template function
   is a **code** change wearing config's clothes. An old worker replaying a
   workflow whose config now says `mode: "SAGA"` throws `CONFIG_ERROR` on a live
   loan. Production needs a `minInterpreterVersion` on each config version and a
   publish gate against the lowest deployed worker build id.
5. **No skeleton/binding compatibility check.** A binding version must cover every
   `callKey` of every live skeleton version. Bindings should be additive-only with
   respect to any skeleton still in flight.
6. **Config is not a time machine.** A binding fix does not un-send a bad request.
   If the lender already accepted a malformed payload, only compensation fixes it.

### Operational

7. **No caching.** Every read hits the store, and `skeleton()` is N+1 reads
   (one `flows` document plus one per stage). Fine for a POC — one read per stage
   boundary and two per activity attempt — and better for demoing. Wrong for
   production: cache immutable versions forever and give the mutable pointer a
   short TTL.
8. **The store is a new outage surface.** Config used to ship in the jar and could
   not fail. Production needs: infinite cache on immutable versions, last-known-
   good on local disk, the bundled `lender-config.json` as a fallback, and
   fail-closed-to-last-pointer behaviour.
9. **Pointer TTL makes rollout non-atomic.** For the cache window, some pods use
   the old binding and some the new. Harmless for idempotent reads; **dangerous
   for a money-moving call whose payload changed.** Recommendation: mark bindings
   `hot: true|false` per call and default `FUND_TRANSFER`-class calls to pinned.
10. **No search attributes.** Without `lenderSlug` / `flowVersion` /
    `bindingVersion` / `currentStage` indexed, you cannot answer "which loans are
    affected by the binding I just published", and targeted remediation is
    impossible. Mandatory for production, trivial to add.
11. **Dynamic activity type names.** `RUNBOOK.md` §11.1 already argues for a
    constant activity type plus `ActivityOptions.setSummary`. Hot-editable config
    makes the existing risk worse: renaming a call in config mid-flight is a
    non-determinism error, and mid-rollout an old pod without
    `DynamicStageActivityImpl` fails unregistered types against real loans.
12. **PARK has no bound in practice.** It waits 7 days and there is no alerting,
    no dashboard, and no cap on how many times a step may park.
13. **`retryStep` is a blunt instrument.** One signal releases whichever step is
    parked; production wants the step identity in the signal payload. `skipStep`
    does not exist. `patchContext` **does** now exist (see the recovery ladder) —
    but it is a signal, so it is fire-and-forget with no synchronous rejection,
    it has no schema or type checking on the values it writes, and authorisation
    is "whoever can reach the Temporal frontend". Production wants it as an
    `@UpdateMethod` with validation, an actor identity, and a reason string.
14. **A parked-then-retried step generates a new `_requestId`.** Deliberate
    re-fire is usually what you want, but it means the idempotency key changes —
    which is only safe once #1 is done and the key is derived from stable uids.

### Data and governance

15. **`ctx` is an untyped flat `Map<String,String>` with no namespacing.** Two
    calls extracting `status` collide silently. Hot config turns that into an
    edit-time hazard. Namespace extracts as `<step>.<key>` and declare a per-call
    output schema so a validator has something to check.
16. **PII.** `ctx` carries PAN, Aadhaar reference and mobile; `rawResponseSnippet`
    is free text; hot config lets an editor add an `extract` for any response
    field and push new PII into permanent history. Needs a per-call `contextKeys`
    whitelist (send only declared keys into the activity), a payload codec,
    snippet redaction and a publish-time PII lint.
17. **No RBAC, no per-lender scoping, no blast-radius limit.** Anyone who can
    write the store can change a disbursal payload for every lender.
18. **Cost.** One extra `loadStageKnobs` activity per stage, plus parking waits
    and longer retry ladders, all consume Temporal Actions. Small per loan;
    multiply by ~30 lenders × volume before signing off.

---

## 8. Viability

**Viable, and the right direction — but the hard part is not Temporal.**

- *Replay and versioning*: **low risk**. The pre-existing design (config read
  inside an activity) was already the correct foundation. The change here is
  subtractive — shrink what the workflow sees. No `getVersion` patching is needed
  for config changes; Worker Versioning (build ids) covers interpreter changes.
- *Template engine*: **low risk**. Bounded and fully unit-tested.
- *Config service and publish pipeline*: **this is the project.** With
  engineers-only editing it is far smaller than a UI-driven low-code platform,
  but the validator (§7.2–7.5) and a shadow-render gate are where the schedule
  actually goes.

Rough sequencing to production, one engineer:

| phase | work | est |
|---|---|---|
| 0 | stable uids (§7.1); constant activity type + `setSummary` (§7.11); namespaced ctx (§7.15) | ~1 wk |
| 1 | caching, fallback, store resilience (§7.7–7.9) | ~1 wk |
| 2 | publish pipeline: schema, dataflow validator, interpreter gate, audit, rollback (§7.2–7.5) | ~2–3 wks |
| 3 | ops: search attributes, alerting on PARK, richer operator updates (§7.10, 7.12–7.13) | ~1.5 wks |
| 4 | PII whitelist, codec, RBAC (§7.16–7.17) | ~1.5 wks |

≈ **7–8 weeks**, and phase 2 is the one that slips.

**A recommendation worth arguing about:** given that editors are engineers, keep
the *authoring* surface in a **Git repo of per-lender JSON** with CI compiling and
publishing into Mongo, rather than building CRUD + a UI. You get review, diff,
audit, approval and rollback for free, and "no redeploy" is preserved — publishing
a config version is not a worker deploy. Mongo then holds only compiled immutable
artefacts plus the one mutable pointer.

---

## 9. Try it

```bash
mvn -q compile

# file-backed (default, no infra)
mvn exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="seed"
mvn exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="show federal"

# Mongo-backed
docker run -d --name mongodb -p 27017:27017 mongo:7.0
export CONFIG_STORE=mongo
mvn exec:java -Dexec.mainClass=com.rupeek.poc.temporal.Runner -Dexec.args="seed"
docker exec mongodb mongosh lender_config --eval \
  'db.bindings.find({lenderSlug:"federal", stage:"FUND_TRANSFER"})'
```

For the live hot-edit demo (break config under a running loan, then fix it
without restarting the worker), see **RUNBOOK.md §12**.
