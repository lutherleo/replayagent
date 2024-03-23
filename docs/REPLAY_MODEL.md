# The replay model

This document explains exactly what replayagent guarantees, how replay works, and what
your workflow code must (and must not) do. It is the reference for anyone writing
workflows or reviewing a code change to one.

## 1. Two kinds of code

| | Workflow code | Activity code |
|---|---|---|
| What | `fn(ctx, input) -> result`, registered with `@workflow` | any function, registered with `@activity` |
| Runs | many times: once per *workflow task*, always from the top | once per *attempt* |
| May do I/O | **no** | yes (LLM calls, HTTP, files, databases) |
| Must be deterministic | **yes** | no |
| Its outputs are | *commands* (schedule this, wait for that) | a JSON result or an error, recorded in history |

Workflow code is the *decision logic*: which model to ask next, which tool to call,
whether to wait for a human. Activities are the *effects*. LLM calls (`llm.chat`) and
tool calls (`tool.<name>`) are just activities.

## 2. History

Every workflow has an append-only history in SQLite. Events are numbered `seq = 1..N`.
Workflow code produces **command events**, each with a command index `cmd = 0..M` in the
order the code issued them:

| Command event | Issued by | Resolved by |
|---|---|---|
| `ActivityScheduled` | `ctx.activity(...)`, `ctx.start_activity(...)` | `ActivityCompleted` / `ActivityFailed` |
| `TimerStarted` | `ctx.sleep(s)`, `ctx.start_timer(s)` | `TimerFired` |
| `SignalWaitStarted` | `ctx.wait_signal(name, timeout=...)` | `SignalConsumed` / `SignalWaitTimedOut` |
| `SideEffectRecorded` | `ctx.side_effect(fn)`, `ctx.now()` | (value stored in the event itself) |
| `VersionMarker` | `ctx.get_version(change_id, min, max)` | (version stored in the event itself) |

The engine adds bookkeeping events that are not commands: `WorkflowStarted`,
`ActivityStarted` (one per attempt), `ActivityAttemptFailed` (with the computed
`retry_at`), `SignalReceived` (a signal entering the mailbox), `CancelRequested`, and one
terminal event (`WorkflowCompleted`, `WorkflowFailed`, `WorkflowCancelled`).

Run `replayagent history <id>` to see one; `make demo` prints a real one.

## 3. A workflow task

A workflow task re-executes the workflow function **from the beginning** against the
current history:

```
cursor = 0
for every durable call the code makes:
    if cursor < number of command events in history:
        compare the call with history[cursor]          # determinism check
        if the call blocks: return the recorded outcome if there is one,
                            otherwise SUSPEND
    else:
        record a NEW command (it is not in history yet)
        if the call blocks: SUSPEND
    cursor += 1
```

Suspending raises an internal `BaseException` that unwinds the function. The worker then
appends the new commands in one transaction, starts any new activities, and waits. When
an outcome arrives (an activity finishes, a timer fires, a signal is matched) it is
appended and another task runs. Because the function is only ever *observing* history,
"recover after a crash" and "continue after an activity finished" are the same code
path: read history, re-run the function.

The function's final return value (or uncaught exception) becomes the terminal event.

### What blocks

`handle.result()`, `ctx.activity()`, `ctx.gather()`, `ctx.wait_any()`, `ctx.sleep()`,
`timer.wait()` and `ctx.wait_signal()` block. `ctx.start_activity()` and
`ctx.start_timer()` do not: they let you fan out, e.g. three LLM calls in parallel:

```python
handles = [start_chat(ctx, [user(q)]) for q in sub_questions]
answers = [response_of(h) for h in handles]   # suspends until all three are recorded
```

There is deliberately no non-blocking `handle.done()`: whether an outcome "is ready
yet" depends on timing, and a replay would see a different answer. `ctx.wait_any()`
instead returns the handle whose outcome was **recorded first in history**, which is
the same on every replay.

## 4. Determinism rules

Given the same history, workflow code must issue the same commands in the same order
with the same arguments. In practice:

* **Do not** read the clock, random numbers, environment variables, files or the
  network in workflow code. Use `ctx.now()`, `ctx.random()`, `ctx.uuid4()`,
  `ctx.side_effect(fn)` - or put it in an activity.
* **Do not** iterate over unordered collections when the order decides what to schedule
  (`set` iteration order of strings changes between processes with hash randomisation).
* **Do not** catch `BaseException` around durable calls. It would swallow the
  suspension signal; the engine detects this and blocks the workflow.
* Logging is fine: `ctx.logger` drops records while replaying so logs are not duplicated.
* Pure computation on recorded results (parsing JSON, building prompts) is fine.

## 5. How violations are detected

For every command the code re-issues during replay, the engine compares it with the
recorded event:

| Command | Compared fields |
|---|---|
| activity | type, activity name, SHA-256 of the canonical-JSON arguments |
| timer | duration |
| signal wait | signal name, timeout |
| side effect | label |
| version marker | change id (see §7) |

Retry policies and timeouts are **not** compared: changing them is allowed and the
recorded options keep applying to already-scheduled activities.

The engine also detects: code that finishes while history still has commands it never
re-issued, and code that swallowed a suspension. Each of these raises
`NonDeterminismError` / `WorkflowTaskError`. The worker **does not** record them as a
workflow failure - the history is left untouched and the workflow is parked as
`blocked`:

```
$ replayagent show <id>
status     blocked
error      NonDeterminismError: non-deterministic workflow 'x' at command #11: command type
           differs; history has wait_signal 'review' (...) but code issued activity
           'research.check_citations' (...)
```

Fix (or roll back) the code, then `replayagent resume <id>`.

## 6. Delivery guarantees

| Thing | Guarantee |
|---|---|
| Workflow progress | never lost once its transaction commits; never repeated |
| An activity whose `ActivityCompleted` was committed | never executed again |
| An activity attempt interrupted by a crash (`at_least_once`, the default) | executed again as a new, recorded attempt (`WorkerLost` in history, retried immediately, counts toward `maximum_attempts`) |
| An activity attempt interrupted by a crash (`at_most_once`) | never re-run; fails with non-retryable `ActivityOutcomeUnknown`, the workflow decides |
| Signals | delivered exactly once into history (inbox row deleted in the same transaction) |
| Timers / retry backoff | survive crashes (due times are in an indexed table written with the event) |

**At-least-once means a crash between "the side effect happened" and "the completion
was recorded" re-runs the activity.** To make such effects happen once, use the
activity's idempotency key, which is stable across retries and restarts:

```python
@activity
def charge(amount: int) -> str:
    key = activity_info().idempotency_key          # "<workflow_id>/<cmd>"
    db.execute("INSERT OR IGNORE INTO charges(key, amount) VALUES (?, ?)", (key, amount))
    return key
```

The crash tests demonstrate both halves: idempotent charges stay exact under every crash
point, and a deliberately non-idempotent counter is bumped twice by a crash at
`activity_after_execute` (`tests/test_crash_subprocess.py`).

## 7. Versioning

Changing workflow code while workflows are in flight is the usual way to break replay.
Wrap the change in `ctx.get_version`:

```python
draft = write_draft(...)
if ctx.get_version("citation-check", DEFAULT_VERSION, 1) >= 1:
    checked = ctx.activity("research.check_citations", draft)   # new in v1
review = ctx.wait_signal("review", timeout=...)
```

* A **new** execution reaches `get_version` beyond the end of its history, records
  `VersionMarker(citation-check=1)` and takes the new branch.
* An **old** execution has some other command at that position in history, so
  `get_version` returns `DEFAULT_VERSION` (-1) *without consuming a position* and the old
  branch replays exactly.
* An execution that recorded version 1 replays the marker and the new branch.
* If a recorded version is outside `[min_supported, max_supported]` - e.g. you raised
  `min_supported` while old executions still exist - the workflow is blocked with
  `VersionError` instead of silently taking the wrong branch.
* Once no old executions remain you may delete the `get_version` call; recorded markers
  that the code no longer asks for are skipped during replay.
* Markers are looked up **by change id** in the run of consecutive markers at the current
  position, not by exact position. Deleting one of two adjacent `get_version` calls, or
  swapping their order, leaves the other call's recorded version intact.
* The version is decided **once per change id and execution**. When one change touches
  several sites, call `get_version` with the same id at each: the first call decides
  (replayed marker, `DEFAULT_VERSION` for an older history, or a newly recorded
  `max_supported`) and every later call returns that value without recording another
  marker. An in-flight execution that predates the change therefore takes the old branch
  at *every* site, even sites it reaches after the end of its history. Each call still
  checks the decided version against its own `[min_supported, max_supported]`.

The repository proves this with real recorded histories: `tests/golden/research_v0.json`
was recorded by the research agent *before* the citation check existed
(commit `test(golden): record research agent history from the pre-citation-check code`),
and the current code still replays it. The same change without the guard
(`create_unguarded_app`) is rejected at command #11. Replaying recorded histories against
new code before deploying it is cheap:

```
replayagent replay --history tests/golden/research_v0.json --app my.module:create_app
```

`WorkflowStarted` also stores a hash of the workflow function's source and
`replayagent show <id> --app ...` flags `CHANGED since start` when it differs - a hint,
not a check (a refactor that changes no command is fine).

## 8. Cancellation

`replayagent cancel <id>` is a hard stop: the next worker tick appends
`CancelRequested` + `WorkflowCancelled`, abandons in-flight activities (their late
outcomes are ignored) and clears timers. There is no in-workflow cleanup hook; model
compensations explicitly if you need them (see the roadmap).

## 9. Cost of replay

Each task re-runs the function from the top, so a workflow with *n* sequential steps
costs O(n) work per task and O(n²) in total. Replay of a recorded step is a dictionary
lookup plus an argument hash, so this stays small for agent-sized workflows (measured by
`make bench`: one full replay of a 1,000-step history takes about 10 ms on the
development machine), but it is the main scaling limit. See README "Limitations".
