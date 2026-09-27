# Async Request Coalescer

A small production-oriented Python component for **coalescing concurrent requests for the same key**.

If multiple callers request the same expensive operation while that operation is already running, the coalescer executes the operation once and makes all callers await the same `asyncio.Task`.

This project was built as a hands-on exercise for understanding Python's asynchronous execution model:

* `asyncio`
* coroutines
* `Task`
* `Future`
* `await`
* event-loop scheduling
* cancellation propagation
* `asyncio.shield()`
* asynchronous lifecycle management
* deterministic concurrency testing

---

## Problem

Consider an application where several requests simultaneously need the same expensive resource:

```text
Request A ──► fetch_user(42)
Request B ──► fetch_user(42)
Request C ──► fetch_user(42)
```

Without request coalescing, the application may execute the same operation three times:

```text
fetch_user(42)
fetch_user(42)
fetch_user(42)
```

For expensive operations such as:

* database queries
* external API calls
* LLM requests
* metadata retrieval
* filesystem/network operations

this can create unnecessary load and duplicate work.

The coalescer instead maintains a mapping of currently running operations:

```text
_in_flight

"user:42" ─────────► Task
                         │
                         ▼
                    fetch_user(42)
```

Subsequent callers for the same key reuse that Task.

---

## Core Invariant

The fundamental invariant is:

> For a given key, there can be at most one in-flight operation.

For example:

```text
"user:42"
     │
     ▼
   Task A
     │
     ▼
fetch_user(42)
```

If five callers request `"user:42"` concurrently:

```text
Caller 1 ─┐
Caller 2 ─┤
Caller 3 ─┼──► Task A ──► operation()
Caller 4 ─┤
Caller 5 ─┘
```

`operation()` executes exactly once.

Different keys are independent:

```text
"user:42" ──► Task A ──► fetch_user(42)
"user:99" ──► Task B ──► fetch_user(99)
```

Both operations can execute concurrently.

---

## Architecture

The implementation uses a dictionary of in-flight Tasks:

```python
self._in_flight: dict[str, asyncio.Task[T]]
```

The lifecycle is:

```text
get(key, operation)
       │
       ▼
look for key in _in_flight
       │
   ┌───┴────┐
   │        │
 found    missing
   │        │
   │        ▼
   │    create Task
   │        │
   │        ▼
   │    store Task
   │        │
   └────┬───┘
        │
        ▼
 asyncio.shield(task)
        │
        ▼
     result
```

The Task executes `_run()`:

```text
Task
 │
 ▼
_run()
 │
 ├── await operation()
 │
 └── finally:
       remove Task from _in_flight
```

---

# Why a Task?

A `Task` is used instead of a bare `Future`.

A `Future` represents an eventual result:

```text
Future
  │
  ├── pending
  │
  └── done → result / exception
```

But a Future does not execute the operation itself.

A Task combines:

```text
Task
 ├── drives a coroutine
 └── represents its eventual result
```

Therefore:

```python
task = asyncio.create_task(operation())
```

both schedules the operation and gives us an awaitable handle that multiple callers can share.

Conceptually:

```text
Task
 │
 ├── executes operation()
 │
 └── stores eventual result/exception
```

Multiple callers can then do:

```python
await task
```

and all observe the same completion.

---

# Coroutine vs Task vs Future

One of the main goals of this project was to distinguish these three concepts.

### Coroutine

Calling an async function produces a coroutine object:

```python
operation()
```

It does not mean the operation is currently executing.

```text
async def operation():
    ...

operation()
    │
    ▼
coroutine object
```

### Task

A Task schedules and drives a coroutine:

```python
task = asyncio.create_task(operation())
```

```text
Task
 │
 └── drives coroutine
```

### Future

A Future represents an eventual completion:

```text
Future
 │
 ├── PENDING
 │
 └── DONE
      ├── result
      └── exception
```

A Task is also Future-like and, in Python's asyncio implementation, `Task` derives from `Future`.

The practical distinction is:

> A Future represents an eventual result; a Task also drives the coroutine that produces that result.

---

# What `await` Actually Does

`await` does not mean "block the thread."

For example:

```python
result = await task
```

If the Task is not finished, the current coroutine suspends.

The event loop can then execute other Tasks.

Conceptually:

```text
Task A
 │
 └── await Task B
          │
          ▼
      suspended
          │
          │
          ├── Task C runs
          ├── Task D runs
          └── Task E runs
          │
          ▼
      Task B completes
          │
          ▼
      Task A resumes
```

This is cooperative concurrency.

---

# Asyncio Race Conditions

Asyncio does not eliminate race conditions.

However, coroutine execution is cooperative rather than arbitrarily preemptive.

This sequence:

```python
task = self._in_flight.get(key)

if task is None:
    task = asyncio.create_task(...)
    self._in_flight[key] = task
```

contains no `await` between the lookup and insertion.

Therefore another coroutine cannot normally interleave between those operations.

But this would introduce a race:

```python
task = self._in_flight.get(key)

if task is None:
    await something()
    task = asyncio.create_task(...)
    self._in_flight[key] = task
```

Two callers could both observe:

```text
_in_flight[key] == None
```

before either inserts its Task.

The general rule is:

> In asyncio, look for suspension points when reasoning about shared mutable state.

---

# Task Lifecycle and Cleanup

A completed Task must not remain in `_in_flight`.

Otherwise:

```text
_in_flight
    │
    └── "user:42" → completed Task
```

Future requests could incorrectly reuse stale state, and the dictionary could retain unnecessary objects.

The worker therefore owns its lifecycle:

```python
async def _run(...):
    current = asyncio.current_task()

    try:
        return await operation()
    finally:
        if self._in_flight.get(key) is current:
            del self._in_flight[key]
```

The `finally` block executes on:

* successful completion
* normal exception
* cancellation

This makes cleanup part of the Task's lifecycle rather than something individual callers are responsible for.

---

# Why Identity Checking Matters

Cleanup uses:

```python
self._in_flight.get(key) is current
```

rather than simply:

```python
del self._in_flight[key]
```

Consider:

```text
Task A
 │
 └── fails
     │
     └── cleanup delayed/interleaved

Task B
 │
 └── starts for same key
     │
     └── stored in _in_flight
```

Task A must never accidentally delete Task B.

The identity check guarantees:

> Only the Task currently registered for the key can remove that registration.

---

# Failure Semantics

A failed operation is still a shared result.

If three callers share one Task:

```text
Caller A ─┐
Caller B ─┼──► Task ──► RuntimeError
Caller C ─┘
```

all callers observe the same failure.

The operation still executes only once:

```python
assert operation_count == 1
```

After failure, cleanup removes the key:

```text
_in_flight
    │
    └── key removed
```

This allows a later request to retry.

Therefore:

```text
First attempt
    │
    └── failure
          │
          ▼
       cleanup
          │
          ▼
    new request
          │
          ▼
    new Task
```

A failed Task is not reused indefinitely.

---

# Cancellation

Cancellation introduces an important distinction between:

1. cancellation of an individual caller
2. cancellation of the shared operation

Without protection:

```python
await task
```

allows cancellation to propagate into the shared Task.

That creates this situation:

```text
Caller A ── cancelled
              │
              ▼
        Shared Task ── cancelled
              │
              ├── Caller B fails
              └── Caller C fails
```

That is undesirable when B and C still depend on the operation.

---

# `asyncio.shield()`

The coalescer therefore awaits the shared Task through:

```python
await asyncio.shield(task)
```

Now cancellation of one caller does not automatically cancel the shared operation:

```text
Caller A
   │
   └── shield ── cancelled

Caller B
   │
   └── shield ────────────────┐
                              │
                              ▼
                         Shared Task
                              │
                              ▼
                         operation()
```

If A disconnects, B can still receive the result.

Important:

> `asyncio.shield()` does not make the Task uncancellable.

It only prevents cancellation from that particular awaiting caller from propagating into the protected Task.

Explicit cancellation of the shared Task can still cancel it.

---

# Testing Strategy

The tests focus on behavioral invariants rather than implementation details.

Important cases include:

### Concurrent requests share work

```text
3 callers
   │
   ▼
1 operation
```

Assert:

```python
assert operation_count == 1
```

### Completed work is removed

After successful completion:

```python
assert key not in coalescer._in_flight
```

### Failed work is removed

Even when the operation raises:

```python
assert key not in coalescer._in_flight
```

### Failure is shared

All concurrent callers receive the same exception.

### Failed operations can be retried

After cleanup:

```text
Task A fails
   ↓
key removed
   ↓
Task B created
```

### Caller cancellation does not cancel shared work

One caller can cancel its own wait while another caller continues receiving the shared result.

---

# Production Lessons

Although this is a small component, it demonstrates several patterns used in larger asynchronous systems.

### 1. Shared work needs explicit ownership

The coalescer creates the Task, so the Task lifecycle belongs to the coalescer.

### 2. Awaiting a Task creates dependency relationships

Multiple coroutines can depend on one Task.

Understanding those relationships is essential when reasoning about cancellation.

### 3. Cleanup belongs to lifecycle boundaries

The `finally` block ensures cleanup regardless of how the operation completes.

### 4. Cancellation is part of API design

Cancellation is not merely an implementation detail.

A library needs to decide whether cancellation means:

```text
cancel my wait
```

or:

```text
cancel the underlying work
```

Those are different semantics.

### 5. Async concurrency is cooperative

When reviewing async code, identify suspension points:

```python
await ...
```

They are often the places where execution can interleave.

### 6. Tests should prove invariants

Instead of testing internal implementation mechanics, test properties such as:

```text
one key → one active operation
failure → cleanup
completion → cleanup
new request after cleanup → new operation
caller cancellation → shared work survives
```

---

# Running the Project

Create the environment and install development dependencies using your preferred Python environment manager.

Run the tests:

```bash
uv run pytest -v
```

For stdout/debug output:

```bash
uv run pytest -s
```

---

# Project Status

This project intentionally remains small.

The purpose is not to create a complete request-coalescing library. The purpose is to use a realistic component to understand the mechanics behind production asynchronous Python systems.

The next exercise builds on these concepts with an **async worker/job queue**, introducing:

* `asyncio.Queue`
* producer/consumer architecture
* bounded concurrency
* backpressure
* worker lifecycle
* graceful shutdown
* cancellation
* task supervision

Those concepts are closer to the execution models found in larger event-driven systems.
