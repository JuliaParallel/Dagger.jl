# Guidance for AI agents working on Dagger.jl

Lessons learned the hard way while working on this codebase. Follow them, and
when you learn a new hard-won truth of your own, record it here as a new
lesson.

## Lessons

1. **Record hard-won truths here.** When you discover something non-obvious
   about this codebase — a subtle invariant, a lifecycle rule, a performance
   cliff — add it to this file as a new lesson so the next agent doesn't have
   to rediscover it.

2. **Make tight commits.** One separable change per commit, with a commit
   message that explains the *why* (the cost being removed, the invariant
   being preserved). Tight commits make review and rebasing far easier.

3. **Avoid allocations when they're easily avoided.** Steady-state code paths
   (scheduling, planning, argument moves, task teardown) run per-task or
   per-argument; a single stray `Set`, closure capture, or splat there
   multiplies by the task count. Prefer reuse (pools, scratch buffers on
   long-lived state, `@reusable_vector`) and plain loops over closure
   pipelines. The `test/allocations.jl` suite enforces upper bounds — keep it
   green, and re-calibrate its bounds only for intentional shifts.

4. **Profile memory before and after.** When a change plausibly affects
   allocation behavior, measure it: deep warmup (~10 iterations plus `GC.gc()`),
   then compare `Base.gc_num()` deltas (count and bytes) over several runs,
   taking the minimum. Cite the numbers in the commit message.

5. **Consider type-stability in every change.** Check that hot-path locals
   infer concretely (`@code_warntype`, Cthulhu, or `--track-allocation`), that
   captured-and-reassigned variables aren't forcing `Core.Box`, and that
   containers have concrete element types.

6. **Use `@nospecialize` only on explicitly type-unstable paths** where the
   value does not need to be type-inspected by the compiler (e.g. it is only
   stored, passed through, or checked with runtime `isa`). Never use it where
   inference of the value's type feeds later dispatch or arithmetic — that
   just moves the dynamic dispatch somewhere less visible.

7. **Long-lived and pooled tasks must not inherit their creator's dynamic
   scope.** Julia copies `current_task().scope` (the `ScopedValues` chain) into
   every new `Task`. A task pool, a processor runner, or the eager scheduler
   task is created lazily by whichever call first needed it — often inside a
   `Dagger.with_options(...)` block — and then serves *every* later caller, so
   an inherited scope silently applies those options to unrelated work for the
   rest of the session. Clear it with `clear_task_scope!` before starting such
   a task (it can only be set before the task starts). Per-task option
   propagation is explicit (`get_propagated_options`), so these tasks want no
   ambient scope at all. Note that `task.scope` only exists on Julia 1.11+; on
   1.10 the `ScopedValues.jl` compat package hides the scope in `task.logstate`,
   so `clear_task_scope!` is version-split — always go through it rather than
   writing the field directly. The same reasoning applies to any other
   creation-time-captured state (e.g. `TaskLocalValue`s) on a reused task.

8. **A `Thunk`'s input slots hold *weak* references, so every path that skips
   creating a dependents edge must resolve the slot itself.** Input slots are
   normally resolved by walking the dependents edges
   (`schedule_dependents!` → `resolve_finished_input!`) when an upstream
   finishes. An upstream that is *already* finished gets no edge — there is
   nothing left to wait on — so nothing will ever resolve its slot, and the
   consumer keeps only a `WeakThunk`. With Thunk pooling that reference dies
   deterministically and immediately: the upstream is recycled and handed back
   out with a fresh id, so `unwrap_weak` returns `nothing` and
   `unwrap_weak_checked` asserts. Resolve eagerly, while the upstream is still
   alive and holding its result. The same applies to any future lifecycle
   shortcut: if you stop registering an edge, you have taken on the job that
   edge was doing.

9. **An exception on a pooled or detached task is a hang, not a crash.**
   Scheduler work runs on `ReusableTaskCache` tasks (whose loop only `@error`s
   a failed payload and moves on) and on bare `Threads.@spawn` (whose exception
   nobody fetches). Anything that has already been credited to
   `running_count` — or that some `fetch` is waiting on — is lost silently if
   an error escapes there, and the symptom is a session that hangs with one
   logged error, which is far harder to diagnose than a crash. Wrap such work
   so a failure becomes a *failed thunk* (`set_failed!` plus the matching
   `running_count` release), and attach the backtrace with `CapturedException`
   so the waiter sees where it actually broke.

10. **In a lock-free handshake, publish last and release last.** Where two
   sides coordinate through a single atomic counter (`ProcessRingBuffer`'s
   `count`), that counter is a *permission grant*, not a bookkeeping detail.
   The producer must fill the slot before incrementing (an incremented count
   entitles the consumer to read it) and the consumer must read the value out
   before decrementing (a decremented count entitles the producer to overwrite
   it). Getting the order wrong is invisible to assertions that only check
   counts and ranges — both sides stay perfectly self-consistent while values
   are silently lost or duplicated — and it only bites when the buffer is at a
   boundary, i.e. exactly under the backpressure the buffer exists to provide.
   Test such a structure by driving it from two real threads with a
   deliberately tiny capacity and checking the *sequence*, not the counts.

11. **Never iterate a shared collection across a point where you drop the
   lock.** Blocking calls in this codebase release and reacquire their lock
   (`wait(store.lock)`, `@lock`-guarded condition waits), and anything else may
   mutate the collection in that gap. Julia will not warn you: `Dict` iteration
   has no modification check, so an insert that triggers a rehash mid-iteration
   silently *revisits* some entries and *skips* others — measured, not
   theoretical. The revisit is the dangerous half, because re-processing an
   entry you already handled can block you on a resource you just consumed and
   hang the loop forever. Snapshot the keys into a `@reusable_vector` before
   the loop (steady-state allocation-free) and re-check membership per
   iteration; the resulting "entries added while we waited are picked up next
   round" semantics is well-defined, which the accidental version was not.

12. **Keep type-stable and type-unstable paths at the right stability level.**
   If both kinds of path exist for an operation (e.g. typed kernel execution
   vs. dynamically-typed planning), consider whether they need to be
   *separate* paths: don't force the dynamic path to specialize per signature
   (compile-time explosion, tuple re-boxing), and don't erase types on the
   path where the compiler genuinely uses them (kernel invocation, argument
   moves). A function barrier at the boundary lets each side be what it is.
