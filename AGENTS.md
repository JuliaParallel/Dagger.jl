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

7. **Keep type-stable and type-unstable paths at the right stability level.**
   If both kinds of path exist for an operation (e.g. typed kernel execution
   vs. dynamically-typed planning), consider whether they need to be
   *separate* paths: don't force the dynamic path to specialize per signature
   (compile-time explosion, tuple re-boxing), and don't erase types on the
   path where the compiler genuinely uses them (kernel invocation, argument
   moves). A function barrier at the boundary lets each side be what it is.
