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

12. **Never wait by spinning on `yield()`.** Julia permanently marks a task
   `sticky` the moment it schedules any sticky task — an `@async`, which plenty
   of library code (Distributed's transport included) does on your behalf.
   `Base.enq_work` says so itself: *"XXX: Ideally we would be able to unset
   this."* A sticky task re-enqueues itself into its **thread-local**
   workqueue on `yield()`, and `trypoptask` drains that queue before it ever
   consults the multiqueue where `Threads.@spawn`ed tasks live. So a sticky
   task spinning on `yield()` stops its thread from picking up spawned work
   *ever again*, and once every default thread is spinning that way, nothing
   newly spawned can start at all — a permanent deadlock, not a slowdown.
   Spin briefly if you want a cheap hand-off, then `sleep`: only a real
   deschedule empties the thread's local queue. Two traps when testing this:
   `enq_work` places default threads at `threadpoolsize(:interactive)+1`
   onward, so pinning probes to tids `1:nthreads` leaves a default thread free
   and hides the bug completely; and the starved task's own `istaskstarted`
   flips the instant the spinners stop, so read it *before* releasing them.

13. **Keep type-stable and type-unstable paths at the right stability level.**
   If both kinds of path exist for an operation (e.g. typed kernel execution
   vs. dynamically-typed planning), consider whether they need to be
   *separate* paths: don't force the dynamic path to specialize per signature
   (compile-time explosion, tuple re-boxing), and don't erase types on the
   path where the compiler genuinely uses them (kernel invocation, argument
   moves). A function barrier at the boundary lets each side be what it is.

14. **`test/allocations.jl` measures scheduling overhead only while every
   task really is pinned.** Its bounds assume the pinned scope holds for the
   whole suite, but `allocate_array` tasks (the ones building each
   `let`-block DArray) take no `Chunk` inputs, so they carry *zero*
   data-transfer cost — nothing anchors them to a worker, and the scheduler
   spreads them the moment it can see per-processor load. The measured call,
   still pinned to worker 1, then pays to pull those chunks back, and a
   scheduler change shows up as a 5x allocation "regression" that is really
   cross-worker data movement. Build the fixtures inside the same
   `with_options(scope=...)` as the measurement. More generally: when this
   suite jumps on a scheduler change, first ask whether placement changed
   before hunting for a stray closure or box.

15. **A per-candidate term folded into a shared scalar is a no-op, and the
   tests won't tell you.** `estimate_task_costs!` compares candidate
   processors, so only terms that *differ per candidate* can change its
   decision. Accumulating one — e.g. summing every candidate's compute
   pressure and adding that total to `est_time_util` — leaves a constant
   offset that cancels out of both the comparison and the `sort!`, so the
   scheduler's ordering is bit-for-bit unchanged while the code reads as
   though the term is now considered. `test/scheduler.jl`'s cost assertions
   run against an *idle* scheduler where every pressure is zero, which makes
   summed and per-processor forms indistinguishable; a test that pins the
   term to one candidate and asserts the *other* one wins is what catches it.
   The behavioral check is cheaper still: 40 sleeping tasks over 4 workers
   land `[1 => 40]` when the term is dead and `10/10/10/10` when it is live.

16. **Only the acceleration x backend cross product exercises the data-movement
   paths, and one of the axes alone will pass while they are broken.** Two
   independent bugs in whole-object (`aliases_as_whole`) sparse tiles survived a
   green single-process-GPU suite *and* a green MPI-CPU suite, and both fell out
   the first time MPI x GPU ran. First: a host-to-device `move!` of a whole
   object only happens when tiles are *built* on the host and then placed on a
   device, which no single-axis configuration does. And such a hook must be
   defined at the `dep_mod` (5-argument) arity, because every GPU extension
   claims `move!(::TheirVRAMSpace, ::OtherSpace, ::AbstractArray{T,N},
   ::AbstractArray{T,N})` for the 4-argument form — a container method with
   unconstrained spaces is genuinely ambiguous with all of them (more specific
   in the value arguments, less specific in the space arguments), so it would
   need a tie-breaker per backend per space pair; nothing outside core defines
   the 5-argument form for arrays, and that is what every Datadeps copy path
   calls anyway. Second: `collect`'s gather tasks run wherever the caller's
   compute scope puts them, so under a GPU scope the `cat` tree runs *on the
   device* — and generic `cat` fills its output element by element, which is
   scalar indexing. Keep shared test bodies in a `test/array/*_defs.jl` file
   (as `stencil_defs.jl` and `sparse_defs.jl` do) and call them from all four
   entry points; `test/mpi_opencl.jl` makes the fourth cell cheap to run
   locally.

17. **Extensions of the same package must not reach into each other.** Load
   order between two extensions of one package is unspecified, so
   `Base.get_extension(Dagger, :MPIExt)` from another extension is a coin flip.
   When `AExt` and `A×BExt` both need to extend the same generic, declare that
   generic in core Dagger and let each extension add methods to it (see
   `inplace_mpi_parts` in `src/memory-spaces.jl`, the
   `mpi_device_direct`/`mpi_remap_space` hooks in `src/gpu.jl`, and the GPU
   processor types / `with_context` in `src/gpu.jl`). GPU×SparseArrays
   extensions import `CuArrayDeviceProc` (etc.) from Dagger and call
   `Dagger.with_context` — never `Base.get_extension(Dagger, :CUDAExt)`. The tempting
   shortcut — adding `B` to `AExt`'s trigger list — is worse than it looks: it
   makes `AExt` refuse to load at all until `B` is loaded, so MPI acceleration
   would have silently required SparseArrays.

18. **The scheduler needs DataStructures 0.19.** `Sch.jl` does
   `popfirst!(::PriorityQueue)`. That method exists only in DataStructures
   0.19; 0.18 resolves, loads, and then the scheduler throws on the first
   pop. Compat is `0.19` only — do not re-add `0.18` to satisfy a downstream
   pin. If a demo package pins 0.18, bump *that* package's compat (as the
   Jutul clone patch does), not Dagger's.

19. **Per-tile AMG can report `stats.solved` while `‖Ax−b‖` is O(1)–O(100).**
   `AMGPreconditioner` is block-diagonal: one V-cycle per diagonal tile,
   applied as Krylov `M` (`ldiv=false`). Left-preconditioned GMRES/BiCGStab
   then converge in the *preconditioned* residual. With many tiles that
   V-cycle is a weak additive-Schwarz operator, so Krylov stops while the
   true residual is huge (seen on Chan, VoronoiFVM penalty rows, and Jutul
   heat). BlockJacobi (exact tile LU) on the same layout is fine. Global AMG
   is `Blocks(n, n)` (one tile). Do not treat `stats.solved` as `Ax≈b` for
   block AMG; check the un-preconditioned residual. CG will also reject AMG
   as non-SPD — use GMRES.

21. **Reassigning a local to a value of a different type deoptimizes the
   *whole* body, not the assignment.** `arg = adopt_sparse_arg!(state, arg,
   deps)` in `_populate_one_arg!` looks like a cheap normalization, but
   inference must pick one type for `arg` over the entire method, so it
   widens to the join and every later use — `type_may_alias(typeof(arg))`,
   `supports_inplace_move`, the `ArgumentWrapper` construction,
   `get_or_make_arg_chunk!` — becomes a dynamic dispatch on a boxed value.
   The cost lands on the *common* path (the branch that never fires) and is
   invisible in a diff of the branch that does. Argument-processing code is
   per-argument per-task, so this is the worst possible place for it. Pass
   the new value forward into a function barrier instead of assigning it
   back; the callee then specializes per concrete type and the conditional
   is confined to choosing which call to make. `get_or_make_arg_chunk!`
   already exists for exactly this reason — extend the pattern rather than
   reintroducing the reassignment next to it.

22. **A re-tiling fallback needs the tile *backend*, and needs to outlive the
   call.** Making non-square-tiled operands work instead of erroring is two
   traps in one. First, allocate the destination through a tile-type-dispatched
   allocator (`allocate_tiled`): `DArray{T}(undef, part, dims)` gives dense
   tiles, so "repartition this sparse operator" silently becomes "densify this
   sparse operator" — an out-of-memory multiplier that a correctness test
   passes. A `DArray`'s type parameters do not record its tile type, so read it
   off a chunk (`chunktype(first(A.chunks))`). Second, `maybe_copy_buffered`
   frees its buffers when its body returns, which is only right when nothing
   escapes; a block preconditioner builds per-tile operators *from* the re-tiled
   tiles in tasks it never awaits, so it needs an ordinary array
   (`repartition`) whose lifetime is its own. And note that a *device* sparse
   tile supports neither end of a sub-range copy — reading it is scalar
   indexing, writing it would insert nonzeros into a CSC in place — so
   `copyto_view!` has to stage the whole thing on the host and re-upload
   through `move`, which is the one hook every GPU sparse extension already
   defines.

23. **A memory space must be keyed on the device, never on a handle that
   records current *ownership*.** `OpenCLExt.memory_space(::CLArray)` looked the
   buffer's `Managed.queue` up in Dagger's registered `QUEUES`. That field is
   OpenCL.jl's ownership tracking, not provenance: `convert(::CLPtr, ::Managed)`
   synchronizes and then re-stamps it with `cl.queue()` whenever the accessing
   task's queue differs, and `cl.queue()` is task-local *and lazily created*, so
   the first touch from a task that never ran `with_context!` re-stamps the
   buffer with a queue Dagger has never seen. The lookup then yields `nothing`
   and `CLMemorySpace(myid(), nothing)` does not even construct — a
   `MethodError` deep inside `aliasing`, arbitrarily far from the access that
   moved the ownership, on an array that was allocated perfectly correctly.
   Nothing about the memory changed; only a mutable bookkeeping field did. Key
   on `queue.device` (matched against `DEVICES`) instead. Suspect this shape
   whenever a space lookup fails for a value that a `Chunk` already carries a
   valid space for: the chunk recorded the space once, the value is being asked
   to re-derive it, and only the second one goes through the mutable field.

24. **`similar(::DArray)` must not fetch the source tiles.** Spawning
   `similar(chunk, T, sz)` per result tile looks like the way to preserve
   sparse/GPU backends, but it is a false data-dependency: `A * A` then moves
   every tile of `A` into the allocation tasks (and those tasks have no
   concrete `return_type`). At the small sizes the dense GEMM bench uses, that
   is a measured ~2×. Use `allocate_tiled` instead — dense stays
   `DArray(undef)` (GPU processors still override `AllocateUndef`), sparse
   stays sparse zeros. `similar(chunk)` is only right when you actually need
   the source value.
