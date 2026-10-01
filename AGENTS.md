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

25. **Vendor GPU sparse tiles do not speak SparseArrays' names, and CSC×CSC is
   not a given.** `CuSparseMatrixCSC` / `ROCSparseMatrixCSC` store values in
   `nzVal` (and `colPtr`/`rowVal`); a kernel that reaches `.nzval` works on
   host CSC and `DeviceSparseMatrixCSC` and then dies on CUDA. Use `hasfield`
   or `nonzeros`, not a hardcoded field. Separately, `A * B` of two rocSPARSE
   CSC tiles falls into LinearAlgebra's generic matmul (scalar indexing) unless
   that AMDGPU version wraps CSR SpGEMM — tile `matmatmul!` must gather to
   host CSC, as `DeviceSparseMatrixCSC` already does, rather than hoping `*`
   stays on-device. oneAPI's `zeMemOpenIpcHandle` out-param is
   `Ptr{PtrOrZePtr{Cvoid}}`, not `Ptr{Ptr{Cvoid}}`. And once a GPU processor
   type (and its `show`) lives in core so combo extensions can dispatch on it,
   the backend extension must not redefine `show` — precompilation forbids the
   overwrite.

26. **A VRAM-stamped host `Array` is not a device buffer.** Collect's MPI cat
   tree densifies every tile to `Array` (device `cat` is scalar indexing),
   and those gather tasks run on the GPU compute scope. `execute!` used to
   label the result with the *processor's* space, so a host `Matrix` sat in
   a `ROCVRAM`/`CUDAVRAM` chunk. The next hop then took same-node device IPC
   and died in `ipc_export(::Matrix)`. Stamp the result from
   `value_memory_space`, and do not select IPC unless the chunktype is a
   GPU array (`ipc_type_eligible`). Space-only `ipc_eligible` is not enough.

27. **Log emitters in tests must be count-bounded, and chunk lists must be
   memory-bounded.** A `while !stop[]` logger on every default thread will
   starve the task that flips `stop` (lesson 12) and allocate slabs until
   the machine OOMs — measured at 250GB+ virtual across leftover
   `Pkg.test` children after only the parent shell was killed. Use a
   fixed `for i in 1:N` (N on the order of a few chunks), cap published
   slabs (`MAX_CHUNKS`), and when killing a hung Julia test kill the
   whole process group (`kill -- -$PGID`), not just the `julia -e`
   wrapper. `Pkg.test` spawns a child that keeps running if you only
   SIGTERM the wrapper.

28. **`nworkers() == 1` means "this process", not "there are workers".**
   Without `addprocs`, `workers() == [1]`. `remotecall_wait` /
   `remotecall_fetch` to `myid()` deadlocks — the Distributed waiter
   never runs on the calling task. Gate broadcasts with
   `length(procs()) > 1` (then `workers()` are remote only; Dagger does
   not import `nprocs`). `_map_workers` already calls `f()`
   locally when `p == myid()`; do not reintroduce a self-remotecall
   around `enable_logging!` / `get_logs!`.

29. **Do not mutate TimespanLogging globals at another package's toplevel.**
   `@logcategory` used to call `register_category!` while Dagger was
   precompiling. Those writes land in TimespanLogging's arrays in the
   *precompile process* and are discarded when TimespanLogging loads from
   its own image; Dagger's baked `const` IDs then disagree with a fresh
   runtime registry (or collide with MemPool/tests that register later).
   Category IDs are assigned lazily on first `category_id` use. Keep it
   that way — a `const ID = register_category!(...)` at Dagger toplevel
   is not safe.

30. **Benchmark fixtures must be fully awaited.** DArray construction is
   asynchronous: if setup launches `A` and `B` but waits only for `A`, the
   timed body inherits an arbitrary fraction of `B`'s allocation and scheduling
   work. That contamination is placement- and timing-dependent, so a scheduler
   improvement can look like a multi-fold regression. Wait every fixture that
   the measured operation consumes.

31. **SPMD benchmarks need a rank-uniform sample loop.** BenchmarkTools applies
   its `seconds` cutoff independently on each process; small timing differences
   can make one MPI rank stop while a peer enters another collective sample,
   deadlocking the next benchmark. Run one sample per rank at a time, use a
   collective maximum to make the stop decision, and use a cooperatively-waited
   `MPI.Ibarrier` after per-sample GC/teardown before timing the next sample (a
   blocking `MPI.Barrier` has the same progress-engine deadlock risk as lesson
   12). Otherwise a fast rank also charges its next sample for waiting on a
   peer's preceding GC, creating large but fake timing regressions in short
   collective operations.

32. **An empty benchmark suite is a benchmark failure, not a successful no-op.**
   AirspeedVelocity adds its own `time_to_load` result, so an orchestrator that
   swallows every leaf error still emits a plausible-looking green report with
   one row. Require a nonempty manifest, and abort an SPMD run on a leaf error:
   after one rank leaves a failed operation, attempting the next collective is
   unsafe. Also treat BenchmarkTools' generated `samplefunc` as a versioned
   internal API: 1.8 changed it from a two-argument function returning a tuple
   to a four-argument function writing measurements through a `Ref`.

33. **Synchronous and asynchronous submission need different batch sizes.** A
   large asynchronous batch amortizes channel and scheduler overhead while its
   submitter overlaps with continued planning. The same batch size on a
   one-thread `BatchedEnqueueQueue` withholds every task in a short region until
   planning has nearly or completely finished, serializing planning against
   execution. This is especially costly for iterative solvers, which execute
   many small datadeps regions. Tune the two queue types independently.

34. **A benchmark script shared across revisions must probe backend capability.**
   Airspeed runs today's script against old Dagger code, and a generic suite can
   also contain a leaf unsupported by the selected backend. MPI SVD, for
   example, uses a Distributed-only processor grid and divides by zero before
   sampling. Probe optional or newly backend-enabled operations while
   constructing the suite, so unsupported leaves are absent rather than
   aborting the comparison. When an external MPI worker does abort, persist
   each failing rank's `CapturedException` before `MPI.Abort`: the first failure
   may be off rank 0 and can terminate rank 0 before it writes anything, mpiexec
   output may disappear from the parent runner's log, and a generic "worker
   exited" message discards the only actionable diagnosis.

35. **A whole-region copy owner must represent the whole copy batch.** A
   remainder can be assembled by several copy tasks from disjoint source
   spaces. Registering each task as the owner of the whole destination makes
   the last registration replace the earlier ones; a whole-region consumer
   then waits for one piece and a late copy can overwrite its result. Making
   the last task depend on every earlier copy repairs that owner invariant,
   but couples disjoint copies unnecessarily and puts the last one on the
   critical path. Represent the logical write as a batch producer instead
   (lesson 38). Registering every exact destination span is correct too, but
   is a performance cliff for halo exchange: a 64-tile stencil produced
   hundreds of megabytes of interval-tree/overlap bookkeeping and regressed
   by 7–8x.

36. **Distributed benchmark kernels must exist on every worker before
   sampling.** Defining an `@stencil` wrapper only on the driver serializes its
   generated closure to whichever worker happens to receive a tile. Globals
   used by the closure (`Clamp`, `Reflect`, etc.) can then be missing or too new
   for that worker's world age, and even successful leaves randomly pay remote
   compilation during a timed sample. Import macros/globals in one
   `@everywhere` statement, then define the macro-using wrappers in a second
   `@everywhere` statement (the import must be evaluated before remote macro
   expansion). A one-block capability probe is not a distributed warmup.

37. **BenchmarkTools sees only the driver process's allocations.** In a
   Distributed benchmark, arbitrary tile placement makes the reported bytes
   include however many payload tiles happened to execute on the driver. A
   1024² `Float64` allocation consequently varied from 2–8 MiB with no global
   allocation change, and task overhead varied with the driver's share too.
   Give Distributed benchmark fixtures a deterministic balanced proc grid (the
   array and stencil suites use `assignment=:cyclicrow` when
   `length(procs()) > 1`) so both revisions measure the same local fraction of
   the workload. Do not force that assignment under MPI: named cyclic grids are
   built from Distributed processors, and an MPI rank's `procs()` is only `[1]`,
   so the grid is empty and allocation divides by zero. Retain `:arbitrary`
   there so the MPI-aware scheduler places tiles. The reported number is still
   process-local; deterministic placement only makes the comparison meaningful.

38. **A logical copy batch must keep every physical producer in every
   dependency view.** When a `MultiRemainderAliasing` restores one whole-region
   replica from disjoint pieces, launch each copy with its own source readers
   and syncdeps, but do not let each copy rewrite the destination's whole
   `ainfos_owner`, `arg_history`, or `arg_current`. After all copies are
   launched, record one logical writer whose producer is the complete task
   batch. Expand that producer in whole-object read/write dependencies,
   historical remainder dependencies, and free-buffer syncdeps; widening only
   the live owner silently leaves history or teardown waiting for one copy.
   Keep the singleton-copy path direct so ordinary per-argument moves do not
   allocate a batch vector.

39. **Gate diagnostic work at the call site, not only inside the logger.**
   `hier_log!` checks `HIER_TIMING[]`, but Julia evaluates its arguments before
   that check. Unconditional `time_ns()` calls around per-argument aliasing and
   slot creation therefore survived with timing disabled. Gate both the start
   timestamp and the finish/event argument evaluation; a disabled diagnostic
   should not pay for the measurement it discards.

40. **Reusable scratch macros must return their declared container types.**
   `@reusable_vector` and `@reusable_dict` expand through non-const global
   `TaskLocalValue`s. Without a type assertion, their expressions infer as
   `Any` even though the runtime containers are typed: iteration, element
   stores, and closures capturing them lose that type throughout the hot loop.
   The aliasing-result scratch introduced one boxed pair allocation per
   argument this way. Assert the container type inside the macros, before
   `empty!`, rather than relying on each caller to do so. Escape the supplied
   type expressions to resolve caller-defined types correctly, and check the
   return types with `@inferred` as well as the containers' runtime types.

41. **Remote aliasing batches must seed the driver's region memo.** Phase 1
   groups arguments by owner and computes aliasing remotely, but Distributed
   RPCs do not inherit the caller's `CHUNK_AINFO_MEMO` scoped value. Keeping
   those answers only in `arg_to_ainfo` makes Phase 4 ask the owners again.
   Seed the memo when merging each remote batch, keyed by the original local
   `Chunk`/`ChunkView` and dependency modifier, not the deserialized argument
   wrapper. Keep the local/MPI path unchanged: it already fills that memo.
   A typed merge barrier also prevents `remotecall_fetch`'s `Any` result from
   erasing the result container's type in the per-argument loop.

42. **Fix the SPD fixture's destination placement, not just its input's.**
   Giving `G` a cyclic assignment does not give `G * G'` that assignment:
   `similar(G)` allocates an arbitrarily placed result. A Cholesky benchmark
   must explicitly allocate its SPD destination with the same assignment and
   use `mul!` in untimed setup, or its input layout still varies between
   samples/revisions. Keep named grids Distributed-only and leave MPI on its
   native allocator. This controls the fixture, not the measured operation:
   out-of-place multiplication and Cholesky still allocate/place their own
   outputs normally, and neither scheduling heuristics nor thresholds change.

43. **Broadcast delivery must wake only its own consumers.** One shared
   condition for all `(root, tag)` slots makes a delivery wake every unrelated
   waiter: draining 1024 tags one at a time took 395 ms, versus 3 ms with
   per-slot conditions sharing the registry lock. Create conditions only when
   a consumer actually blocks. Keep a slot alive until all its consumers have
   left, including consumers already notified but still reacquiring the lock;
   an empty FIFO alone does not mean its condition can be replaced. Heartbeats
   and shutdown still notify every slot, and an empty consumer must also check
   `running`: a consumer already woken by a heartbeat is outside the shutdown
   notification queues and otherwise goes back to sleep after the relay stops.

44. **An uncontended MPI receive needs a lease, not an event.** Uniformity
   checks made the old receive guard create hundreds of thousands of
   `Base.Event`s per sparse product, although its `(comm, source, tag)` stream
   had only one receiver. Register `nothing` for an owned stream and create
   a one-shot event only when a competitor arrives; delete the stream and
   notify that event when the owner leaves. Never reset/recycle the event:
   notified competitors can still be inside `wait`. Use explicit locked
   blocks, since the old captured `our_event` lowered to a `Core.Box`, and
   release the lease in `finally` on both serialized and in-place paths so a
   failed receive cannot strand every subsequent receiver of that stream.

45. **Task-pool capacity is not a reason to create every slot upfront.**
   Completion and placement tasks can be short-lived, but each owns a
   task-local fire cache with capacity 32. Creating all 32 tasks, channels,
   and monitors on its first dispatch pays for 31 idle slots when that caller
   dispatches only once. Initialize slots on demand without changing capacity
   or the overflow policy; 100 single-use caches fell from 75,100 allocations
   / 4,004,800 bytes to 6,800 / 296,000. Clear the dynamic scope at actual slot
   creation, run its setup before scheduling, and register each dispatch
   before publishing the payload. Finalization must skip unassigned slots.

46. **Repeated calls do not necessarily warm the option-default cache.**
   `BasicLFUCache` can immediately evict a newly inserted frequency-1 key
   when every resident key is more frequent, so a new signature keeps taking
   the default fallback after arbitrarily many warmups. Test that path with
   a full cache in a fresh task, not by changing capacity or eviction policy.
   Splat `Signature.sig` directly for non-keyword calls: splatting its
   identical `sig_nokw` view boxes the view and an iteration pair per type.
   The generic fallback also needs no specialization of its unused arguments;
   leave type-dependent user overrides untouched. A four-type cold default
   population fell from 217 allocations / 9,088 bytes to 49 / 1,792, while
   cache hits stayed at 1 / 256. Keep keyword views instead of adding a copy
   to every signature, especially signatures whose defaults are already cached.

47. **Even a typed `findmin(::Dict)` can box its result in an eviction loop.**
   The defaults LFU allocated one `(frequency, key)` tuple per eviction
   despite its concrete key/frequency types. A direct minimum-frequency scan
   removes that allocation without changing capacity, admission or eviction
   decisions. Initialize from the first dictionary entry and update only on
   strict `<` so equal-frequency ties preserve the original iteration order.
   Compare exact cache contents and frequencies against the old algorithm
   after mixed hits/misses, including zero capacity. Ten thousand repeated
   evictions fell from 10,000 allocations / 320,000 bytes to zero; cache-hit
   behavior is unchanged. Together with lesson 46, cold default population
   is 25 allocations / 1,024 bytes rather than 217 / 9,088.

48. **Hierarchical Datadeps has two dispatch branches, and `-p 0` only
   exercises one.** `distribute_tasks_hierarchical!` takes the shared-state
   (sequential) branch whenever `all_procs` spans more than one memory space,
   and the parallel per-partition branch otherwise. With no workers there is
   one memory space, so a suite run at `-p 0` never enters the shared-state
   path at all. A change to the hierarchical scheduler that passes cleanly at
   `-p 0` can be completely broken with a worker present — AOT planning was
   silently disabled on that branch, degrading every AOT scheduler to JIT
   round-robin, and the scheduling suite went from 516 pass / 0 error to 38
   errors the moment `-p 1` was used. Test hierarchical changes with `-p >= 1`.

49. **A snapshot-per-operation metrics read is a deep copy per operation.**
   `MetricsTracker.snapshot` rebuilds whenever the cache's generation has
   moved, and the rebuild copies every context and every per-metric storage
   (measured: 273 allocations / 67 KB against 488 stored values). The
   generation advances on *every* recorded value, so with tasks completing
   continuously a per-task consumer rebuilds every time. A cost model does not
   need an exact view — it reduces many samples to an estimate — so it should
   use `snapshot_stale` and bound rebuilds by time rather than by the rate at
   which values arrive.

50. **Memoize metrics lookup *misses*, not just hits.** `MT.find_keys` scans
   every storage and every key of a snapshot, building `Set{Any}`s, and it
   costs the same whether or not it matches anything. `metrics_lookup_alloc`
   and `metrics_lookup_transfer_rate` run once per candidate processor per
   task; caching only their hits leaves the common no-samples case paying full
   price forever, which is exactly the situation for transfer rate (its inputs
   have been dead since "Sch: Drop dead per-task transfer-stat Atomics"). Key
   such memos on the snapshot's `objectid` so a rebuild invalidates them
   wholesale, and keep them task-local so the concurrent per-partition
   scheduling tasks neither share nor lock them.

51. **A micro-benchmark of a metrics lookup lies if the cache is empty.**
   `metrics_lookup_transfer_rate` measures 0 allocations when no transfer-rate
   values exist, because it returns before using the scan's result — but the
   scan already ran. Benchmark these against a *populated* cache, or profile
   the real path with `--track-allocation`; several rounds of plausible
   hypotheses here were all wrong, and the profiler answered it immediately.

52. **`Meta.parseall` does not throw on a syntax error.** It returns a
   `:toplevel` expression containing an `Expr(:error, ...)` node, so
   `Meta.parseall(read(f, String))` completing without an exception says
   nothing about whether the file is valid. A `test/scheduler.jl` with an
   unbalanced `end` passed that check repeatedly while the suite was silently
   running 132 of its 305 tests — the stray `end` closed an enclosing
   `@testset` early, so the rest of the file was reparented and the parse error
   only surfaced when the runner actually included it. To check a file, count
   the error nodes:

       ex = Meta.parseall(read(path, String))
       any(a -> a isa Expr && a.head === :error, ex.args)

   The general trap: a verification step that cannot observe the failure it is
   meant to catch will report success forever. The same shape produced two
   other false "verified"s in the same session — a suite run at `-p 0` that
   never enters the branch under test (lesson 48), and a lookup benchmarked
   against an empty cache that measures the early return rather than the scan
   behind it (lesson 51). When a check passes, ask what it would have done had
   the thing been broken.

53. **Wrapping a function that other code recognizes by identity silently
   drops the special case.** Datadeps copy tasks used to be spawned as
   `move!`; the metrics work wrapped them in `move_toplevel!` (then named
   `instrumented_move!`). Two backends decide how to run a task with
   `f === move!`: MPIExt makes *every* rank run a
   copy (the source rank sends, the destination receives), and OpenCLExt skips
   its device lock so a copy's long cross-rank receive cannot deadlock against
   the task producing the data. With the wrapper, MPI copies ran on the
   destination rank alone and blocked forever in a receive nobody answered --
   every region that moved data across ranks hung, RoundRobin included, while
   the whole Distributed suite stayed green. Before wrapping or renaming a task
   function, grep for `=== thatfunction` (and `typeof(thatfunction)`) across
   `src/` *and* `ext/`, and route every hit through one predicate
   (`is_move_task`) instead of adding a second identity check next to it.

54. **Under uniform execution, a planner that reads rank-local inputs must plan
   on one rank.** Every MPI rank plans every region and must place every task
   identically, but metrics caches are rank-local (a rank records only the
   tasks it ran) and a search with a wall-clock budget stops at a different
   iteration on each rank. So a cost-model scheduler that is perfectly
   deterministic given its inputs still diverges the moment any task has run:
   `check_uniform(proc)` fails in `distribute_task!` (without the checker, tags
   desynchronize and the job hangs). Fixed RNG seeds do not help. The cure is
   structural -- plan on rank 0 and adopt its plan everywhere
   (`aot_plans_locally` / `uniform_aot_schedule!`), exchanging exactly once per
   planned region whether the local cache hit or missed, so that a cache that
   ever disagrees across ranks still cannot desync the exchange. Run
   `check_uniformity!(true)` on any new scheduler under MPI before timing it.

55. **A task-local singleton is shared by everything its task does -- including
   work the design deliberately split into per-partition shards.** Hierarchical
   Datadeps gives each partition its own scheduler via `similar` because a
   round-robin index advanced over one partition's processor list can overrun
   another's. The AOT fallback (`AOT_JIT_FALLBACK`, a task-local
   `RoundRobinScheduler`) walked around that: the sequential path schedules
   every partition from one task, so one index walked lists of different
   lengths, and Cholesky died with a `BoundsError` under any AOT scheduler as
   soon as a worker had fewer threads than the driver. The scheduler sweep
   never saw it, because every process there had the same thread count: a run
   in which every process is identical cannot find a bug that needs them to
   differ. When touching per-partition state, also run a mismatched
   configuration (driver `-t 4`, workers `-t 2`).

56. **Task-local is the lifetime of a task, not of a program.** The AOT schedule
   cache was a `TaskLocalValue`, so "plan once per DAG shape" held only within
   one task: every region submitted from `Threads.@spawn`, `@async` or a Dagger
   task planned again from scratch, while a benchmark calling from its main
   task only ever measured the cached case. Pick a cache's lifetime from what
   its key depends on -- a plan depends on DAG structure, so the cache is
   process-wide, behind a lock. Check a caching claim from a *second* task, not
   by repeating calls in the first.

57. **A cost model's catch-all method is where the arguments it cannot see go
   to be priced at zero.** The EFT planners charged for moving an input only
   when it was a `Chunk`; `DTask`s from outside the region and everything else
   fell to methods returning 0. But a DArray's `chunks` are the finished
   `DTask`s that produced each tile, so *every* tile reached a region that way:
   the model saw all data as free on every processor, and plans for a real
   Cholesky put tile writers on their tile's owner at exactly chance rates.
   The unit tests passed throughout because they built their regions from
   plain arrays and `Chunk`s. Before trusting a cost over argument values,
   log what kinds of values a real workload actually delivers (a DArray
   region, not a hand-built one) and check the catch-all is not where they go.

58. **A plan is only as good as the measurements it was made with, and the
   first call has none.** Three things conspired to make every AOT plan a
   blind one. The metrics cache kept the last 100 tasks, fewer than one
   region's kernels plus its copies, so by planning time most runtime samples
   were gone. A region's kernels have never run when it is first planned, so
   every task cost the 1 s placeholder, which dwarfs any transfer and reduces
   the plan to balancing task counts. And that plan was cached for the life of
   the process. The benchmark sweeps timed exactly those plans. A plan made
   from placeholders is now provisional and replanned once
   (`datadeps_plan_informed`), and a wrapper scheduler must forward that hook
   just as it forwards `datadeps_uses_aot`, or its plans silently stay blind.

59. **Iterating a container held behind an abstract type dispatches on every
   element.** A metrics context stores its storages as
   `AbstractMetricStorage`, and `find_keys` & co. looped over a storage's
   `data` inline: ~1 us per stored value, so one runtime lookup took 1.9 ms
   against a 100-entry cache and 14 ms at 5000. Passing the storage to a
   small function (a function barrier) runs the loop specialized: 31 us and
   1.7 ms. This is lesson 5 in a form easy to miss, because the container's
   own type parameters are concrete; only the field holding it is not. The
   same trap made trimming the cache O(bound) per recorded task until it was
   batched (322 us per task at a 5000 bound, 11 us after).

60. **Under uniform execution, a rank's metrics must describe only the work it
   did -- and rank 0 plans for everyone from its own.** Three independent
   paths broke this, and together they had the EFT planners piling a region
   onto whichever rank looked cheapest. Every rank runs every task's
   `do_task`, so each recorded its own few-microsecond spectating as the
   owner's runtime (`records_metrics` now drops those). An `MPIProcessor`'s
   runner is not pinned (lesson 12 forbids it: MPI waits spin on `yield`), so
   `ThreadProc.execute!` runs the kernel on a pinned sub-task and the runner's
   thread clock measured something else entirely -- 4x too low, or wrapped to
   1.8e19 ns when the runner migrated (`KernelTimeMetric` clocks the kernel
   where it runs). And only a copy's source rank knows its size, so the
   planning rank had unusable exact samples for every copy *into* it, and a
   fallback taken only when no samples *exist* priced those at the 1 MB/s
   default against 1.5 GB/s elsewhere. None of this was visible in plan
   quality metrics or tests; dumping the `EFTCostCache` the planner actually
   builds (per-processor runtimes, the space-to-space rate matrix, where each
   datum starts) showed all three in one screen. Do that before tuning a
   heuristic, and make any fallback chain fall through on unusable values,
   not only on missing keys.

61. **When a smart policy loses to a dumb one, check whether the dumb one is
   dodging a cost the model does not know about.** With correct inputs, the
   Greedy planner still lost to RoundRobin on matmul. Its plan predicted 382
   copies; execution ran ~720. For RoundRobin's placement the same model
   predicted 342 and execution ran 357. The gap was Datadeps, not the
   planner: copy-ins were decided from a write history that records every
   copy as a write, so a read-only tile read on workers 3, 4, 3, 4 bounced
   between them, while `arg_current` knew both replicas were current.
   On four processes RoundRobin's matmul read each tile on at most one other
   process, so it never paid there -- but on 8 MPI ranks it paid too (2,477 MB
   moved per matmul before the fix, 517 after), so the bug had been taxing
   the default path all along. Count what the runtime actually does (copies here, via a trace
   in `enqueue_*copy*!` or `:datadeps_copy` log events) and compare it with
   what the model predicts, per policy; a model that is exact for one policy
   and wrong for another points at the runtime.

62. **Check a textbook scheduling model's assumptions against how the runtime
   executes, one at a time.** The EFT planners used HEFT's model, and two of
   its assumptions are false for Datadeps. HEFT prices a task's inputs as the
   *latest* arrival, because transfers overlap on dedicated links; Datadeps
   runs each copy as a task on the receiving processor, one after another,
   so eight inbound tiles cost that processor eight transfers, not one.
   HEFT assumes every task exists at time zero; flat Datadeps launches a
   region's tasks serially from one task (aliasing, remote buffer
   allocation and copy spawning per task), which took 75-90% of a region's
   wall time -- 1.3-9 ms per task on Distributed, ~7 ms under MPI, where
   every rank steps through every launch. Both errors pushed the plans the
   same way: moving work off its data looked cheap, and the data's owner
   looked busier than it would be when each task actually arrived. A
   blocked stencil, already perfectly balanced owner-computes, got 21 of 64
   tasks on their owner, and on 4 MPI nodes moved 3.5 GB per call where
   hierarchical mode moved 24 MB. Charging copies to the receiver
   (`_eft_ready_and_runtime`) and releasing tasks at the measured launch
   rate (`DATADEPS_RELEASE_NS`) put all 64 on their owner and cut that
   traffic to 38 MB. On 4 nodes, against RoundRobin in the same sweep, the
   stencil went to 0.48-0.71x under MPI, Cholesky to about 0.5x, and matmul
   under Distributed from 1.5-1.7x slower to 0.48-0.77x. Neither error shows
   in a model-level test; both showed in a
   timeline of `:add_thunk` launch events against `:compute` events, and in
   counting where each planned task ran relative to its data.

63. **A bounded sample cache is the wrong place to keep an estimate, because
   the estimate vanishes exactly when the workload is large.** The scheduler
   priced every task from the per-task metrics cache, which keeps the most
   recent 1000 tasks. A Cholesky region at 256² tiles is ~2000 tasks, so by
   the next `copy(A)` every sample of its signature was gone and each of its
   tasks cost the 1 s placeholder for an unknown signature. With pressure
   then counted in whole seconds per reserved task, the 0.5 s transfer
   penalty that keeps a task next to its data lost as soon as its owner had
   one more task queued than another worker: a quarter of the copied tiles
   went to other workers, and on four nodes the default path ran Cholesky
   2x slower than master while moving 2.8x the data. Master never had the
   problem only because its per-signature table was unbounded. The first
   call of a workload is always fine (the cache is empty of *everything*, so
   the placeholder applies evenly), which is why the warmup looked like
   master and the samples did not -- when a workload is fast once and slow
   afterwards, look for state that a previous call leaves behind. Keep the
   samples bounded and the estimates separate (`CostSummary`: one blended
   number per key, O(1) to update and read); a test that floods the cache
   past its bound and then asks for the estimate is what guards it.

64. **Do not ship what the receiver already knows, and measure per-task
   overhead against the same workload with the feature switched off.** Each
   task result carried its signature (a vector of types) and processor so
   the driver could tag its metrics -- but `handle_result!` holds the
   `Thunk`, whose signature it memoized when scheduling it, and the
   processor it fired the task on. Serializing those, plus a vector of
   boxed metric pairs applied with a dynamic dispatch each, cost more than
   the rest of the scheduler path: with three workers an eager task took
   0.071 ms against master's 0.029 and a Datadeps task 0.280 against 0.175.
   The quickest attribution was not a profiler but a switch: with
   `records_metrics` returning `false` on the workers the same benchmark
   gave 0.035 and 0.183, so shipping and applying was ~90% of the gap. A
   fixed record of measurements (`TaskMetrics`), tagged on the driver,
   brought it to 0.038 and 0.207 (on a quiet VM, within 7% of master on the
   Datadeps path). Profile in one process only after that kind of A/B has
   said where to look, and run timing comparisons on a quiet machine: on a
   shared one the same build varied 2x between back-to-back runs.

65. **A first-use compilation inside a lock is a stall for everyone waiting
   on that lock -- and under uniform execution, once per rank.** The metrics
   cache trims itself the first time a process has recorded more tasks than
   its bound, and that first trim compiled one specialization of the
   eviction helpers per metric storage: 500 ms in a fresh process, 0.6 ms
   ever after. It ran inside `handle_result!`, which holds the scheduler
   lock, so no task was scheduled for half a second; and under MPI each rank
   fills its cache at a different task (it records only what it ran), so a
   job stalled once per rank, spread across the first few calls big enough
   to fill the cache. The symptom was a call sequence that master never
   showed -- 1.7, 2.4, 2.4, 1.6, 0.9 s, the same on every sweep -- with the
   first call clean (the cache was still filling) and the same bytes moved.
   Two things found it: the same pattern in every earlier sweep's CSV since
   the metrics work landed, and a rank-0 profile of the slow call whose own
   time sat in `trim_context!` while the rest waited on other ranks. Then a
   200-task micro-benchmark across the bound, run twice in one process,
   separated compilation from work. Put such paths in the precompile
   workload (`src/precompile.jl` now crosses a bound of 8 with every storage
   present), and when a workload is slow only on calls two to four, suspect
   something that runs once per process at a size threshold.

66. **A signature's first sample on a process is its compile time, and
   "first" is per process, not per key.** The cost summary replaced a key's
   first sample with its second, which handles the thread that compiled a
   task -- if that thread ever runs the task again. A thread that ran it once
   kept the outlier for good, and once the region was planned over every
   processor the MILP planner, told 12 ms for a 5 us `add!` on such a thread,
   stacked two independent tasks on the other one. And another process's
   first sample (its own compilation) blended into the cross-process tiers
   that already held real numbers. Track the first sample per (signature,
   worker): use it only while nothing has measured the type anywhere, and
   drop it from the first processor's entry when the process's second sample
   arrives on another. Whenever an estimate is "per X", ask what event
   (compilation, warm-up, a cold cache) happens once per *process* rather
   than once per X, and make sure a single occurrence cannot pin an X.

67. **In hierarchical Datadeps, placement is decided by the partitioner;
   a plan made afterwards can only pick threads.** The planners beat flat
   RoundRobin by up to 3.7x and never beat the default path, because the
   default (hierarchical) mode assigned each task's *process* by data
   affinity in `partition_dag`, planned the region after that, and dropped
   every planned processor outside the task's partition -- the plan chose a
   thread within an owner it had no say in. The sweeps never showed this
   because the planners were only ever timed in flat mode, where they do
   choose processes and pay a serial launch loop for it. Plan the whole
   region before partitioning and let the plan name each task's owner. When
   a policy "has no effect" in one mode, find the line that discards its
   decision before tuning the policy.

68. **A model's default for an unmeasured quantity must be the right order
   of magnitude, or one missing sample decides the plan.** The planners
   priced a copy between two spaces that no copy had been timed between at
   1 MB/s. In flat mode every pair had been timed, because RoundRobin's
   warm-ups moved whole tiles. In hierarchical mode a stencil's warm-ups
   move only halos, a few kilobytes each and below the size floor a rate can
   be taken from, so no pair was ever timed, and the plan charged a task on
   its own owner 0.5 s per cross-rank neighbor tile: every placement looked
   equally hopeless and the ranking was noise (49 of 64 on their owner, 540
   MB moved per call). At 1 GB/s, the order of any interconnect, the same
   plan puts 64 of 64 on their owner. Two related traps found on the way: a
   remainder copy sized by the tile it was cut from reports a rate hundreds
   of times too high, and a copy is itself a task the launcher must spawn,
   so a plan that adds copies must release everything after them later.
   When a policy misbehaves only in one mode, dump the cost table it
   actually built in that mode (`_build_eft_cost_cache`) -- the "move rates
   0.00" line was the whole story.

69. **A cost term that is right in principle can erase a real win if its
   constant is wrong; the two plans bracket the number to measure.** With
   the planners deciding placement in the default mode on four Distributed
   nodes, Greedy ran Cholesky at 256² tiles 40% faster than master's
   default, moving 16 MB more than RoundRobin: it moved a few update tasks
   off the diagonal's owners onto idle processes. The same model moved LU's
   and matmul's tiles wholesale and lost 1.6-4.6x. The missing term was
   the copy back of a written tile at region end, which Datadeps always
   pays; charged as one more transfer and one more launch, LU and matmul
   went to parity -- and so did Cholesky, because the charge is too coarse
   for the few cheap moves that won. Under MPI the same term took Cholesky
   and LU at 512² from 1.6-1.8x to 1.0x. So the term belongs, and its size
   does not: the model needs a measured per-copy cost (fixed overhead,
   which every rank pays under uniform execution, plus a rate from copies of
   the right size), not a guess. When adding a term flips cells both ways,
   stop tuning it and instrument what it stands for.

70. **A Distributed GPU transfer must preserve the source's ownership.**
   Sending a raw device array in a slot-creation RPC deserializes it on the
   receiver while `from_proc` still names the sender. The subsequent GPU
   `move` then activates a remote worker's context locally and asserts; some
   backends cannot serialize the array in the first place. Send an owner-side
   `Chunk` and use the backend's Chunk transport instead. The same rule
   applies when fetching a remote source for in-place copies. Pin regression
   inputs and consumers to different workers selecting device 1: a GPU scope
   alone does not guarantee that a transfer actually happens.

71. **CUDA IPC must export staging memory and return an owned copy.**
   CUDA's pooled allocations cannot be exported with `cuIpcGetMemHandle`, and
   returning an imported mapping makes the receiver alias the sender's data
   and depend on its lifetime. Use the shared `ipc_export`/`ipc_materialize`
   hooks, keep the staging token on the sender until the receiver finishes,
   and release it in `finally`. Use `CUDADRV` for closing handles too: CUDA 6
   moved the driver API into CUDACore. Test repeated transfers in both
   directions and mutate the destination to verify source independence.

72. **Two GPUs of one process are not one device, and may not even be
   peers.** Several paths assumed "same worker" meant "one kernel may touch
   both buffers": the same-worker device remainder copy, `collect`'s
   in-process `cat`, and CUDA `pointer()` (which takes ownership for the
   *active* device and throws without P2P). `CUDA.pin` is also only deduped
   per context while host registration is process-wide. Key direct device
   paths on equal memory spaces, not equal workers, read addresses without
   ownership side effects, and test with tiles on two devices of one
   process: the one-GPU-per-worker suites cannot see any of this.

73. **Reading a GPU task's result needs Dagger's stream, not the caller's.**
   GPU `execute!` returns without synchronizing: the kernels are still queued
   on Dagger's per-device stream. A plain `fetch` + `Array(x)` from the
   caller copies on the caller's own task-local stream, and is correct only
   if the array library synchronizes the previous owner when another stream
   touches a buffer. CUDA, AMDGPU and OpenCL do; oneAPI does not, and it also
   copies on the *calling task's* device. A green CUDA run therefore does not
   prove the wait exists. Before reading a tile on the host, call
   `gpu_synchronize(chunk.processor)` and copy under `with_context`, on the
   chunk's owner (as `collect` does via `_collect_host_tile`). Note that
   IntelExt's hooks sync only the calling task's stream, and each oneAPI
   `execute!` runs on a fresh task, so oneAPI's own hook is not sufficient yet.

74. **Never unregister host memory from a finalizer that can block.**
   `CUDA.pin` unregisters in a GC finalizer that takes a `ReentrantLock`. When
   that lock is contended the finalizer throws "task switch not allowed from
   inside gc finalizer", the unregistration is lost, and GC frees the memory
   anyway: the range stays registered with the driver, and a later
   allocation at that address fails to register or is DMA'd through stale
   pages. Multi-GPU GEMM segfaulted a few iterations in, with a backtrace
   that blew the stack while printing (`jl_static_show` recursion) and showed
   nothing of the cause; the "error in running finalizer" lines before it
   were the only clue. `pin_buffer!` now registers itself and its finalizer
   only `trylock`s, re-arming itself on contention (which keeps the buffer
   alive until a later GC can unregister it). Anything that must run before
   memory is freed has to be finalizer-safe: no blocking locks, no yields.

75. **Two GPUs without peer access: never let CUDA.jl stage the copy.**
   Without P2P, `copyto!` between devices goes through a fresh *pageable*
   `Vector` with a synchronous DtoH: 1.4 GB/s on PCIe-attached L40S, where
   `cuMemcpyPeerAsync` (which works without peer access; the driver pipelines
   it through its own pinned buffers) reached 22 GB/s. A 4-GPU GEMM spent
   27 s per call in copies. Check `can_access_peer` before assuming a copy
   path is cheap, and measure the copy primitive in isolation first -- it
   took one 30-line script to find a 15x.

76. **One stream per device serializes every transfer with that device's
   compute.** Copies enqueued on the device stream wait behind its kernels,
   and an event recorded on the *source's* stream waits for kernels that
   merely read the data being sent (a GEMM's panel owner is busy reading the
   panel it is asked to send). Peer copies now run on a per-device copy stream
   ordered only against the allocations they touch (`BUFFER_EVENTS`, fed by
   Datadeps' `writes` option), and are host-synced so every existing
   synchronization point stays valid; 4-GPU GEMM went from ~3.9 s to ~3.1 s.
   Two traps found on the way: Datadeps spawns copy tasks `meta`, so
   `execute!` sees `Chunk`s, not arrays (unwrap before classifying, or every
   copy looks like an opaque argument and falls back to whole-stream waits);
   and `unsafe_free!` tasks hand you a freed array (check `data.freed`).

77. **A policy that "works" may be working by coincidence of task order.**
   RoundRobin placed a row-major GEMM's tasks exactly on their C tiles'
   owners, because the inner loop ran over columns in owner order. Visiting
   tiles diagonally (so each GPU starts with its local panel and copies
   spread out) made the same RoundRobin move A, B *and* C for every task,
   with 48 copies per call instead of 12. Count copies per call when changing
   either the traversal or the scheduler; `GreedyScheduler` keeps
   owner-computes regardless of order. The same coincidence hides in every
   region whose tasks follow chunk order: in-place DArray broadcasts
   (`x .+= a .* p`) matched their owners on 2 GPUs (3.8 ms) and moved every
   chunk off and back on 4 (170 ms), which made a CG solve 30x slower at 4
   GPUs than at 2. Test placement-sensitive code at more than two processors.
