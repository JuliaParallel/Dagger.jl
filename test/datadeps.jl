import Dagger: ChunkView, Chunk, AbstractAliasing, MemorySpace, ArgumentWrapper
import Dagger: aliasing, memory_space
using LinearAlgebra, Graphs

@testset "Memory Aliasing" begin
    A = rand(4)
    a = Dagger.aliasing(A)
    @test a isa Dagger.ContiguousAliasing
    @test a.span.ptr.addr == UInt(pointer(A))
    @test a.span.len == sizeof(Float64) * length(A)

    r = Ref(3)
    a = Dagger.aliasing(r)
    @test a isa Dagger.CombinedAliasing
    @test length(a.sub_ainfos) == 1
    s = only(a.sub_ainfos)
    @test s isa Dagger.ObjectAliasing
    @test s.ptr.addr == UInt(pointer_from_objref(r))
    @test s.sz == sizeof(3)
end

@testset "ChunkView" begin
    DA = rand(Blocks(8, 8), 64, 64)
    task1 = DA.chunks[1,1]::DTask
    chunk1 = fetch(task1; raw=true)::Chunk
    v1 = view(chunk1, :, :)
    task2 = DA.chunks[1,2]::DTask
    chunk2 = fetch(task2; raw=true)::Chunk
    v2 = view(chunk2, :, :)

    for obj in (chunk1, task1)
        @testset "Valid Slices" begin
            @test view(obj, :, :)     isa ChunkView && view(obj, 1:8, 1:8)   isa ChunkView
            @test view(obj, 1:2:7, :) isa ChunkView && view(obj, :, 2:2:8)   isa ChunkView
            @test view(obj, 1, :)     isa ChunkView && view(obj, :, 1)       isa ChunkView
            @test view(obj, 3:3, 5:5) isa ChunkView && view(obj, 5:7, 1:2:4) isa ChunkView
            @test view(obj, 8, 8)     isa ChunkView
            @test view(obj, 1:0, :)   isa ChunkView
        end

        @testset "Dimension Mismatch" begin
            @test_throws DimensionMismatch view(obj, :)
            @test_throws DimensionMismatch view(obj, :, :, :)
        end

        @testset "Int Slice Out of Bounds" begin
            @test_throws ArgumentError view(obj, 0, :)
            @test_throws ArgumentError view(obj, :, 9)
            @test_throws ArgumentError view(obj, 9, 1)
        end

        @testset "Range Slice Out of Bounds" begin
            @test_throws ArgumentError view(obj, 0:5, :)
            @test_throws ArgumentError view(obj, 1:8, 5:10)
            @test_throws ArgumentError view(obj, 2:2:10, :)
        end

        @testset "Invalid Slice Types" begin
            @test_throws DimensionMismatch view(obj, (1:2, :))
            @test_throws ArgumentError view(obj, :, [1, 2])
        end
    end

    @testset "View of View" begin
        outer = view(chunk1, 2:5, 3:6)
        nested = view(outer, 1:2, 2:3)
        direct = view(chunk1, 2:3, 4:5)
        @test nested isa ChunkView
        @test nested.chunk === chunk1
        @test nested.slices == direct.slices
        @test fetch(nested) == fetch(direct)

        # Colon parent dims pass sub-slices through
        outer_colon = view(chunk1, :, 2:5)
        nested_colon = view(outer_colon, 3:4, 1:2)
        @test nested_colon.slices == view(chunk1, 3:4, 2:3).slices

        # Int parent dim is dropped; nested view indexes remaining dims only
        outer_drop = view(chunk1, 3, 1:8)
        nested_drop = view(outer_drop, 2:5)
        @test nested_drop.slices == view(chunk1, 3, 2:5).slices
        @test fetch(nested_drop) == fetch(view(chunk1, 3, 2:5))

        @test_throws DimensionMismatch view(outer, :)
        @test_throws DimensionMismatch view(outer, :, :, :)
        @test_throws ArgumentError view(outer, 1:5, 1:2)  # out of outer range → composed OOB
        @test_throws ArgumentError view(outer_drop, 0)
        @test_throws ArgumentError view(outer_drop, 9)
    end

    @test fetch(v1) == fetch(chunk1)

    @test Dagger.memory_space(v1) == Dagger.memory_space(chunk1)
    @test Dagger.aliasing(v1) isa Dagger.StridedAliasing
    ptr = remotecall_fetch(chunk1.handle.owner, chunk1) do chunk
        UInt(pointer(Dagger.unwrap(chunk)))
    end
    @test Dagger.aliasing(v1).base_ptr.addr == ptr

    @testset "Aliasing" begin
        f! = v1 -> begin
            v1 .= 0
            return
        end
        Dagger.spawn_datadeps() do
            Dagger.@spawn f!(InOut(v1))
        end
        @test collect(DA)[1:8, 1:8] == zeros(8, 8)
    end
end

@testset "DArray" begin
    A = rand(Blocks(2), 4)
    @test_throws ConcurrencyViolationError Dagger.spawn_datadeps() do
        Dagger.@spawn sum(A)
    end
end

function test_move_rewrap_aliasing(obj, dest_space)
    accel = Dagger.current_acceleration()
    src_space = Dagger.memory_space(obj)
    from_proc = first(Dagger.processors(src_space))
    to_proc = first(Dagger.processors(dest_space))

    # move_rewrap like generate_slot!
    dummy_backing = Dagger.tochunk(Dagger.AliasedObjectCacheStore(accel))
    cache = Dagger.AliasedObjectCache(accel, dest_space, dummy_backing)

    dest_obj_chunk = Dagger.remotecall_endpoint_toplevel(Dagger.move_rewrap, accel, cache, from_proc, to_proc, src_space, dest_space, obj)

    # VERIFICATION: Check that source and destination have compatible memory spans
    # Use the chunk directly for aliasing so it handles remote workers correctly
    src_ainfo = Dagger.aliasing(obj, identity)
    dst_ainfo = Dagger.aliasing(dest_obj_chunk, identity)
    src_spans = Dagger.memory_spans(src_ainfo)
    dst_spans = Dagger.memory_spans(dst_ainfo)

    # Verify that the source and destination memory spans are the same length and do not overlap
    @test length(src_spans) == length(dst_spans)
    for (ss, ds) in zip(src_spans, dst_spans)
        @test Dagger.span_len(ss) == Dagger.span_len(ds)
        @test !Dagger.spans_overlap(ss, ds)
    end

    # Verify that no span is contained in another within the same space
    for (i, ss_i) in enumerate(src_spans)
        for (j, ss_j) in enumerate(src_spans)
            if i != j
                @test !Dagger.spans_overlap(ss_i, ss_j)
            end
        end
    end
    for (i, ds_i) in enumerate(dst_spans)
        for (j, ds_j) in enumerate(dst_spans)
            if i != j
                @test !Dagger.spans_overlap(ds_i, ds_j)
            end
        end
    end

    # Constructs an IntervalTree{ManyMemorySpan} using the source obj memory spans
    # Subtracts all of the memory spans of dest_obj from the IntervalTree
    # Verifies that the IntervalTree is now empty

    # N.B. We need ManyMemorySpan to track both spaces simultaneously
    # to catch misalignment between them.
    N = 2
    tree = Dagger.IntervalTree{Dagger.ManyMemorySpan{N}}()
    @test isempty(tree)
    for (ss, ds) in zip(src_spans, dst_spans)
        # Test that insert is fully reversible
        insert!(tree, Dagger.ManyMemorySpan{N}((Dagger.LocalMemorySpan(ss), Dagger.LocalMemorySpan(ds))))
        Dagger.subtract_spans!(tree, [Dagger.ManyMemorySpan{N}((Dagger.LocalMemorySpan(ss), Dagger.LocalMemorySpan(ds)))])
        @test isempty(tree)
    end
    for (ss, ds) in zip(src_spans, dst_spans)
        # Insert the same spans again
        insert!(tree, Dagger.ManyMemorySpan{N}((Dagger.LocalMemorySpan(ss), Dagger.LocalMemorySpan(ds))))
    end

    # Now subtract using the same pairs from dest_obj
    Dagger.subtract_spans!(tree, [Dagger.ManyMemorySpan{N}((Dagger.LocalMemorySpan(ss), Dagger.LocalMemorySpan(ds))) for (ss, ds) in zip(src_spans, dst_spans)])

    @test isempty(tree)
end
@testset "Aliased Object Copying" begin
    nw = nprocs()
    spaces = [Dagger.CPURAMMemorySpace(w) for w in 1:nw]
    test_pairs = filter(((ws, wd),) -> ws <= nw && wd <= nw,
                        [(1, 2), (2, 1), (2, 3)])

    for (w_src, w_dst) in test_pairs
        @testset "Worker $w_src -> $w_dst" begin
            # Array
            @testset "Array" begin
                obj = remotecall_fetch(w_src) do
                    Dagger.tochunk(zeros(Int, 4, 4))
                end
                test_move_rewrap_aliasing(obj, spaces[w_dst])
            end

            # SubArray
            @testset "SubArray" begin
                obj = remotecall_fetch(w_src) do
                    A = zeros(Int, 4, 4)
                    Dagger.tochunk(view(A, 1:2, 1:2))
                end
                test_move_rewrap_aliasing(obj, spaces[w_dst])
            end

            # ChunkView
            @testset "ChunkView" begin
                obj = remotecall_fetch(w_src) do
                    A = zeros(Int, 4, 4)
                    A_chunk = Dagger.tochunk(A)
                    view(A_chunk, 1:2, 1:2)
                end
                test_move_rewrap_aliasing(obj, spaces[w_dst])
            end

            # HaloArray
            @testset "HaloArray" begin
                obj = remotecall_fetch(w_src) do
                    H = Dagger.HaloArray{Int, 2}((4, 4), (1, 1))
                    Dagger.tochunk(H)
                end
                test_move_rewrap_aliasing(obj, spaces[w_dst])
            end
        end
    end
end

function with_logs(f)
    Dagger.enable_logging!(;taskdeps=true, taskargs=true, timeline=true, taskfuncnames=true)
    try
        f()
        return Dagger.fetch_logs!()
    finally
        Dagger.disable_logging!()
    end
end
task_id(t::Dagger.DTask) = Int(t.uid)
function taskdeps_for_task(logs::Dict{Int,<:Dict}, tid::Int)
    for w in keys(logs)
        _logs = logs[w]
        for idx in 1:length(_logs[:core])
            core_log = _logs[:core][idx]
            if core_log.category == :add_thunk && core_log.kind == :finish
                taskdeps = _logs[:taskdeps][idx]::Pair{Int,Vector{Int}}
                if taskdeps[1] == tid
                    return taskdeps[2]
                end
            end
        end
    end
    error("Task $tid not found in logs")
end
function all_tasks_in_logs(logs::Dict)
    all_tids = Int[]
    for w in keys(logs)
        _logs = logs[w]
        for idx in 1:length(_logs[:core])
            core_log = _logs[:core][idx]
            id_log = _logs[:id][idx]
            if core_log.category == :add_thunk && core_log.kind == :finish
                tid = id_log.thunk_id::Int
                push!(all_tids, tid)
            end
        end
    end
    return all_tids
end
mutable struct FlowEntry
    kind::Symbol
    tid::Int
    ainfo::AbstractAliasing
    to_ainfo::AbstractAliasing
    from_space::MemorySpace
    to_space::MemorySpace
    read::Bool
    write::Bool
end
struct FlowCheck
    read::Bool
    write::Bool
    arg_w::ArgumentWrapper
    orig_ainfo::AbstractAliasing
    orig_space::MemorySpace
    function FlowCheck(kind, arg, dep_mod=identity)
        if kind == :read
            read = true
            write = false
        elseif kind == :write
            read = false
            write = true
        elseif kind == :readwrite
            read = true
            write = true
        else
            error("Invalid kind: $kind")
        end
        arg_w = maybe_rewrap_arg_w(ArgumentWrapper(arg, dep_mod))
        return new(read, write, arg_w, aliasing(arg, dep_mod), memory_space(arg))
    end
end
struct FlowGraph
    g::SimpleDiGraph
    tid_to_v::Dict{Int,Int}
    FlowGraph() = new(SimpleDiGraph(), Dict{Int,Int}())
end
struct FlowState
    flows::Dict{ArgumentWrapper,Vector{FlowEntry}}
    graph::FlowGraph
    FlowState() = new(Dict{ArgumentWrapper,Vector{FlowEntry}}(), FlowGraph())
end
function maybe_rewrap_arg_w(arg_w::ArgumentWrapper)
    arg = arg_w.arg
    if arg isa DTask
        arg = fetch(arg; raw=true)
    end
    if arg isa Chunk && Dagger.root_worker_id(arg) == myid()
        arg = Dagger.unwrap(arg)
    end
    return ArgumentWrapper(arg, arg_w.dep_mod)
end
function build_dataflow(logs::Dict; verbose::Bool=false)
    state = FlowState()
    orig_ainfos = Dict{AbstractAliasing,AbstractAliasing}()
    ainfo_arg_w = Dict{AbstractAliasing,ArgumentWrapper}()

    function add_execute!(arg_w, orig_ainfo, ainfo, tid, space, read, write)
        ainfo_flows = get!(Vector{FlowEntry}, state.flows, arg_w)
        # Skip duplicates (same arg 2+ times to same task)
        dup_idx = findfirst(flow->flow.tid == tid, ainfo_flows)
        if dup_idx === nothing
            if !haskey(orig_ainfos, ainfo)
                orig_ainfos[ainfo] = orig_ainfo
            end
            if !haskey(ainfo_arg_w, ainfo)
                ainfo_arg_w[ainfo] = arg_w
            end
            verbose && println("Adding execute flow (tid $tid, space $space, read $read, write $write):\n  $orig_ainfo ->\n  $ainfo")
            verbose && println("  $(arg_w.dep_mod), $(arg_w.arg)")
            push!(ainfo_flows, FlowEntry(:execute, tid, ainfo, ainfo, space, space, read, write))
        else
            # Union read and write fields
            ainfo_flows[dup_idx].read |= read
            ainfo_flows[dup_idx].write |= write
        end
    end
    function add_copy!(arg_w, from_arg, to_arg, tid, from_space, to_space)
        dep_mod = arg_w.dep_mod
        from_ainfo = aliasing(from_arg, dep_mod)
        to_ainfo = aliasing(to_arg, dep_mod)
        if !haskey(orig_ainfos, from_ainfo)
            orig_ainfos[from_ainfo] = from_ainfo
        end
        if !haskey(ainfo_arg_w, from_ainfo)
            ainfo_arg_w[from_ainfo] = arg_w
        end
        if !haskey(ainfo_arg_w, to_ainfo)
            ainfo_arg_w[to_ainfo] = arg_w
        end
        orig_ainfo = orig_ainfos[from_ainfo]
        orig_ainfos[to_ainfo] = orig_ainfo
        arg_flows = get!(Vector{FlowEntry}, state.flows, arg_w)
        verbose && println("Adding copy flow (tid $tid, from_space $from_space, to_space $to_space):\n  $orig_ainfo ->\n  $to_ainfo")
        verbose && println("  $(arg_w.dep_mod), $(arg_w.arg)")
        push!(arg_flows, FlowEntry(:copy, tid, from_ainfo, to_ainfo, from_space, to_space, true, true))
    end

    # Populate graph from syncdeps
    seen = Set{Int}()
    to_visit = all_tasks_in_logs(logs)
    while !isempty(to_visit)
        this_tid = popfirst!(to_visit)
        this_tid in seen && continue
        push!(seen, this_tid)
        if !(this_tid in keys(state.graph.tid_to_v))
            add_vertex!(state.graph.g); state.graph.tid_to_v[this_tid] = nv(state.graph.g)
        end

        # Add syncdeps
        deps = taskdeps_for_task(logs, this_tid)
        for dep in deps
            if !(dep in keys(state.graph.tid_to_v))
                add_vertex!(state.graph.g); state.graph.tid_to_v[dep] = nv(state.graph.g)
            end
            add_edge!(state.graph.g, state.graph.tid_to_v[this_tid], state.graph.tid_to_v[dep])
            push!(to_visit, dep)
        end
    end

    # Populate flows and graphs from datadeps logs
    for w in keys(logs)
        _logs = logs[w]
        for idx in 1:length(_logs[:core])
            core_log = _logs[:core][idx]
            id_log = _logs[:id][idx]
            tl_log = _logs[:timeline][idx]
            if core_log.category == :datadeps_execute && core_log.kind == :finish
                tid = id_log.thunk_id
                for (remote_arg, depset) in zip(tl_log.args, tl_log.deps)
                    for dep in depset.deps
                        arg_w = maybe_rewrap_arg_w(dep.arg_w)
                        orig_ainfo = aliasing(arg_w.arg, arg_w.dep_mod)
                        remote_ainfo = aliasing(remote_arg, arg_w.dep_mod)
                        space = memory_space(remote_arg)
                        add_execute!(arg_w, orig_ainfo, remote_ainfo, tid, space, dep.readdep, dep.writedep)
                    end
                end
            elseif (core_log.category == :datadeps_copy || core_log.category == :datadeps_copy_skip) && core_log.kind == :finish
                tid = tl_log.thunk_id
                from_space = tl_log.from_space
                to_space = tl_log.to_space
                from_arg = tl_log.from_arg
                to_arg = tl_log.to_arg
                arg_w = maybe_rewrap_arg_w(tl_log.arg_w)
                add_copy!(arg_w, from_arg, to_arg, tid, from_space, to_space)
            end
        end
    end

    return state
end
function test_dataflow(state::FlowState, checks...; verbose::Bool=true)
    # Check that each ainfo starts and ends in the same space
    for arg_w in keys(state.flows)
        ainfo = aliasing(arg_w.arg, arg_w.dep_mod)
        arg_flows = state.flows[arg_w]
        orig_space = memory_space(arg_w.arg) #arg_flows[1].from_space
        #=if ainfo != arg_flows[1].ainfo
            verbose && println("Ainfo key $(ainfo) is not the same as the first flow's ainfo $(ainfo_flows[1].ainfo)")
            return false
        end=#
        final_space = arg_flows[end].to_space
        # FIXME: will_alias doesn't check across spaces
        any_writes = any(flows->Dagger.will_alias(flows[1], ainfo) && any(flow->flow.write, flows[2]), state.flows)
        if orig_space != final_space
            if verbose
                println("Arg ($(arg_w.dep_mod), $(arg_w.arg)) starts in $(orig_space) but ends in $(final_space)")
                for flow in arg_flows
                    println("  $(flow.kind) $(flow.tid) $(flow.from_space) -> $(flow.to_space)")
                end
            end
            return false
        end
    end

    # Check each flow against the previous flow, ensuring that the previous flow is a dominator of the current flow
    # FIXME: Validate non-dominance when unnecessary?
    for arg_w in keys(state.flows)
        arg_flows = state.flows[arg_w]
        for (idx, flow) in enumerate(arg_flows)
            if idx > 1
                prev_flow = arg_flows[idx-1]
                if !prev_flow.write && !flow.write
                    # R->R don't depend on each other
                    continue
                end
                if !prev_flow.write && flow.write && prev_flow.kind == :execute && flow.kind == :copy && prev_flow.ainfo != flow.to_ainfo
                    # Copy only writes to a different ainfo, so don't depend on each other
                    continue
                end
                if flow.tid == 0
                    # Ignore copy skip flows
                    continue
                end
                v = state.graph.tid_to_v[flow.tid]
                prev_v = state.graph.tid_to_v[prev_flow.tid]
                path_state = dijkstra_shortest_paths(state.graph.g, v; allpaths=true)
                if path_state.pathcounts[prev_v] == 0
                    if verbose
                        println("Flow $(idx-1) (tid $(prev_flow.tid), $(prev_flow.kind), R:$(prev_flow.read), W:$(prev_flow.write)) is not a dominator of flow $(idx) (tid $(flow.tid), $(flow.kind), R:$(flow.read), W:$(flow.write))")
                        @show length(state.flows[arg_w])
                        for flow in state.flows[arg_w]
                            println("  $(flow.kind) $(flow.tid) $(flow.from_space) -> $(flow.to_space) (R:$(flow.read), W:$(flow.write))")
                        end
                        for flow in state.flows[arg_w]
                            println("  May write to: $(flow.to_ainfo)")
                        end
                        e_vs = collect(edges(state.graph.g))
                        e_tids = map(e->Edge(only(filter(tv->tv[2]==src(e), state.graph.tid_to_v))[1],
                                            only(filter(tv->tv[2]==dst(e), state.graph.tid_to_v))[1]),
                                    e_vs)
                        sort!(e_tids)
                        for e in e_tids
                            s_tid, d_tid = src(e), dst(e)
                            println("Edge: $s_tid -(up)> $d_tid")
                        end
                    end
                    return false
                end
            end
        end
    end

    # Walk through each check, ensuring that the current state of the flow matches the check
    arg_locations = Dict{ArgumentWrapper,MemorySpace}()
    flow_idxs = Dict{ArgumentWrapper,Int}(arg_w=>1 for arg_w in keys(state.flows))
    for (idx, check) in enumerate(checks)
        # Record the original location of the ainfo
        if !haskey(arg_locations, check.arg_w)
            arg_locations[check.arg_w] = check.orig_space
        end

        # Try to advance a flow
        if !haskey(flow_idxs, check.arg_w)
            if verbose
                @warn "Didn't encounter argument ($(check.arg_w.dep_mod), $(check.arg_w.arg))"
                println("Seen arguments:")
                for arg_w in keys(state.flows)
                    println("  ($(arg_w.dep_mod), $(arg_w.arg))")
                end
                return false
            end
        end
        flow_idx = flow_idxs[check.arg_w]
        while true
            if flow_idx > length(state.flows[check.arg_w])
                verbose && println("Exhausted all tasks while trying to find $(check.arg_w)")
                return false
            end
            flow = state.flows[check.arg_w][flow_idx]
            if flow.kind == :execute
                # The current flow state must match the check
                if flow.read == check.read && flow.write == check.write
                    # Match, move on to next check
                    flow_idx += 1
                    break
                else
                    verbose && println("Expected ($(check.read), $(check.write)), got ($(flow.read), $(flow.write))")
                    return false
                end
            elseif flow.kind == :copy
                # We need to advance our ainfo location
                # FIXME: Assert proper data progression (requires more complex tracking of other arguments)
                #@assert flow.from_space == arg_locations[check.arg_w]
                arg_locations[check.arg_w] = flow.to_space
                flow_idx += 1
            end
        end

        flow_idxs[check.arg_w] = flow_idx
    end

    return true
end

@everywhere do_nothing(Xs...) = nothing
@everywhere mut_ref!(R) = (R[] .= 0;)
@everywhere mut_V!(V) = (V .= 1;)
function test_datadeps(;args_chunks::Bool,
                        args_thunks::Bool,
                        args_loc::Int)
    # Returns last value
    @test Dagger.spawn_datadeps() do
        42
    end == 42

    # Tasks are started and finished as spawn_datadeps returns
    ts = []
    Dagger.spawn_datadeps() do
        for i in 1:5
            t = Dagger.@spawn sleep(0.1)
            @test !istaskstarted(t)
        end
    end
    @test all(istaskdone, ts)

    # Rethrows any task exceptions
    @test_throws Exception Dagger.spawn_datadeps() do
        Dagger.@spawn error("Test")
    end

    A = rand(1)
    if args_chunks
        A = remotecall_fetch(Dagger.tochunk, args_loc, A)
    elseif args_thunks
        A = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)
    end

    @warn "Negative-test the test_dataflow helper"

    # Task return values can be tracked
    ts = []
    local t1
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            t1 = Dagger.@spawn fill(42, 1)
            push!(ts, t1)
            push!(ts, Dagger.@spawn copyto!(Out(A), In(t1)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    @test fetch(A)[1] == 42.0
    state = build_dataflow(logs)

    # FIXME: We don't record the task as a syncdep, but instead internally `fetch` the chunk
    # We don't see the :readwrite because we don't see the use of t1
    #@test test_dataflow(state, FlowCheck(:readwrite, t1))
    @test test_dataflow(state, FlowCheck(:read, t1), FlowCheck(:write, A))

    # R->R Non-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            push!(ts, Dagger.@spawn do_nothing(In(A)))
            push!(ts, Dagger.@spawn do_nothing(In(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    state = build_dataflow(logs)
    test_dataflow(state, FlowCheck(:read, A), FlowCheck(:read, A))

    # R->W Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            push!(ts, Dagger.@spawn do_nothing(In(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    state = build_dataflow(logs)
    @test test_dataflow(state, FlowCheck(:read, A), FlowCheck(:write, A))

    # W->W Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            push!(ts, Dagger.@spawn do_nothing(Out(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    state = build_dataflow(logs)
    @test test_dataflow(state, FlowCheck(:write, A), FlowCheck(:write, A))

    # R->R Non-Self-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            push!(ts, Dagger.@spawn do_nothing(In(A), In(A)))
            push!(ts, Dagger.@spawn do_nothing(In(A), In(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    state = build_dataflow(logs)
    @test test_dataflow(state, FlowCheck(:read, A), FlowCheck(:read, A))

    # R->W Self-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            push!(ts, Dagger.@spawn do_nothing(In(A), In(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A), Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    state = build_dataflow(logs)
    @test test_dataflow(state, FlowCheck(:read, A), FlowCheck(:write, A))

    # W->W Self-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            push!(ts, Dagger.@spawn do_nothing(Out(A), Out(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A), Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    state = build_dataflow(logs)
    @test test_dataflow(state, FlowCheck(:write, A), FlowCheck(:write, A))

    function wrap_chunk_thunk(f, args...)
        if args_thunks || args_chunks
            result = Dagger.@spawn scope=Dagger.scope(worker=args_loc) f(args...)
            if args_thunks
                return result
            elseif args_chunks
                return fetch(result; raw=true)
            end
        else
            # N.B. We don't allocate remotely for raw data
            return f(args...)
        end
    end
    B = wrap_chunk_thunk(rand, 4, 4)

    # Views
    B_ul = wrap_chunk_thunk(view, B, 1:2, 1:2)
    B_ur = wrap_chunk_thunk(view, B, 1:2, 3:4)
    B_ll = wrap_chunk_thunk(view, B, 3:4, 1:2)
    B_lr = wrap_chunk_thunk(view, B, 3:4, 3:4)
    B_mid = wrap_chunk_thunk(view, B, 2:3, 2:3)
    for (B_name, B_view) in (
                                (:B_ul, B_ul),
                                (:B_ur, B_ur),
                                (:B_ll, B_ll),
                                (:B_lr, B_lr),
                                (:B_mid, B_mid))
        @test Dagger.will_alias(Dagger.aliasing(B), Dagger.aliasing(B_view))
        B_view === B_mid && continue
        @test Dagger.will_alias(Dagger.aliasing(B_mid), Dagger.aliasing(B_view))
    end
    local t_A, t_B, t_ul, t_ur, t_ll, t_lr, t_mid
    local t_ul2, t_ur2, t_ll2, t_lr2
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            t_A = Dagger.@spawn do_nothing(InOut(A))
            t_B = Dagger.@spawn do_nothing(InOut(B))
            t_ul = Dagger.@spawn do_nothing(InOut(B_ul))
            t_ur = Dagger.@spawn do_nothing(InOut(B_ur))
            t_ll = Dagger.@spawn do_nothing(InOut(B_ll))
            t_lr = Dagger.@spawn do_nothing(InOut(B_lr))
            t_mid = Dagger.@spawn do_nothing(InOut(B_mid))
            t_ul2 = Dagger.@spawn do_nothing(InOut(B_ul))
            t_ur2 = Dagger.@spawn do_nothing(InOut(B_ur))
            t_ll2 = Dagger.@spawn do_nothing(InOut(B_ll))
            t_lr2 = Dagger.@spawn do_nothing(InOut(B_lr))
        end
    end
    tid_A, tid_B, tid_ul, tid_ur, tid_ll, tid_lr, tid_mid =
        task_id.([t_A, t_B, t_ul, t_ur, t_ll, t_lr, t_mid])
    tid_ul2, tid_ur2, tid_ll2, tid_lr2 =
        task_id.([t_ul2, t_ur2, t_ll2, t_lr2])
    tids_all = [tid_A, tid_B, tid_ul, tid_ur, tid_ll, tid_lr, tid_mid,
                tid_ul2, tid_ur2, tid_ll2, tid_lr2]
    state = build_dataflow(logs)
    @test test_dataflow(state, FlowCheck(:readwrite, A))
    @test test_dataflow(state, FlowCheck(:readwrite, B))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_ul))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_ur))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_ll))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_lr))
    for arg in [B_ul, B_ur, B_ll, B_lr]
        @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, arg), FlowCheck(:readwrite, B_mid), FlowCheck(:readwrite, arg))
    end

    # (Unit)Upper/LowerTriangular and Diagonal
    B_upper = wrap_chunk_thunk(UpperTriangular, B)
    B_unitupper = wrap_chunk_thunk(UnitUpperTriangular, B)
    B_lower = wrap_chunk_thunk(LowerTriangular, B)
    B_unitlower = wrap_chunk_thunk(UnitLowerTriangular, B)
    for (B_name, B_view) in (
                                (:B_upper, B_upper),
                                (:B_unitupper, B_unitupper),
                                (:B_lower, B_lower),
                                (:B_unitlower, B_unitlower))
        @test Dagger.will_alias(Dagger.aliasing(B), Dagger.aliasing(B_view))
    end
    @test Dagger.will_alias(Dagger.aliasing(B_upper), Dagger.aliasing(B_lower))
    @test !Dagger.will_alias(Dagger.aliasing(B_unitupper), Dagger.aliasing(B_unitlower))
    @test Dagger.will_alias(Dagger.aliasing(B_upper), Dagger.aliasing(B_unitupper))
    @test Dagger.will_alias(Dagger.aliasing(B_lower), Dagger.aliasing(B_unitlower))

    @test Dagger.will_alias(Dagger.aliasing(B_upper), Dagger.aliasing(B, Diagonal))
    @test Dagger.will_alias(Dagger.aliasing(B_lower), Dagger.aliasing(B, Diagonal))
    @test !Dagger.will_alias(Dagger.aliasing(B_unitupper), Dagger.aliasing(B, Diagonal))
    @test !Dagger.will_alias(Dagger.aliasing(B_unitlower), Dagger.aliasing(B, Diagonal))

    local t_A, t_B, t_upper, t_unitupper, t_lower, t_unitlower, t_diag
    local t_upper2, t_unitupper2, t_lower2, t_unitlower2
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            t_A = Dagger.@spawn do_nothing(InOut(A))
            t_B = Dagger.@spawn do_nothing(InOut(B))
            t_upper = Dagger.@spawn do_nothing(InOut(B_upper))
            t_unitupper = Dagger.@spawn do_nothing(InOut(B_unitupper))
            t_lower = Dagger.@spawn do_nothing(InOut(B_lower))
            t_unitlower = Dagger.@spawn do_nothing(InOut(B_unitlower))
            t_diag = Dagger.@spawn do_nothing(Deps(B, InOut(Diagonal)))
            t_unitlower2 = Dagger.@spawn do_nothing(InOut(B_unitlower))
            t_lower2 = Dagger.@spawn do_nothing(InOut(B_lower))
            t_unitupper2 = Dagger.@spawn do_nothing(InOut(B_unitupper))
            t_upper2 = Dagger.@spawn do_nothing(InOut(B_upper))
        end
    end
    tid_A, tid_B, tid_upper, tid_unitupper, tid_lower, tid_unitlower, tid_diag =
        task_id.([t_A, t_B, t_upper, t_unitupper, t_lower, t_unitlower, t_diag])
    tid_upper2, tid_unitupper2, tid_lower2, tid_unitlower2 =
        task_id.([t_upper2, t_unitupper2, t_lower2, t_unitlower2])
    tids_all = [tid_A, tid_B, tid_upper, tid_unitupper, tid_lower, tid_unitlower, tid_diag,
                tid_upper2, tid_unitupper2, tid_lower2, tid_unitlower2]
    state = build_dataflow(logs)
    @test test_dataflow(state, FlowCheck(:readwrite, A))
    @test test_dataflow(state, FlowCheck(:readwrite, B))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_upper), FlowCheck(:readwrite, B_unitupper))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_upper), FlowCheck(:readwrite, B_lower))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_lower), FlowCheck(:readwrite, B_unitlower))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_upper), FlowCheck(:readwrite, B_lower),
                               FlowCheck(:readwrite, B, Diagonal))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_lower), FlowCheck(:readwrite, B_unitlower),
                               FlowCheck(:readwrite, B, Diagonal), FlowCheck(:readwrite, B_unitlower))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_lower), FlowCheck(:readwrite, B_unitlower),
                               FlowCheck(:readwrite, B, Diagonal), FlowCheck(:readwrite, B_unitlower), FlowCheck(:readwrite, B_lower))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_upper), FlowCheck(:readwrite, B_unitupper),
                               FlowCheck(:readwrite, B_unitupper))
    @test test_dataflow(state, FlowCheck(:readwrite, B), FlowCheck(:readwrite, B_upper), FlowCheck(:readwrite, B_unitupper),
                               FlowCheck(:readwrite, B, Diagonal), FlowCheck(:readwrite, B_unitupper), FlowCheck(:readwrite, B_upper))

    # Additional aliasing tests
    views_overlap(x, y) = Dagger.will_alias(Dagger.aliasing(x), Dagger.aliasing(y))

    A = wrap_chunk_thunk(identity, B)

    A_r1 = wrap_chunk_thunk(view, A, 1:1, 1:4)
    A_r2 = wrap_chunk_thunk(view, A, 2:2, 1:4)
    B_r1 = wrap_chunk_thunk(view, B, 1:1, 1:4)
    B_r2 = wrap_chunk_thunk(view, B, 2:2, 1:4)

    A_c1 = wrap_chunk_thunk(view, A, 1:4, 1:1)
    A_c2 = wrap_chunk_thunk(view, A, 1:4, 2:2)
    B_c1 = wrap_chunk_thunk(view, B, 1:4, 1:1)
    B_c2 = wrap_chunk_thunk(view, B, 1:4, 2:2)

    A_mid = wrap_chunk_thunk(view, A, 2:3, 2:3)
    B_mid = wrap_chunk_thunk(view, B, 2:3, 2:3)

    @test views_overlap(A_r1, A_r1)
    @test views_overlap(B_r1, B_r1)
    @test views_overlap(A_c1, A_c1)
    @test views_overlap(B_c1, B_c1)

    @test views_overlap(A_r1, B_r1)
    @test views_overlap(A_r2, B_r2)
    @test views_overlap(A_c1, B_c1)
    @test views_overlap(A_c2, B_c2)

    @test !views_overlap(A_r1, A_r2)
    @test !views_overlap(B_r1, B_r2)
    @test !views_overlap(A_c1, A_c2)
    @test !views_overlap(B_c1, B_c2)

    @test views_overlap(A_r1, A_c1)
    @test views_overlap(A_r1, B_c1)
    @test views_overlap(A_r2, A_c2)
    @test views_overlap(A_r2, B_c2)

    for (name, mid) in ((:A_mid, A_mid), (:B_mid, B_mid))
        @test !views_overlap(A_r1, mid)
        @test !views_overlap(B_r1, mid)
        @test !views_overlap(A_c1, mid)
        @test !views_overlap(B_c1, mid)

        @test views_overlap(A_r2, mid)
        @test views_overlap(B_r2, mid)
        @test views_overlap(A_c2, mid)
        @test views_overlap(B_c2, mid)
    end

    @test views_overlap(A_mid, A_mid)
    @test views_overlap(A_mid, B_mid)

    # SubArray hashing
    V = zeros(3)
    Dagger.spawn_datadeps() do
        Dagger.@spawn mut_V!(InOut(view(V, 1:2)))
        Dagger.@spawn mut_V!(InOut(view(V, 2:3)))
    end
    @test fetch(V) == [1, 1, 1]

    # FIXME: Deps

    # Outer Scope
    exec_procs = fetch.(Dagger.spawn_datadeps() do
        [Dagger.@spawn Dagger.task_processor() for i in 1:10]
    end)
    unique!(exec_procs)
    scope = Dagger.get_compute_scope()
    all_procs = vcat([collect(Dagger.get_processors(OSProc(w))) for w in procs()]...)
    scope_procs = filter(proc->!isa(Dagger.constrain(scope, ExactScope(proc)), Dagger.InvalidScope), all_procs)
    for proc in exec_procs
        @test proc in scope_procs
    end
    for proc in scope_procs
        proc == Dagger.ThreadProc(1, 1) && continue
        @test proc in exec_procs
    end

    # Inner Scope
    @test_throws Dagger.Sch.SchedulingException Dagger.spawn_datadeps() do
        Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 5000)) 1+1
    end

    # Field aliasing
    X = Ref(rand(1000))
    @test all(x->x==0, fetch(Dagger.spawn_datadeps() do
        Dagger.@spawn mut_ref!(Deps(X, InOut(:x)))
        Dagger.@spawn getfield(Deps(X, In(:x)), :x)
    end))

    # Add-to-copy
    A = rand(1000)
    B = rand(1000)
    C = rand(1000)
    D = zeros(1000)
    add!(X, Y) = (X .+= Y;)
    expected = (A .+ B) .+ (A .+ C)
    if args_chunks
        A = remotecall_fetch(Dagger.tochunk, args_loc, A)
        B = remotecall_fetch(Dagger.tochunk, args_loc, B)
        C = remotecall_fetch(Dagger.tochunk, args_loc, C)
        D = remotecall_fetch(Dagger.tochunk, args_loc, D)
    elseif args_thunks
        A = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)
        B = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(B)
        C = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(C)
        D = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(D)
    end
    Dagger.spawn_datadeps() do
        Dagger.@spawn add!(InOut(B), In(A))
        Dagger.@spawn add!(InOut(C), In(A))
        Dagger.@spawn add!(InOut(C), In(B))
        Dagger.@spawn copyto!(Out(D), In(C))
    end
    @test isapprox(fetch(C), expected)
    @test isapprox(fetch(D), expected)

    # Tree reduce
    As = [rand(1000) for _ in 1:1000]
    expected = reduce((x,y)->x .+ y, As)
    if args_chunks
        As = map(A->remotecall_fetch(Dagger.tochunk, args_loc, A), As)
    elseif args_thunks
        As = map(A->(Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)), As)
    end
    Dagger.spawn_datadeps() do
        to_reduce = Vector[]
        push!(to_reduce, As)
        while !isempty(to_reduce)
            As = pop!(to_reduce)
            n = length(As)
            if n == 2
                Dagger.@spawn Base.mapreducedim!(identity, +, InOut(As[1]), In(As[2]))
            elseif n > 2
                push!(to_reduce, [As[1], As[div(n,2)+1]])
                push!(to_reduce, As[1:div(n,2)])
                push!(to_reduce, As[div(n,2)+1:end])
            end
        end
    end
    @test isapprox(fetch(As[1]), expected)

    # Cholesky
    m, n = 1000, 1000
    nb = 100
    mt, nt = fld(m+nb-1, nb), fld(n+nb-1, nb)
    M_dense = rand(m, n)
    # Make M positive definite
    M_dense = M_dense * M_dense'
    expected = copy(M_dense); LAPACK.potrf!('L', expected)
    M = [M_dense[i:(i+nb-1), j:(j+nb-1)] for i in 1:nb:m, j in 1:nb:n]
    if args_chunks
        M = map(m->remotecall_fetch(Dagger.tochunk, args_loc, m), M)
    elseif args_thunks
        M = map(m->(Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(m)), M)
    end
    Dagger.spawn_datadeps() do
        for k in range(1, mt)
            Dagger.@spawn LAPACK.potrf!('L', InOut(M[k, k]))
            for _m in range(k+1, mt)
                Dagger.@spawn BLAS.trsm!('R', 'L', 'T', 'N', 1.0, In(M[k, k]), InOut(M[_m, k]))
            end
            for _n in range(k+1, nt)
                Dagger.@spawn BLAS.syrk!('L', 'N', -1.0, In(M[_n, k]), 1.0, InOut(M[_n, _n]))
                for _m in range(_n+1, mt)
                    Dagger.@spawn BLAS.gemm!('N', 'T', -1.0, In(M[_m, k]), In(M[_n, k]), 1.0, InOut(M[_m, _n]))
                end
            end
        end
    end
    for i in 1:nb:m, j in 1:nb:n
        M_dense[i:(i+nb-1), j:(j+nb-1)] .= fetch(M[div(i,nb)+1, div(j,nb)+1])
    end
    @test isapprox(M_dense, expected)
end

@testset "$args_mode Data" for args_mode in (:Raw, :Chunk, :Thunk)
    args_chunks = args_mode == :Chunk
    args_thunks = args_mode == :Thunk
    for nw in (1, 2)
        args_loc = nw == 2 ? 2 : 1
        for nt in (1, 2)
            if nprocs() >= nw && Threads.nthreads() >= nt
                @testset "$nw Workers, $nt Threads" begin
                    Dagger.with_options(;scope=Dagger.scope(workers=1:nw, threads=1:nt)) do
                        test_datadeps(;args_chunks, args_thunks, args_loc)
                    end
                end
            end
        end
    end
end

# Custom scheduler tests

struct DummyErrorScheduler <: Dagger.DataDepsScheduler end
struct DummySchedulerError <: Exception end
function Dagger.datadeps_schedule_task(::DummyErrorScheduler, state, all_procs, all_scope, task_scope, spec, task)
    throw(DummySchedulerError())
end

# Shared workload for the per-scheduler tests below. Kernels are `@everywhere`
# because a scheduler is free to place any of them on any worker -- which is
# the entire point of the exercise.
@everywhere sched_fill_tile!(y, v) = (fill!(y, v); nothing)
@everywhere sched_axpy_tile!(y, x, a) = (y .+= a .* x; nothing)
@everywhere sched_scale_tile!(y, a) = (y .*= a; nothing)
@everywhere sched_accum_tile!(acc, x) = (acc[1] += sum(x); nothing)
@everywhere sched_where(_) = Dagger.task_processor()

const SCHED_NT = 4      # tiles
const SCHED_TILE = 32   # elements per tile

sched_region_args() = ([zeros(SCHED_TILE) for _ in 1:SCHED_NT], zeros(1))

"""
    sched_region!(C, acc; spawn::Bool)

One region's worth of work, or -- with `spawn=false` -- the plain sequential
Julia that must produce the identical answer.

Shaped to be hostile to a broken scheduler rather than merely non-trivial:

- every `C[j]` is written by four separate tasks in sequence (`Out`, then
  three `InOut`s), so a lost or reordered write-after-write shows up in the
  result rather than being masked;
- `acc` is `InOut` in every tile's last task, which serializes across tiles
  and therefore forces the copy-back/copy-to path whenever consecutive tiles
  land in different memory spaces;
- the constants depend on both `j` and `k`, so a task placed correctly but
  fed a stale replica of `C[j]` produces a wrong number rather than a
  coincidentally-equal one.

`spawn_datadeps` guarantees results equivalent to sequential execution in
submission order, so `spawn=false` is a genuine oracle, not an approximation.
"""
function sched_region!(C, acc; spawn::Bool)
    for j in 1:SCHED_NT
        if spawn
            Dagger.@spawn sched_fill_tile!(Out(C[j]), Float64(j))
        else
            sched_fill_tile!(C[j], Float64(j))
        end
        for k in 1:3
            a = 1 / (j + k)
            if spawn
                Dagger.@spawn sched_axpy_tile!(InOut(C[j]), In(C[mod1(j + k, SCHED_NT)]), a)
            else
                sched_axpy_tile!(C[j], C[mod1(j + k, SCHED_NT)], a)
            end
        end
        if spawn
            Dagger.@spawn sched_scale_tile!(InOut(C[j]), 0.5)
            Dagger.@spawn sched_accum_tile!(InOut(acc), In(C[j]))
        else
            sched_scale_tile!(C[j], 0.5)
            sched_accum_tile!(acc, C[j])
        end
    end
    return
end

@testset "Custom Schedulers" begin
    @testset "DummyErrorScheduler" begin
        # Test that our custom scheduler is actually called by Datadeps
        @test_throws DummySchedulerError Dagger.spawn_datadeps(; scheduler=DummyErrorScheduler()) do
            Dagger.@spawn 1 + 1
        end
    end

    @testset "$sched_name" for (sched_name, make_sched) in
            ("RoundRobinScheduler" => Dagger.RoundRobinScheduler,
             "NaiveScheduler"      => Dagger.NaiveScheduler,
             "UltraScheduler"      => Dagger.UltraScheduler)
        @testset "Smoke" begin
            A = rand(10)
            result = Dagger.spawn_datadeps(; scheduler=make_sched()) do
                Dagger.@spawn sum(In(A))
            end
            @test fetch(result) ≈ sum(A)
        end

        # The real test. A single read-only `sum(In(A))` cannot distinguish a
        # scheduler that works from one that returns an arbitrary processor:
        # there is nothing to order and nothing to copy back, so *any*
        # placement produces the right answer. `sched_region!` below is
        # write-heavy and cross-tile-serialized, so it is only correct if the
        # scheduler's placement, the resulting copies, and the dependency
        # ordering all hold together.
        @testset "Multi-task, mixed read/write" begin
            for hierarchical in (true, false)
                @testset "hierarchical=$hierarchical" begin
                    C, acc = sched_region_args()
                    C_ref, acc_ref = sched_region_args()
                    sched_region!(C_ref, acc_ref; spawn=false)
                    Dagger.spawn_datadeps(; scheduler=make_sched(), hierarchical) do
                        sched_region!(C, acc; spawn=true)
                    end
                    @test C ≈ C_ref
                    @test acc ≈ acc_ref
                end
            end
        end

        # A scheduler that always returned the same processor would still pass
        # the correctness tests above, so assert placement actually spreads.
        #
        # `hierarchical=false` deliberately. Under the default hierarchical
        # partitioner this measures the *partitioner*, not the scheduler:
        # `partition_dag` assigns each task to the owner holding the most of
        # its argument data before any scheduler runs, and these tiles are all
        # freshly allocated on worker 1, so every task is (correctly, for a
        # locality heuristic) partitioned onto worker 1 and every scheduler --
        # including plain round robin -- returns one processor. Measured with
        # 4 workers x 1 thread: all three schedulers place all 16 tasks on
        # `ThreadProc(1, 1)` with `hierarchical=true`. See the report in the
        # commit message for why that is the partitioner's call to make.
        #
        # Skipped when there is only one processor to spread *over*, which is
        # a property of how the suite was invoked, not of the scheduler.
        #
        # What counts as "spread" depends on the invocation, and getting this
        # wrong makes the test lie about `NaiveScheduler`. With more than one
        # memory space present, the meaningful question is whether tasks reach
        # more than one *space*: `NaiveScheduler` never sends one across (see
        # below), but `estimate_task_costs` shuffles equally-costly processors
        # with `randperm!`, so it does scatter over the threads of whichever
        # single worker it picked -- which looks like spreading and isn't. With
        # only one space, that shuffle is the only spreading available to
        # anyone and processor identity is the right measure.
        @testset "Uses more than one processor (flat path)" begin
            all_procs = collect(Dagger.all_processors())
            spaces = unique(only(Dagger.memory_spaces(p)) for p in all_procs)
            multi_space = length(spaces) > 1
            if length(all_procs) < 2
                @test_skip "Needs >1 processor (more workers, or -t 2+)"
            else
                tiles = [rand(64) for _ in 1:16]
                tasks = Dagger.spawn_datadeps(; scheduler=make_sched(), hierarchical=false) do
                    map(t->Dagger.@spawn(sched_where(In(t))), tiles)
                end
                placed = fetch.(tasks)
                spread = if multi_space
                    length(unique(only(Dagger.memory_spaces(p)) for p in placed)) > 1
                else
                    length(unique(placed)) > 1
                end
                if make_sched === Dagger.NaiveScheduler && multi_space
                    # Known and inherent, not a regression: `NaiveScheduler`
                    # costs every task against the *live* scheduler's pressure,
                    # which planning does not move -- only tasks that have
                    # actually run do. With every tile resident on worker 1,
                    # `estimate_task_costs` charges every other worker a
                    # cross-worker transfer plus the fixed 1ms task-transfer
                    # cost, for every task, so worker 1 wins all 16 times.
                    # `@test_broken` rather than a skip so this flags if the
                    # scheduler ever grows a memory of its own decisions.
                    @test_broken spread
                else
                    @test spread
                end
            end
        end

        # Placement must respect a per-task scope, not just the region-wide
        # one: `all_procs` is pre-filtered by the region scope, but a
        # `scope=`/`compute_scope=` on an individual `@spawn` is handed to the
        # scheduler as `task_scope`, and honoring it is the scheduler's job.
        # Both `NaiveScheduler` and `UltraScheduler` used to ignore it
        # entirely and hand back whatever their cost model ranked first, which
        # `distribute_task!` then rejected with an `InvalidScope` -- a region
        # failing on a constraint that was perfectly satisfiable.
        #
        # Pinned to the *last* processor rather than the first, and run on the
        # flat path, so the answer can't be right by accident: the data all
        # starts on worker 1, so "wherever the data is" and "whatever comes
        # first in `all_procs`" are both the wrong answer here.
        @testset "Respects a per-task scope" begin
            if length(Dagger.all_processors()) < 2
                @test_skip "Needs >1 processor (more workers, or -t 2+)"
            else
                pinned = last(sort!(collect(Dagger.all_processors()); by=repr))
                tiles = [rand(64) for _ in 1:8]
                tasks = Dagger.spawn_datadeps(; scheduler=make_sched(), hierarchical=false) do
                    map(tiles) do t
                        Dagger.@spawn scope=Dagger.ExactScope(pinned) sched_where(In(t))
                    end
                end
                @test all(==(pinned), fetch.(tasks))
            end
        end
    end

    # `UltraScheduler` is the only one of the three that simulates the region
    # it is planning, so it is the only one whose decisions are worth
    # inspecting directly. These are white-box assertions about the model, run
    # against the same synthetic setup `datadeps_locality.jl` uses.
    @testset "UltraScheduler placement model" begin
        if length(Dagger.all_processors()) < 2
            @test_skip "Needs >1 processor (more workers, or -t 2+)"
        else
            all_procs = sort!(collect(Dagger.all_processors()); by=repr)
            all_scope = Dagger.UnionScope(map(Dagger.ExactScope, all_procs))
            state = Dagger.DataDepsState()
            probe = Dagger.@spawn 1 + 1
            fetch(probe)
            spec = Dagger.DTaskSpec(Dagger.Argument[Dagger.Argument(1, sched_where),
                                                     Dagger.Argument(2, In(rand(8)))],
                                    Dagger.Options())

            # Every task costs the same (nothing has been measured, so they
            # all get the placeholder) and nothing is resident anywhere, so
            # earliest-finish-time reduces to strict round robin over
            # `all_procs` -- the same load-spreading round robin does, but
            # arrived at by simulation rather than by rotation.
            sched = Dagger.UltraScheduler()
            n = length(all_procs)
            placed = map(1:(2n)) do _
                Dagger.datadeps_schedule_task(sched, state, all_procs, all_scope,
                                              all_scope, spec, probe)
            end
            @test placed == vcat(all_procs, all_procs)

            # Placement is a pure function of the scheduler's state, so a
            # fresh shard (what `similar` hands each hierarchical partition)
            # must replay identically rather than continuing the first one.
            sched2 = Base.similar(sched)
            @test sched2 !== sched
            replayed = map(1:n) do _
                Dagger.datadeps_schedule_task(sched2, state, all_procs, all_scope,
                                              all_scope, spec, probe)
            end
            @test replayed == all_procs

            # Pinning every task to one processor must not silently spill onto
            # another just because that one now looks less loaded.
            pinned = all_procs[end]
            sched3 = Dagger.UltraScheduler()
            pinned_scope = Dagger.ExactScope(pinned)
            @test all(1:(2n)) do _
                Dagger.datadeps_schedule_task(sched3, state, all_procs, all_scope,
                                              pinned_scope, spec, probe) === pinned
            end
        end
    end
end

# Regression tests for the free-syncdeps hole: `gather_free_syncdeps!`
# (src/datadeps/aliasing.jl) must never hand back an empty syncdep set while
# some task could still be reading or writing the buffer about to be freed.
# Previously, the region barrier at the end of `spawn_datadeps` hid this --
# every task had already retired by the time the free loop ran -- but that
# cover disappears once frees can be deferred, so it needs to be correct on
# its own merits.

# A leaf type whose own `aliasing` we pretend is unavailable to inspect
# locally, mimicking the MPI situation this branch exists for (an `MPIRef`
# owned by a different rank -- see `ext/MPIExt.jl`'s override of the same
# name). This lets us exercise `gather_free_syncdeps!`'s
# `!aliasing_obtainable` fallback without needing an actual MPI session.
struct FreeSyncdepsUnavailableLeaf
    data::Vector{Float64}
end
Dagger.aliasing_available(::Dagger.Chunk{FreeSyncdepsUnavailableLeaf}) = false

@testset "gather_free_syncdeps!" begin
    @testset "aliasing-unavailable fallback finds overlapping writers" begin
        # Before the fix, this fallback only synced when the buffer's
        # rank-uniform cache key ainfo happened to *already* be a registered
        # key of `state.ainfos_overlaps` -- which only holds ainfos that were
        # directly tracked as a task dependency. A buffer standing in for
        # memory that's only tracked *through* an overlapping (but distinct)
        # ainfo -- exactly the situation set up below -- hit `haskey(...) ==
        # false` and silently produced zero syncdeps, so `unsafe_free!` could
        # race the still-running writer.
        state = Dagger.DataDepsState()

        # A directly-tracked ainfo (as if some task took `In`/`Out` of a view
        # over part of `A`) with a live writer recorded against it.
        A = zeros(8)
        view_ainfo = Dagger.AliasingWrapper(Dagger.aliasing(view(A, 1:4)))
        push!(state.ainfos_lookup, view_ainfo)
        state.ainfos_readers[view_ainfo] = Pair{Dagger.DTask,Int}[]
        writer_task = Dagger.@spawn 1 + 1
        fetch(writer_task)
        state.ainfos_owner[view_ainfo] = writer_task => 1

        # The buffer being freed represents *all* of `A` (as the shared parent
        # backing a view would), so it overlaps `view_ainfo` but is not
        # content-identical to it -- `state.ainfos_overlaps` has no entry for
        # it, matching the case that was silently mishandled.
        key_ainfo = Dagger.AliasingWrapper(Dagger.aliasing(A))
        @test !haskey(state.ainfos_overlaps, key_ainfo)

        remote_arg = Dagger.tochunk(FreeSyncdepsUnavailableLeaf(A))
        @test !Dagger.aliasing_obtainable(remote_arg)
        space = Dagger.memory_space(remote_arg)
        chunk_to_ainfos = IdDict{Any,Vector{Dagger.AliasingWrapper}}()
        syncdeps = Set{Dagger.ThunkSyncdep}()
        Dagger.gather_free_syncdeps!(state, space, key_ainfo, remote_arg, 2,
                                     chunk_to_ainfos, syncdeps)

        @test !isempty(syncdeps)
        @test Dagger.ThunkSyncdep(writer_task) in syncdeps
    end

    @testset "buffer underlying shared views" begin
        # End-to-end version of the same hole: two views into disjoint slices
        # of one parent array, each written by a task on the same remote
        # worker. The parent array is moved there exactly once (`move_rewrap`
        # dedups children of both views onto the same object-cache entry), and
        # that shared parent buffer is never itself a direct task argument --
        # it only "underlies" the two view arguments -- so its free syncdeps
        # must be computed via the aliasing-overlap search, not a direct
        # `chunk_to_ainfos` hit. Confirm every `unsafe_free!` task the region
        # emits has a non-empty syncdep set.
        if nprocs() >= 2
            w = workers()[1]
            A = rand(4, 4)
            v1 = view(A, 1:2, :)
            v2 = view(A, 3:4, :)
            logs = with_logs() do
                Dagger.spawn_datadeps() do
                    Dagger.@spawn scope=Dagger.scope(worker=w) mut_V!(Out(v1))
                    Dagger.@spawn scope=Dagger.scope(worker=w) mut_V!(Out(v2))
                end
            end
            @test all(==(1), A)

            free_tids = Int[]
            for wl in keys(logs)
                _logs = logs[wl]
                for idx in 1:length(_logs[:core])
                    core_log = _logs[:core][idx]
                    if core_log.category == :add_thunk && core_log.kind == :start &&
                       _logs[:taskfuncnames][idx] == "unsafe_free!"
                        push!(free_tids, _logs[:id][idx].thunk_id::Int)
                    end
                end
            end
            @test !isempty(free_tids)
            for tid in free_tids
                @test !isempty(taskdeps_for_task(logs, tid))
            end
        end
    end
end

@testset "Dagger.synchronize" begin
    # `sync=false` is only allowed to genuinely defer on the flat,
    # non-uniform-execution path -- `hierarchical=true` (the default) and
    # MPI/SPMD both force a synchronous trailing drain regardless (see the
    # N.B. in `spawn_datadeps`). Every test below is written against that
    # flat path explicitly.

    @testset "Pipelining across regions" begin
        # Three consecutive regions, none synchronized in between: region N+1's
        # planning must see region N's writes without an intervening flush,
        # and a single trailing `synchronize()` must produce the same result
        # as three fully-synchronous regions would.
        A = rand(64)
        B = zeros(64)
        C = zeros(64)
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn copyto!(Out(B), In(A))
        end
        @test !Dagger.issynchronized()
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn (x -> x .+= 1)(InOut(B))
        end
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn copyto!(Out(C), In(B))
        end
        @test !Dagger.issynchronized()
        Dagger.synchronize()
        @test Dagger.issynchronized()
        @test C == A .+ 1
    end

    @testset "Error locality" begin
        # A throwing task in region 1, with regions 2 and 3 already queued
        # before anyone observes region 1's failure: the eventual report
        # names region 1 specifically (not region 2 or 3, which are perfectly
        # healthy), its backtrace contains region 1's `spawn_datadeps` call
        # site, and -- once the failure has been discovered but deliberately
        # not "handled" (`check_errors=false`) -- the context refuses to plan
        # further work.
        good1 = rand(4)
        good2 = zeros(4)
        region1_line = (@__LINE__) + 1
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn error("region 1 boom")
        end
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn identity(In(good1))
        end
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn copyto!(Out(good2), In(good1))
        end

        # Force discovery without "handling" it (`check_errors=false`): the
        # drain completes (nothing is left in flight, buffers are freed), but
        # the poison stays in place because nobody has actually been shown
        # the error yet -- see `_do_synchronize!`'s docstring.
        Dagger.synchronize(; check_errors=false)
        # Still considered "not synchronized": the drain itself completed
        # (nothing is left in flight, buffers are freed), but `issynchronized`
        # also requires no *unreported* error, and this one deliberately
        # wasn't reported (`check_errors=false`).
        @test !Dagger.issynchronized()

        # A *new* region must now be refused outright.
        threw_poisoned = false
        local poisoned_err
        try
            Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                Dagger.@spawn identity(In(good1))
            end
        catch err
            threw_poisoned = true
            poisoned_err = err
        end
        @test threw_poisoned
        @test poisoned_err isa Dagger.DataDepsPoisonedError
        @test poisoned_err.region == 1
        @test poisoned_err.bt !== nothing
        @test any(frame -> frame.line == region1_line, poisoned_err.bt)

        # Now actually observe it: the region-1 failure is reported, named,
        # and the poison clears so work can proceed again.
        threw_reported = false
        local reported_err
        try
            Dagger.synchronize()
        catch err
            threw_reported = true
            reported_err = err
        end
        @test threw_reported
        @test reported_err isa Dagger.DataDepsRegionError
        @test reported_err.region == 1
        @test occursin("region 1 boom", sprint(showerror, Dagger.Sch.unwrap_nested_exception(reported_err)))
        @test reported_err.bt !== nothing
        @test any(frame -> frame.line == region1_line, reported_err.bt)

        # Poison cleared: planning works again.
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn identity(In(good1))
        end
        Dagger.synchronize()
        @test Dagger.issynchronized()
    end

    @testset "Task-exit drain" begin
        # A task runs a failing async region and returns without
        # synchronizing -- the error must surface (loudly, via `@error`,
        # since there's nobody left to rethrow into) rather than vanish.
        mktemp() do path, io
            t = redirect_stderr(io) do
                inner = Threads.@spawn begin
                    D = rand(4)
                    Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                        Dagger.@spawn error("task-exit-drain boom")
                    end
                    nothing
                end
                wait(inner)
                # Give the watcher a chance to notice `inner` is done and drain.
                for _ in 1:200
                    sleep(0.02)
                end
                inner
            end
            flush(io)
            captured = read(path, String)
            @test occursin("task-exit-drain boom", captured)
        end
    end

    @testset "Finalizer backstop" begin
        # Drop the task (and its context) reference entirely without ever
        # waiting on it, force GC, and confirm the finalizer reports loudly
        # rather than the process hanging or the failure vanishing silently.
        function make_orphan()
            t = Threads.@spawn begin
                D = rand(4)
                Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                    Dagger.@spawn error("finalizer boom")
                end
                nothing
            end
            wait(t)
            return nothing
        end
        mktemp() do path, io
            redirect_stderr(io) do
                make_orphan()
                GC.gc(true)
                GC.gc(true)
                sleep(0.5)
            end
            flush(io)
            captured = read(path, String)
            # Either the task-exit watcher or the finalizer backstop (whichever
            # gets there first) reports; both are acceptable, the point is that
            # *something* reports and the process doesn't hang.
            @test occursin("finalizer boom", captured) ||
                  occursin("garbage-collected with unresolved work", captured)
        end
    end

    @testset "Multi-task isolation" begin
        # Two tasks, independent async pipelines, disjoint data:
        # `synchronize_task!(t)` on one must not disturb the other.
        A1, B1 = rand(16), zeros(16)
        A2, B2 = rand(16), zeros(16)
        queued1 = Base.Event()
        release1 = Base.Event()
        t1 = Threads.@spawn begin
            Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                Dagger.@spawn copyto!(Out(B1), In(A1))
            end
            notify(queued1)
            wait(release1)
        end
        wait(queued1)
        @test !istaskdone(t1)

        # A completely independent pipeline on *this* task must not be
        # affected by, or drain, t1's context.
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn copyto!(Out(B2), In(A2))
        end
        Dagger.synchronize()
        @test B2 == A2
        @test !istaskdone(t1) # t1 untouched by our own bare `synchronize`

        Dagger.synchronize_task!(t1)
        notify(release1)
        wait(t1)
        @test B1 == A1

        # Shared argument: `synchronize(A)` must not drain a *different*
        # task's context even when they happen to reference the same data;
        # `synchronize_all!(A)` must drain every registered context.
        Ashared = rand(16)
        Bshared_other = zeros(16)
        Bshared_here = zeros(16)
        queued2 = Base.Event()
        release2 = Base.Event()
        t2 = Threads.@spawn begin
            Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                Dagger.@spawn copyto!(Out(Bshared_other), In(Ashared))
            end
            notify(queued2)
            wait(release2)
        end
        wait(queued2)
        @test !istaskdone(t2)

        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn copyto!(Out(Bshared_here), In(Ashared))
        end
        Dagger.synchronize(Ashared)
        @test Bshared_here == Ashared
        @test !istaskdone(t2) # a same-named-argument sync on *our* task doesn't touch t2

        Dagger.synchronize_all!(Ashared)
        notify(release2)
        wait(t2)
        @test Bshared_other == Ashared
    end

    @testset "inflight_limit backpressure" begin
        # A tiny watermark forces `apply_inflight_backpressure!` to actually
        # wait mid-planning; correctness (not just that it doesn't hang)
        # is what's being checked here.
        old_limit = Dagger.DATADEPS_INFLIGHT_LIMIT[]
        Dagger.DATADEPS_INFLIGHT_LIMIT[] = 2
        try
            arrs = [zeros(8) for _ in 1:8]
            src = rand(8)
            for arr in arrs
                Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                    Dagger.@spawn copyto!(Out(arr), In(src))
                end
            end
            Dagger.synchronize()
            @test all(arr -> arr == src, arrs)
        finally
            Dagger.DATADEPS_INFLIGHT_LIMIT[] = old_limit
        end
    end

    @testset "Cross-region per-tile syncdeps" begin
        # Two disjoint tiles, each written by region A and read by region B
        # (a *separate*, unsynchronized `spawn_datadeps` call). Since `state`
        # persists across the region boundary, region B's syncdeps must be
        # per-tile: the reader of tile 1 depends on the writer of tile 1
        # only, not on the writer of tile 2 as well. A whole-region barrier
        # could not distinguish this -- it would simply make *everything* in
        # B depend on *everything* in A, and a bug that regressed to
        # per-region granularity would still produce the right answer (the
        # ordering would just be needlessly serialized), so this checks the
        # logged dependency edges directly rather than only the result.
        T1 = zeros(4)
        T2 = zeros(4)
        local tA1, tA2, tB1, tB2
        logs = with_logs() do
            Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                tA1 = Dagger.@spawn do_nothing(Out(T1))
                tA2 = Dagger.@spawn do_nothing(Out(T2))
            end
            Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                tB1 = Dagger.@spawn do_nothing(In(T1))
                tB2 = Dagger.@spawn do_nothing(In(T2))
            end
            Dagger.synchronize() # flush both regions before logs are fetched
        end
        tid_A1, tid_A2, tid_B1, tid_B2 = task_id.([tA1, tA2, tB1, tB2])
        deps_B1 = taskdeps_for_task(logs, tid_B1)
        deps_B2 = taskdeps_for_task(logs, tid_B2)
        @test tid_A1 in deps_B1
        @test !(tid_A2 in deps_B1)
        @test tid_A2 in deps_B2
        @test !(tid_A1 in deps_B2)
    end

    @testset "Write-back elision across regions" begin
        # Same argument, used by two consecutive regions on a remote worker.
        # Synchronous (`sync=true`) regions write back to origin at the end
        # of *each* region and then copy to the worker again at the start of
        # the next, so this touches `move!` twice. Deferred (`sync=false`)
        # regions share one persisted `state`: region 2 sees the worker
        # already holds the current replica (`arg_current`) and never emits
        # a copy-to at all, so this exercises `move!` only *once* for the
        # (deferred) final write-back. A correct final result doesn't prove
        # this happened -- either path produces the same numbers -- so this
        # asserts the move-task *count* dropped, which is the mechanism that
        # actually makes cross-region pipelining cheaper.
        if nprocs() >= 2
            count_moves(logs) = count(
                logs[w][:core][idx].category == :add_thunk &&
                logs[w][:core][idx].kind == :start &&
                logs[w][:taskfuncnames][idx] == "move!"
                for w in keys(logs) for idx in 1:length(logs[w][:core])
            )

            w = workers()[1]
            A_sync = rand(8)
            logs_sync = with_logs() do
                Dagger.spawn_datadeps(; hierarchical=false) do
                    Dagger.@spawn scope=Dagger.scope(worker=w) do_nothing(InOut(A_sync))
                end
                Dagger.spawn_datadeps(; hierarchical=false) do
                    Dagger.@spawn scope=Dagger.scope(worker=w) do_nothing(InOut(A_sync))
                end
            end
            moves_sync = count_moves(logs_sync)

            A_async = rand(8)
            logs_async = with_logs() do
                Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                    Dagger.@spawn scope=Dagger.scope(worker=w) do_nothing(InOut(A_async))
                end
                Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                    Dagger.@spawn scope=Dagger.scope(worker=w) do_nothing(InOut(A_async))
                end
                Dagger.synchronize()
            end
            moves_async = count_moves(logs_async)

            @test moves_async < moves_sync
        end
    end

    @testset "Deferred free racing an in-flight reader" begin
        # The Phase 1 free-syncdeps hole (`gather_free_syncdeps!` emitting an
        # empty syncdep set for a buffer still in use) was masked by the old
        # region barrier: every task had already retired by the time the
        # free loop ran. Under deferral the free for a buffer region 1
        # touched can be computed (and submitted, with syncdeps) *before*
        # region 1's own reader task has necessarily finished -- a real race
        # the barrier used to hide. Run with the debug-mode invariant
        # (`DATADEPS_ASSERT_FREE_SYNCDEPS`) on, which independently re-derives
        # each free's required syncdeps via a linear scan + `will_alias` and
        # `@assert`s the fast path didn't omit one; this would fail loudly
        # (as a task exception surfaced through `synchronize()`) if the hole
        # were still reachable here.
        old_assert = Dagger.DATADEPS_ASSERT_FREE_SYNCDEPS[]
        Dagger.DATADEPS_ASSERT_FREE_SYNCDEPS[] = true
        try
            if nprocs() >= 2
                w = workers()[1]
                A = rand(4, 4)
                v1 = view(A, 1:2, :)
                v2 = view(A, 3:4, :)
                # Two views sharing one parent buffer (per the existing
                # "buffer underlying shared views" test above), but now each
                # view's writer is in its *own*, separately-synchronized
                # region -- so the shared parent's free (computed once both
                # regions' writes are known, via `flush_pending_frees!`) must
                # still find both writers' syncdeps correctly, even though
                # they were queued by two different `spawn_datadeps` calls.
                Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                    Dagger.@spawn scope=Dagger.scope(worker=w) mut_V!(Out(v1))
                end
                Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                    Dagger.@spawn scope=Dagger.scope(worker=w) mut_V!(Out(v2))
                end
                Dagger.synchronize()
                @test all(==(1), A)
            end
        finally
            Dagger.DATADEPS_ASSERT_FREE_SYNCDEPS[] = old_assert
        end
    end

    @testset "Slot reuse plumbing under deferred release" begin
        # `retiring_slots` (context.jl) exists because a region's
        # `SlotReuseRegion` can no longer be released the moment its
        # *planning* finishes: the copy/free tasks that actually touch a
        # checked-out slot can now outlive the region under `sync=false`,
        # and releasing early would let a concurrently-planning later region
        # take a slot those tasks are still writing into. This exercises
        # that plumbing directly against the slot cache.
        #
        # N.B. End-to-end slot reuse (`retain_reusable_slots!`/`reusable_slot`)
        # is currently wired into the *hierarchical* scheduling path only --
        # the flat path this phase's deferral lives on has never called it,
        # independent of this phase -- and hierarchical forces `sync=true`
        # here. So there is not yet a full user-level async program where
        # slot reuse and deferred frees interact; this test covers the
        # mechanism (`retiring_slots` + `release_slot_reuse_region!`) that
        # will matter the moment either gap closes (slot reuse reaching the
        # flat path, or hierarchical planning outliving a region in a later
        # phase).
        data = Dagger.tochunk(rand(4))
        slot = Dagger.tochunk(rand(4))
        space = Dagger.memory_space(data)
        key = (Dagger._identity_hash(data), space)
        try
            region = Dagger.SlotReuseRegion(Set{UInt}([Dagger._identity_hash(data)]))
            Dagger.retain_slot!(region, data, space, slot)

            # Checked out: a fresh region asking for the same key is refused.
            probe() = Dagger.SlotReuseRegion(Set{UInt}([Dagger._identity_hash(data)]))
            @test Dagger.slot_cache_take!(probe(), data, space) === nothing

            ddctx = Dagger.get_context!()
            push!(ddctx.retiring_slots, region)
            # Still not released: nothing has drained it yet.
            @test Dagger.slot_cache_take!(probe(), data, space) === nothing

            Dagger.synchronize()

            # A full drain must have released it: `slot_cache_take!` now
            # succeeds (and immediately re-checks it out).
            @test Dagger.slot_cache_take!(probe(), data, space) === slot
        finally
            Dagger.empty_slot_cache!()
        end
    end

    @testset "Plain @spawn interleaved between async regions" begin
        # A plain (non-Datadeps) `Dagger.@spawn` between two async regions,
        # over the *same* argument. Interop boundary hooks (making a plain
        # task automatically sync against tracked data) are future work
        # (Phase 6); today this only works because the plain task is on the
        # *same* underlying array object and Dagger's normal scheduler
        # dependency tracking (via the shared `DTask`/value handle) still
        # applies -- this documents that today's behavior is correct for
        # that case, not that interop hooks exist yet.
        A = zeros(4)
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn (x -> x .+= 1)(InOut(A))
        end
        Dagger.synchronize() # `A`'s write-back must land before the plain task below touches it directly
        t = Dagger.@spawn (x -> x .+= 1)(A)
        fetch(t)
        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
            Dagger.@spawn (x -> x .+= 1)(InOut(A))
        end
        Dagger.synchronize()
        @test A == fill(3.0, 4)
    end
end
