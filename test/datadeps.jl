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

function test_move_rewrap_aliasing(obj, dest_space)
    src_space = Dagger.memory_space(obj)
    from_proc = first(Dagger.processors(src_space))
    to_proc = first(Dagger.processors(dest_space))

    # move_rewrap like generate_slot!
    dummy_backing = Dagger.tochunk(Dagger.AliasedObjectCacheStore())
    cache = Dagger.AliasedObjectCache(dest_space, dummy_backing)

    dest_obj_chunk = Dagger.move_rewrap(cache, from_proc, to_proc, src_space, dest_space, obj)

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
    spaces = [Dagger.CPURAMMemorySpace(w) for w in 1:3]

    for (w_src, w_dst) in [(1, 2), (2, 1), (2, 3)]
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
    Dagger.enable_logging!(;taskdeps=true, taskargs=true, timeline=true)
    try
        f()
        return Dagger.fetch_logs!()
    finally
        Dagger.disable_logging!()
    end
end
task_id(t::Dagger.DTask) = lock(Dagger.Sch.EAGER_ID_MAP) do id_map
    id_map[t.uid]
end
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

@testset "Custom Schedulers" begin
    @testset "DummyErrorScheduler" begin
        # Test that our custom scheduler is actually called by Datadeps
        @test_throws DummySchedulerError Dagger.spawn_datadeps(; scheduler=DummyErrorScheduler()) do
            Dagger.@spawn 1 + 1
        end
    end

    @testset "RoundRobinScheduler" begin
        # RoundRobinScheduler is the default and tested extensively above,
        # but let's explicitly test passing it as an argument
        A = rand(10)
        result = Dagger.spawn_datadeps(; scheduler=Dagger.RoundRobinScheduler()) do
            Dagger.@spawn sum(In(A))
        end
        @test fetch(result) ≈ sum(A)
    end

    @testset "NaiveScheduler" begin
        # NaiveScheduler uses estimate_task_costs from the main scheduler
        @test_skip begin
            A = rand(10)
            result = Dagger.spawn_datadeps(; scheduler=Dagger.NaiveScheduler()) do
                Dagger.@spawn sum(In(A))
            end
            fetch(result) ≈ sum(A)
        end
    end

    @testset "UltraScheduler" begin
        # UltraScheduler is currently broken (references undefined exec_spaces)
        @test_skip begin
            A = rand(10)
            result = Dagger.spawn_datadeps(; scheduler=Dagger.UltraScheduler()) do
                Dagger.@spawn sum(In(A))
            end
            fetch(result) ≈ sum(A)
        end
    end
end
