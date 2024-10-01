using MPI

MPIAcceleration() = MPIAcceleration(MPI.COMM_WORLD)

#default_processor(accel::MPIAcceleration) = MPIOSProc(accel.comm)

function aliasing(accel::MPIAcceleration, x::Chunk, T)
    @assert x.handle isa MPIRef "MPIRef expected"
    #print("[$(MPI.Comm_rank(x.handle.comm))] Hit probable hang on aliasing \n")
    if x.handle.rank == MPI.Comm_rank(accel.comm)
        ainfo = aliasing(x, T)
        MPI.bcast(ainfo, x.handle.rank, x.handle.comm)
    else
        ainfo = MPI.bcast(nothing, x.handle.rank, x.handle.comm)
    end
    #print("[$(MPI.Comm_rank(x.handle.comm))] Left aliasing hang \n")
    return ainfo
end
default_processor(accel::MPIAcceleration) = MPIOSProc(accel.comm, 0)
default_processor(accel::MPIAcceleration, x) = MPIOSProc(accel.comm, 0)
default_processor(accel::MPIAcceleration, x::Chunk) = MPIOSProc(x.handle.comm, x.handle.rank)
default_processor(accel::MPIAcceleration, x::Function) = MPIOSProc(accel.comm, MPI.Comm_rank(accel.comm))
default_processor(accel::MPIAcceleration, T::Type) = MPIOSProc(accel.comm, MPI.Comm_rank(accel.comm))

#TODO: Add a lock
const MPIClusterProcChildren = Dict{MPI.Comm, Set{Processor}}()

struct MPIClusterProc <: Processor
    comm::MPI.Comm
    function MPIClusterProc(comm::MPI.Comm)
        populate_children(comm)
        return new(comm)
    end
end

Sch.init_proc(state, proc::MPIClusterProc, log_sink) = Sch.init_proc(state, MPIOSProc(proc.comm), log_sink)

MPIClusterProc() = MPIClusterProc(MPI.COMM_WORLD)

function populate_children(comm::MPI.Comm)
    children = get_processors(OSProc())
    MPIClusterProcChildren[comm] = children
end

struct MPIOSProc <: Processor
    comm::MPI.Comm
    rank::Int
end

function MPIOSProc(comm::MPI.Comm)
    rank = MPI.Comm_rank(comm)
    return MPIOSProc(comm, rank)
end

function MPIOSProc()
    return MPIOSProc(MPI.COMM_WORLD)
end
#Sch.init_proc(state, proc::MPIOSProc, log_sink) = Sch.init_proc(state, OSProc(), log_sink)

function memory_spaces(proc::MPIOSProc)
    children = get_processors(proc)
    spaces = Set{MemorySpace}()
    for proc in children
        for space in memory_spaces(proc)
            push!(spaces, space)
        end
    end
    return spaces
end

struct MPIProcessScope <: AbstractScope 
    comm::MPI.Comm
    rank::Int
end

Base.isless(::MPIProcessScope, ::MPIProcessScope) = false
Base.isless(::MPIProcessScope, ::NodeScope) = true
Base.isless(::MPIProcessScope, ::UnionScope) = true
Base.isless(::MPIProcessScope, ::TaintScope) = true
Base.isless(::MPIProcessScope, ::AnyScope) = true
constrain(x::MPIProcessScope, y::MPIProcessScope) =
    x == y ? y : InvalidScope(x, y)
constrain(x::NodeScope, y::MPIProcessScope) =
    x == y.parent ? y : InvalidScope(x, y)

Base.isless(::ExactScope, ::MPIProcessScope) = true
constrain(x::MPIProcessScope, y::ExactScope) =
    x == y.parent ? y : InvalidScope(x, y)

function enclosing_scope(proc::MPIOSProc)
    return MPIProcessScope(proc.comm, proc.rank)
end

struct MPIProcessor{P<:Processor} <: Processor
    innerProc::P
    comm::MPI.Comm
    rank::Int
end

Dagger.iscompatible_func(::MPIProcessor, opts, ::Any) = true
Dagger.iscompatible_arg(::MPIProcessor, opts, ::Any) = true

default_enabled(proc::MPIProcessor) = default_enabled(proc.innerProc)

root_worker_id(proc::MPIProcessor) = myid()
root_worker_id(proc::MPIOSProc) = myid()
root_worker_id(proc::MPIClusterProc) = myid()

get_parent(proc::MPIClusterProc) = proc
get_parent(proc::MPIOSProc) = MPIClusterProc(proc.comm)
get_parent(proc::MPIProcessor) = MPIOSProc(proc.comm, proc.rank)    

function get_processors(mosProc::MPIOSProc)
    populate_children(mosProc.comm)
    children = MPIClusterProcChildren[mosProc.comm]
    mpiProcs = Set{Processor}()
    for proc in children
        push!(mpiProcs, MPIProcessor(proc, mosProc.comm, mosProc.rank))
    end
    return mpiProcs
end

#TODO: non-uniform ranking through MPI groups
#TODO: use a lazy iterator
function get_processors(proc::MPIClusterProc)
    children = Set{Processor}()
    for i in 0:(MPI.Comm_size(proc.comm) -1)
        for innerProc in MPIClusterProcChildren[proc.comm]
            push!(children, MPIProcessor(innerProc, proc.comm, i))
        end
    end
    return children
end

struct MPIMemorySpace{S<:MemorySpace} <: MemorySpace
    innerSpace::S
    comm::MPI.Comm
    rank::Int
end

default_processor(space::MPIMemorySpace) = MPIOSProc(space.comm, space.rank)
default_memory_space(accel::MPIAcceleration) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, 0)

default_memory_space(accel::MPIAcceleration, x) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, 0)
default_memory_space(accel::MPIAcceleration, x::Chunk) = MPIMemorySpace(CPURAMMemorySpace(myid()), x.handle.comm, x.handle.rank)
default_memory_space(accel::MPIAcceleration, x::Function) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, MPI.Comm_rank(accel.comm))
default_memory_space(accel::MPIAcceleration, T::Type) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, MPI.Comm_rank(accel.comm))

function memory_spaces(proc::MPIProcessor)
    rawMemSpace = Set{MemorySpace}()
    for innerSpace in memory_spaces(proc.innerProc)
        push!(rawMemSpace, MPIMemorySpace(innerSpace, proc.comm, proc.rank))
    end
    return rawMemSpace
end

root_worker_id(mem_space::MPIMemorySpace) = myid()

function processors(memSpace::MPIMemorySpace)
    rawProc = Set{Processor}()
    for innerProc in processors(memSpace.innerSpace)
        push!(rawProc, MPIProcessor(innerProc, memSpace.comm, memSpace.rank))
    end
    return rawProc
end

const MPIidcount = Threads.Atomic{Int}(1)

mutable struct MPIRef
    comm::MPI.Comm
    rank::Int
    size::Int
    innerRef::Union{DRef, Nothing}
    id::Int
end

move(from_proc::Processor, to_proc::Processor, x::MPIRef) = move(from_proc, to_proc, poolget(x.innerRef))

function affinity(x::MPIRef)
    if x.innerRef === nothing
        return MPIOSProc(x.comm, x.rank)=>0
    else
        return MPIOSProc(x.comm, x.rank)=>x.innerRef.size
    end
end

#TODO: partitioned scheduling with comm bifurcation
function tochunk_pset(x, space::MPIMemorySpace; device=nothing, kwargs...)
    local_rank = MPI.Comm_rank(space.comm)
    if local_rank != space.rank
        return MPIRef(space.comm, space.rank, 0, nothing, Threads.atomic_add!(MPIidcount, 1))
    else
        return MPIRef(space.comm, space.rank, sizeof(x), poolset(x; device, kwargs...), Threads.atomic_add!(MPIidcount, 1))
    end
end

function recv_yield(src, tag, comm)
    #@dagdebug nothing :mpi "[$(MPI.Comm_rank(comm))][$tag] Hit probable hang on recv \n"
    while true
        (got, msg, stat) = MPI.Improbe(src, tag, comm, MPI.Status)
        if got
            count = MPI.Get_count(stat, UInt8)
            buf = Array{UInt8}(undef, count)
            req = MPI.Imrecv!(MPI.Buffer(buf), msg)
            while true
                finish = MPI.Test(req)
                if finish
                    value = MPI.deserialize(buf)
                    rnk = MPI.Comm_rank(comm)
                    #@dagdebug nothing :mpi "[$rnk][$tag] Left recv hang \n"
                    return value
                end
                yield()
            end
        end
        yield()
    end
end
#discuss this with julian
WeakChunk(c::Chunk{T,H}) where {T,H<:MPIRef} = WeakChunk(c.handle.rank, c.handle.id, WeakRef(c))

function send_yield(value, comm, dest, tag) 
    #@dagdebug nothing :mpi "[$(MPI.Comm_rank(comm))][$tag] Hit probable hang while sending \n"
    req = MPI.isend(value, comm; dest, tag)
    while true
        finish = MPI.Test(req)
        if finish
            #@dagdebug nothing :mpi "[$(MPI.Comm_rank(comm))][$tag] Left send hang \n"
            return
        end
        yield()
    end
end

function poolget(ref::MPIRef)
    @assert ref.rank == MPI.Comm_rank(ref.comm) "MPIRef rank mismatch"
    poolget(ref.innerRef)
end

function move!(dep_mod, dst::MPIMemorySpace, src::MPIMemorySpace, dstarg::Chunk, srcarg::Chunk)
    @assert dstarg.handle isa MPIRef && srcarg.handle isa MPIRef "MPIRef expected"
    @assert dstarg.handle.comm == srcarg.handle.comm "MPIRef comm mismatch" 
    @assert dstarg.handle.rank == dst.rank && srcarg.handle.rank == src.rank "MPIRef rank mismatch"
    local_rank = MPI.Comm_rank(srcarg.handle.comm)
    h = abs(Base.unsafe_trunc(Int32, hash(dep_mod, hash(srcarg.handle.id, hash(dstarg.handle.id, UInt(0))))))
    @dagdebug nothing :mpi "[$local_rank][$h] Moving from  $(src.rank)  to  $(dst.rank)\n"
    if src.rank == dst.rank == local_rank
        move!(dep_mod, dst.innerSpace, src.innerSpace, dstarg, srcarg)
    else
        if local_rank == src.rank
            send_yield(poolget(srcarg.handle), dst.comm, dst.rank, h)
        end
        if local_rank == dst.rank
            val = recv_yield(src.rank, h, src.comm)
            move!(dep_mod, dst.innerSpace, src.innerSpace, poolget(dstarg.handle), val)
        end
    end
    @dagdebug nothing :mpi "[$local_rank][$h] Finished moving from  $(src.rank)  to  $(dst.rank) successfuly\n"
end

move(::MPIOSProc, ::MPIProcessor, x::Union{Function,Type}) = x
move(::MPIOSProc, ::MPIProcessor, x::Chunk{<:Union{Function,Type}}) = poolget(x.handle)

#TODO: out of place MPI move
function move(src::MPIOSProc, dst::MPIProcessor, x::Chunk)
    @assert src.comm == dst.comm "Multi comm move not supported"
    if Sch.SCHED_MOVE[]
        if dst.rank == MPI.Comm_rank(dst.comm) 
            return poolget(x.handle)
        end
    else 
        @assert src.rank == MPI.Comm_rank(src.comm) "Unwraping not permited"
        @assert src.rank == x.handle.rank == dst.rank 
        return poolget(x.handle)
    end
end

#TODO: Discuss this with julian

move(src::Processor, dst::MPIProcessor, x::Chunk) = error("MPI move not supported")
move(to_proc::MPIProcessor, chunk::Chunk) =
    move(chunk.processor, to_proc, chunk)
move(to_proc::Processor, d::MPIRef) =
    move(MPIOSProc(d.rank), to_proc, d)
move(to_proc::MPIProcessor, x) =
    move(MPIOSProc(), to_proc, x)

move(::MPIProcessor, ::MPIProcessor, x::Union{Function,Type}) = x
move(::MPIProcessor, ::MPIProcessor, x::Chunk{<:Union{Function,Type}}) = poolget(x.handle)

function move(src::MPIProcessor, dst::MPIProcessor, x::Chunk)
    @assert src.rank == dst.rank "Unwraping not permited"
    if Sch.SCHED_MOVE[]
        if dst.rank == MPI.Comm_rank(dst.comm) 
            return poolget(x.handle)
        end
    else
        @assert src.rank == MPI.Comm_rank(src.comm) "Unwraping not permited"
        @assert src.rank == x.handle.rank == dst.rank
        return poolget(x.handle)
    end
end

#FIXME:try to think of a better move! scheme
function execute!(proc::MPIProcessor, f, args...; kwargs...)
    local_rank = MPI.Comm_rank(proc.comm)
	tid = sch_handle().thunk_id.id 
    if local_rank == proc.rank || f === move!
        res = execute!(proc.innerProc, f, args...; kwargs...)
    else
		res = nothing
    end
    return tochunk(res, proc, memory_space(proc))
end

accelerate!(::Val{:mpi}) = accelerate!(MPIAcceleration())

function initialize_acceleration!(a::MPIAcceleration)
    if !MPI.Initialized()
        MPI.Init(;threadlevel=:multiple)
    end
    ctx = Dagger.Sch.eager_context()
    sz = MPI.Comm_size(a.comm)
    for i in 0:(sz-1)
        push!(ctx.procs, MPIOSProc(a.comm, i))
    end
    unique!(ctx.procs)
end

accel_matches_proc(accel::MPIAcceleration, proc::MPIOSProc) = true
accel_matches_proc(accel::MPIAcceleration, proc::MPIClusterProc) = true
accel_matches_proc(accel::MPIAcceleration, proc::MPIProcessor) = true
accel_matches_proc(accel::MPIAcceleration, proc) = false

distribute(A::AbstractArray{T,N}, dist::Blocks{N}, root::Int; comm::MPI.Comm=MPI.COMM_WORLD) where {T,N} =
    distribute(A::AbstractArray{T,N}, dist; comm, root) 
distribute(A::AbstractArray, root::Int; comm::MPI.Comm=MPI.COMM_WORLD) = distribute(A, AutoBlocks(), root; comm)
distribute(A::AbstractArray, ::AutoBlocks, root::Int; comm::MPI.Comm=MPI.COMM_WORLD) = distribute(A, auto_blocks(A), root; comm)
function distribute(x::AbstractArray{T,N}, n::NTuple{N}, root::Int; comm::MPI.Comm=MPI.COMM_WORLD) where {T,N}
    p = map((d, dn)->ceil(Int, d / dn), size(x), n)
    distribute(x, Blocks(p), root; comm)
end
distribute(x::AbstractVector, n::Int, root::Int; comm::MPI.Comm=MPI.COMM_WORLD) = distribute(x, (n,), root; comm)
distribute(x::AbstractVector, n::Vector{<:Integer}, root::Int; comm::MPI.Comm) =
    distribute(x, DomainBlocks((1,), (cumsum(n),)); comm, root=0)


distribute(A::AbstractArray{T,N}, dist::Blocks{N}, comm::MPI.Comm; root::Int=0) where {T,N} =
    distribute(A::AbstractArray{T,N}, dist; comm, root) 
distribute(A::AbstractArray, comm::MPI.Comm; root::Int=0) = distribute(A, AutoBlocks(), comm; root)
distribute(A::AbstractArray, ::AutoBlocks, comm::MPI.Comm; root::Int=0) = distribute(A, auto_blocks(A), comm; root)
function distribute(x::AbstractArray{T,N}, n::NTuple{N}, comm::MPI.Comm; root::Int=0) where {T,N}
    p = map((d, dn)->ceil(Int, d / dn), size(x), n)
    distribute(x, Blocks(p), comm; root)
end
distribute(x::AbstractVector, n::Int, comm::MPI.Comm; root::Int=0) = distribute(x, (n,), comm; root)
distribute(x::AbstractVector, n::Vector{<:Integer}, comm::MPI.Comm; root::Int=0) =
    distribute(x, DomainBlocks((1,), (cumsum(n),)), comm; root)

function distribute(x::AbstractArray{T,N}, dist::Blocks{N}, ::MPIAcceleration) where {T,N}
    return distribute(x, dist; comm=MPI.COMM_WORLD, root=0)
end

distribute(A::Nothing, dist::Blocks{N}) where N = distribute(nothing, dist; comm=MPI.COMM_WORLD, root=0)

function distribute(A::Union{AbstractArray{T,N}, Nothing}, dist::Blocks{N}; comm::MPI.Comm, root::Int) where {T,N}
    rnk = MPI.Comm_rank(comm)
    isroot = rnk == root
    csz = MPI.Comm_size(comm)
    d = MPI.bcast(domain(A), comm, root=root)
    sd = partition(dist, d)
    type = MPI.bcast(eltype(A), comm, root=root)
    # TODO: Make better load balancing
    cs = Array{Any}(undef, size(sd))
    if prod(size(sd)) < csz
        @warn "Number of chunks is less than number of ranks, performance may be suboptimal"
    end
    if isroot
        dst = 0
        for (idx, part) in enumerate(sd)
            if dst != root
                h = abs(Base.unsafe_trunc(Int32, hash(part, UInt(0))))
                send_yield(A[part], comm, dst, h)
                data = nothing
            else
                data = A[part]
            end
            p = MPIOSProc(comm, dst)
            s = first(memory_spaces(p))
            cs[idx] = tochunk(data, p, s)
            dst += 1
            if dst == csz
                dst = 0
            end
        end
        println("Sent all chunks")
    else
        dst = 0 
        for (idx, part) in enumerate(sd)
            data = nothing
            if rnk == dst
                h = abs(Base.unsafe_trunc(Int32, hash(part, UInt(0))))
                data = recv_yield(root, h, comm)
            end
            p = MPIOSProc(comm, dst)
            s = first(memory_spaces(p))
            cs[idx] = tochunk(data, p, s)
            dst += 1
            if dst == csz
                dst = 0
            end
            println("Received chunk $idx")
            #MPI.Scatterv!(nothing, data, comm; root=root)
        end
    end
    MPI.Barrier(comm)
    return Dagger.DArray(type, d, sd, cs, dist)
end

function Base.collect(x::Dagger.DMatrix{T};
        comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=true) where {T} 
    csz = MPI.Comm_size(comm)
    rank = MPI.Comm_rank(comm)
    sd = x.subdomains 
    if !acrossranks
        if isempty(x.chunks)
            return Array{eltype(d)}(undef, size(x)...)
        end
        localarr = []
        localparts = []
        curpart = rank + 1
        while curpart <= length(x.chunks)
            print("[$rank] Collecting chunk $curpart\n")
            push!(localarr, fetch(x.chunks[curpart]))
            push!(localparts, sd[curpart])
            curpart += csz
        end
        return localarr, localparts
    else
        reqs = Vector{MPI.Request}()
        dst = 0
        if root === nothing
            data = Matrix{T}(undef, size(x))
            localarr, localparts = collect(x; acrossranks=false)
            for (idx, part) in enumerate(localparts)
                for i in 0:(csz - 1)
                    if i != rank
                        h = abs(Base.unsafe_trunc(Int32, hash(part, UInt(0))))
                        print("[$rank] Sent chunk $idx to rank $i with tag $h \n")
                        push!(reqs, MPI.isend(localarr[idx], comm; dest = i, tag = h))
                    else
                        data[part.indexes...] = localarr[idx]
                    end
                end
            end
            for (idx, part) in enumerate(sd)
                h = abs(Base.unsafe_trunc(Int32, hash(part, UInt(0))))
                if dst != rank
                    print("[$rank] Waiting for chunk $idx from rank $dst with tag $h\n")
                    data[part.indexes...] = recv_yield(dst, h, comm)
                end
                dst += 1
                if dst == MPI.Comm_size(comm)
                    dst = 0
                end
            end
            MPI.Waitall(reqs)
            return data
        else
            if rank == root
                data = Matrix{T}(undef, size(x))
                for (idx, part) in enumerate(sd)
                    h = abs(Base.unsafe_trunc(Int32, hash(part, UInt(0))))
                    if dst == rank
                        localdata = fetch(x.chunks[idx])
                        data[part.indexes...] = localdata
                    else
                        data[part.indexes...] = recv_yield(dst, h, comm)
                    end
                    dst += 1
                    if dst == MPI.Comm_size(comm)
                        dst = 0
                    end
                end
                return fetch.(data)
            else
                for (idx, part) in enumerate(sd)
                    h = abs(Base.unsafe_trunc(Int32, hash(part, UInt(0))))
                    if rank == dst
                        localdata = fetch(x.chunks[idx])
                        push!(reqs, MPI.isend(localdata, comm; dest = root, tag = h))
                    end
                    dst += 1
                    if dst == MPI.Comm_size(comm)
                        dst = 0
                    end
                end
                MPI.Waitall(reqs)
                return nothing
            end
        end
    end
end
