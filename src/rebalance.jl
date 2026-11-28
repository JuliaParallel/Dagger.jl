# Moving work between ranks between steps: a deterministic, sticky assignment of chunks (partitions) to ranks from measured costs,
# and the moves that carry it out.  The application supplies one cost per chunk and a single place per step where a re-plan may happen.

"""
    rebalance_plan(owners, costs, nranks; tolerance=1.05, max_moves=length(owners), speeds=nothing, move_cost=0.0, horizon=1) -> Vector{Pair{Int,Int}}

The moves (`chunk index => new rank`, ranks counted from 0) that bring the per-rank sum of `costs` toward its mean, given the current
`owners` (rank of each chunk). Pure and deterministic: every rank that calls it with the same arguments gets the same plan, which is
what a collective needs. Greedy and sticky: while the most loaded rank exceeds `tolerance` times the mean, the chunk of that rank whose
cost is closest to the amount that would make the two ranks finish together (half the gap at equal speeds) moves to the least loaded rank,
provided it narrows the gap; at most `max_moves` single moves, a chunk that
would move twice moves once to its final rank, and a chunk whose rank does not change is not listed. Nothing moves when the load is
already within `tolerance`.

`speeds` (one positive, finite number per rank, only the ratios matter) says that ranks differ in speed. `costs` are then work (for example
operation counts, or times measured on a reference rank), a rank needs `work / speed`, and the plan equalises that time instead of the work:
a rank of half the speed ends with about half the work. Without `speeds` all ranks are assumed equally fast, so costs measured as
times on the rank that ran them are inflated on a slow rank and chunks can drift back and forth; pass work and `speeds`, or costs that are
already independent of the rank's speed.

`move_cost` (the time a move of each chunk takes: one number for all chunks, or one per chunk; default 0) and `horizon` (the number of steps the
new placement is expected to last; default 1) make a move pay for itself: a move is made only if the time it takes off the slower of the two
ranks, times `horizon`, exceeds its cost. With the default cost of 0 every move that narrows the gap is made. The plan stops at the first move that
does not pay. [`rebalance_loads`](@ref) gives the per-rank times a plan predicts.

To move groups of chunks that must stay together, give one entry of `owners` and `costs` per group (see [`rebalance`](@ref)).
"""
function rebalance_plan(owners::AbstractVector{<:Integer}, costs::AbstractVector{<:Real}, nranks::Integer;
                        tolerance::Real=1.05, max_moves::Integer=length(owners),
                        speeds::Union{Nothing,AbstractVector{<:Real}}=nothing,
                        move_cost::Union{Real,AbstractVector{<:Real}}=0.0, horizon::Real=1)
    length(owners) == length(costs) || throw(ArgumentError("`owners` and `costs` must have the same length"))
    nranks >= 1 || throw(ArgumentError("nranks must be positive"))
    owner = Int[o for o in owners]
    all(o -> 0 <= o < nranks, owner) || throw(ArgumentError("owners must be ranks in 0:$(nranks - 1)"))
    spd = _rebalance_speeds(speeds, nranks)
    mcost = move_cost isa Real ? fill(Float64(move_cost), length(owner)) : Float64[x for x in move_cost]
    length(mcost) == length(owner) || throw(ArgumentError("`move_cost` needs one entry per chunk"))
    all(x -> isfinite(x) && x >= 0, mcost) || throw(ArgumentError("move costs must be finite and non-negative"))
    (isfinite(horizon) && horizon > 0) || throw(ArgumentError("horizon must be finite and positive"))
    c = Float64[x for x in costs]
    all(x -> isfinite(x) && x >= 0, c) || throw(ArgumentError("costs must be finite and non-negative"))
    load = zeros(Float64, nranks)
    for i in eachindex(owner)
        load[owner[i] + 1] += c[i]
    end
    total = sum(load)
    total > 0 || return Pair{Int,Int}[]
    load ./= spd                       # from here on `load` is the time a rank needs: work / speed
    mean_load = total / sum(spd)       # the time every rank would need with a perfect split
    moved = 0
    while moved < max_moves
        hi = argmax(load); lo = argmin(load)
        load[hi] <= tolerance * mean_load && break
        gap = load[hi] - load[lo]
        ideal = gap / (1 / spd[hi] + 1 / spd[lo])   # the work that makes both ranks finish together (gap / 2 at equal speeds)
        best = 0; bestcost = 0.0
        for i in eachindex(owner)
            owner[i] == hi - 1 || continue
            ci = c[i]
            (ci > 0 && ci < gap * spd[lo]) || continue
            if best == 0 || abs(ci - ideal) < abs(bestcost - ideal)
                best = i; bestcost = ci
            end
        end
        best == 0 && break
        # a move must pay for itself: the time it takes off the slowest of the two ranks, over `horizon` steps, against its cost
        gain = load[hi] - max(load[hi] - bestcost / spd[hi], load[lo] + bestcost / spd[lo])
        horizon * gain > mcost[best] || break
        owner[best] = lo - 1
        load[hi] -= bestcost / spd[hi]; load[lo] += bestcost / spd[lo]
        moved += 1
    end
    return Pair{Int,Int}[i => owner[i] for i in eachindex(owner) if owner[i] != owners[i]]
end

function _rebalance_speeds(speeds, nranks::Integer)
    speeds === nothing && return ones(Float64, nranks)
    spd = Float64[x for x in speeds]
    length(spd) == nranks || throw(ArgumentError("`speeds` needs one entry per rank"))
    all(x -> isfinite(x) && x > 0, spd) || throw(ArgumentError("speeds must be finite and positive"))
    return spd
end

"""
    rebalance_loads(owners, costs, nranks; speeds=nothing, plan=Pair{Int,Int}[]) -> Vector{Float64}

The per-rank time (`work / speed`, summed over the chunks a rank owns) before the moves of `plan` (the default, an empty plan) or after them:
the loads [`rebalance_plan`](@ref) predicts, to compare with measured ones.
"""
function rebalance_loads(owners::AbstractVector{<:Integer}, costs::AbstractVector{<:Real}, nranks::Integer;
                         speeds::Union{Nothing,AbstractVector{<:Real}}=nothing, plan=Pair{Int,Int}[])
    length(owners) == length(costs) || throw(ArgumentError("`owners` and `costs` must have the same length"))
    nranks >= 1 || throw(ArgumentError("nranks must be positive"))
    spd = _rebalance_speeds(speeds, nranks)
    owner = Int[o for o in owners]
    all(o -> 0 <= o < nranks, owner) || throw(ArgumentError("owners must be ranks in 0:$(nranks - 1)"))
    for (i, r) in plan
        0 <= r < nranks || throw(ArgumentError("the plan moves a chunk to a rank outside 0:$(nranks - 1)"))
        owner[i] = r
    end
    load = zeros(Float64, nranks)
    for i in eachindex(owner)
        load[owner[i] + 1] += costs[i]
    end
    return load ./ spd
end

"""
    rebalance(chunks, costs; tolerance=1.05, max_moves=length(chunks), speeds=nothing, move_cost=0.0, horizon=1) -> (new_chunks, moves)

(MPI) Collective: every rank calls it at the same point with the same `chunks` and the same `costs` (one non-negative number per chunk, e.g. the
measured time of the chunk's tasks in the last step, made equal on all ranks, e.g. by an allreduce). Computes [`rebalance_plan`](@ref) (which
explains `tolerance`, `max_moves`, `speeds`, `move_cost` and `horizon`) and
moves each chunk that has to change owner by running `identity` on the new rank: the result is a new chunk owned by that rank, with the data copied
over. Returns the chunk vector to use from now on
(unchanged entries are the same objects) and the list of moves.

An element of `chunks` may be a `Chunk`, a `DTask` that produces one (what `DArray.chunks`
holds before the tasks finish), or a tuple or vector of those: a group of chunks that must stay together (for example the buffers of one partition).
The members of a group must have the same owner; the group has one cost, moves as a unit so that all its members land on the same rank, and
comes back as a vector in the same position. `moves` then lists the index of the group.

The old chunks are not freed by the call: their memory is released by the garbage collector
once nothing refers to them. The application must schedule the tasks bound to a chunk on its new owner, e.g.
`Dagger.scope(mpi_rank=Dagger.owner_rank(chunk))`, and drop its references to the old chunks so their memory can be reclaimed.
`costs`, `speeds`, `move_cost` and `horizon` must be equal on all ranks: this is checked on every call and throws an `ArgumentError` on every rank.
The graph must be the one of the whole job. Other accelerations throw.
"""
rebalance(chunks::AbstractVector, costs::AbstractVector; kwargs...) = rebalance(current_acceleration(), chunks, costs; kwargs...)
rebalance(::Acceleration, chunks::AbstractVector, costs::AbstractVector; kwargs...) =
    throw(ArgumentError("`Dagger.rebalance` moves chunks between MPI ranks: run it under `Dagger.accelerate!(:mpi)`"))

"""
    owner_rank(chunk) -> Int

The rank that owns `chunk` under MPI (its data lives there; tasks bound to it should run there), or 0 for chunks of other accelerations.
"""
owner_rank(::Any) = 0
