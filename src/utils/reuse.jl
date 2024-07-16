struct ReusableCache{T,Tnull}
    cache::Vector{T}
    used::Vector{Bool}
    null::Tnull
    sized::Bool
    function ReusableCache(T, null, N::Integer; sized::Bool=false)
        @assert !Base.datatype_pointerfree(T) "ReusableCache is only useful for non-pointerfree types (got $T)"
        #cache = [T() for _ in 1:N]
        cache = Vector{T}(undef, N)
        used = zeros(Bool, N)
        return new{T,typeof(null)}(cache, used, null, sized)
    end
end
function maybetake!(cache::ReusableCache{T}, len=nothing) where T
    for idx in 1:length(cache.used)
        cache.used[idx] && continue
        if cache.sized && isassigned(cache.cache, idx) && length(cache.cache[idx]) != len
            @dagdebug nothing :reuse "Skipping length $(length(cache.cache[idx])) (want length $len) @ $idx"
            continue
        end
        cache.used[idx] = true
        if !isassigned(cache.cache, idx)
            if cache.sized
                @dagdebug nothing :reuse "Allocating length $len @ $idx"
                cache.cache[idx] = alloc!(T, len)
            else
                cache.cache[idx] = alloc!(T)
            end
            # Initialize newly allocated object with null value
            unset!(cache.cache[idx], cache.null)
        end
        return (idx, cache.cache[idx])
    end
    return nothing
end
function putback!(cache::ReusableCache{T}, idx::Integer) where T
    # Check bounds and throw error for invalid indices
    if !(1 <= idx <= length(cache.used))
        throw(BoundsError(cache, idx))
    end

    # Reset the cached object to null values before marking as available
    if isassigned(cache.cache, idx)
        unset!(cache.cache[idx], cache.null)
    end
    cache.used[idx] = false
end
function take_or_alloc!(f::Function, cache::ReusableCache{T}, len=nothing; no_alloc::Bool=false) where T
    idx_value = maybetake!(cache, len)
    if idx_value !== nothing
        idx, value = idx_value
        try
            return f(value)
        finally
            unset!(value, cache.null)
            putback!(cache, idx)
        end
    else
        if no_alloc
            error("No more entries available in cache for type $T")
        end
        return f(T())
    end
end
function maybe_take_or_alloc!(f::Function, cache::ReusableCache{T}, value::Union{T,Nothing}, len=nothing; no_alloc::Bool=false) where T
    if value !== nothing
        return f(value)
    else
        return take_or_alloc!(f, cache, len; no_alloc=no_alloc)
    end
end

alloc!(::Type{V}, n::Integer) where V<:Vector = V(undef, n)
alloc!(::Type{D}) where {D<:Dict} = D()
alloc!(::Type{S}) where {S<:Set} = S()
alloc!(::Type{T}) where T = T()

unset!(v::Vector, null) = fill!(v, null)
# FIXME: Inefficient to use these
unset!(d::Dict, _) = empty!(d)
unset!(s::Set, _) = empty!(s)

macro take_or_alloc!(cache, T, var, ex)
    @gensym idx_value idx
    quote
        $idx_value = $maybetake!($(esc(cache)))
        if $idx_value !== nothing
            $idx, $(esc(var)) = $idx_value
            try
                $(esc(ex))
            finally
                $unset!($(esc(var)), $(esc(cache)).null)
                $putback!($(esc(cache)), $idx)
            end
        else
            #=let=# $(esc(var)) = $(esc(T))()
                $(esc(ex))
            #end
        end
    end
end
macro take_or_alloc!(cache, T, len, var, ex)
    @gensym idx_value idx
    quote
        $idx_value = $maybetake!($(esc(cache)), $(esc(len)))
        if $idx_value !== nothing
            $idx, $(esc(var)) = $idx_value
            try
                $(esc(ex))
            finally
                $unset!($(esc(var)), $(esc(cache)).null)
                $putback!($(esc(cache)), $idx)
            end
        else
            #=let=# $(esc(var)) = $(esc(T))()
                $(esc(ex))
            #end
        end
    end
end
# TODO: const causes issues with Revise
macro reusable(name, T, null, N, var, ex)
    cache_name = Symbol("__$(name)_reuse_cache")
    if !hasproperty(__module__, cache_name)
        __module__.eval(:(#=const=# $cache_name = $TaskLocalValue{$ReusableCache{$T}}(()->$ReusableCache($T, $null, $N))))
    end
    quote
        @take_or_alloc! $(esc(cache_name))[] $T $(esc(var)) $(esc(ex))
    end
end
macro reusable(name, T, null, N, len, var, ex)
    cache_name = Symbol("__$(name)_reuse_cache")
    if !hasproperty(__module__, cache_name)
        __module__.eval(:(#=const=# $cache_name = $TaskLocalValue{$ReusableCache{$T}}(()->$ReusableCache($T, $null, $N; sized=true))))
    end
    quote
        @take_or_alloc! $(esc(cache_name))[] $T $(esc(len)) $(esc(var)) $(esc(ex))
    end
end

# FIXME: Provide ReusableObject{T} interface
# FIXME: Allow objects to be GC'd (if lost via throw/unexpected control flow) (provide optional warning mode on finalization)
# FIXME: Add take/replace interface
# FIXME: Add function annotation for multiple reuse points

#= FIXME: UniquingCache
struct UniquingCache{K,V}
    cache::Dict{WeakRef,WeakRef}
    function UniquingCache(K, V)
        return new(Dict{K,V}())
    end
end
=#

mutable struct ReusableNode{T}
    value::T
    next::Union{ReusableNode{T},Nothing}
end
mutable struct ReusableLinkedList{T} <: AbstractVector{T}
    head::Union{ReusableNode{T},Nothing}
    tail::Union{ReusableNode{T},Nothing}
    free_nodes::ReusableNode{T}
    null::T
    maxlen::Int
    function ReusableLinkedList{T}(null, N) where T
        free_root = ReusableNode{T}(null, nothing)
        for _ in 1:N
            free_node = ReusableNode{T}(null, nothing)
            free_node.next = free_root
            free_root = free_node
        end
        return new{T}(nothing, nothing, free_root, null, N)
    end
end
Base.eltype(list::ReusableLinkedList{T}) where T = T
function Base.getindex(list::ReusableLinkedList{T}, idx::Integer) where T
    checkbounds(list, idx)
    node = list.head
    for _ in 1:(idx-1)
        node === nothing && throw(BoundsError(list, idx))
        node = node.next
    end
    node === nothing && throw(BoundsError(list, idx))
    return node.value
end
function Base.setindex!(list::ReusableLinkedList{T}, value::T, idx::Integer) where T
    checkbounds(list, idx)
    node = list.head
    for _ in 1:(idx-1)
        node === nothing && throw(BoundsError(list, idx))
        node = node.next
    end
    node === nothing && throw(BoundsError(list, idx))
    node.value = value
    return value
end
function Base.push!(list::ReusableLinkedList{T}, value) where T
    value_conv = convert(T, value)
    node = list.free_nodes
    if node.next === nothing
        # FIXME: Optionally allocate extras
        throw(ArgumentError("No more entries available in cache for type $T"))
    end
    list.free_nodes = node.next
    node.value = value_conv
    node.next = nothing
    if list.head === nothing
        list.head = list.tail = node
    else
        list.tail.next = node
        list.tail = node
    end
    return list
end
function Base.pushfirst!(list::ReusableLinkedList{T}, value) where T
    value_conv = convert(T, value)
    node = list.free_nodes
    if node.next === nothing
        # FIXME: Optionally allocate extras
        throw(ArgumentError("No more entries available in cache for type $T"))
    end
    list.free_nodes = node.next
    node.value = value_conv
    node.next = list.head
    list.head = node
    if list.tail === nothing
        list.tail = node
    end
    return list
end
function Base.pop!(list::ReusableLinkedList{T}) where T
    if list.head === nothing
        throw(ArgumentError("list must be non-empty"))
    end
    prev = node = list.head
    while node.next !== nothing
        prev = node
        node = node.next
    end
    if prev !== node
        list.tail = prev
    else
        list.head = list.tail = nothing
    end
    prev.next = nothing
    node.next = list.free_nodes
    list.free_nodes = node
    value = node.value
    node.value = list.null
    return value
end
function Base.popfirst!(list::ReusableLinkedList{T}) where T
    if list.head === nothing
        throw(ArgumentError("list must be non-empty"))
    end
    node = list.head
    list.head = node.next
    if list.head === nothing
        list.tail = nothing
    end
    node.next = list.free_nodes
    list.free_nodes = node
    value = node.value
    node.value = list.null
    return value
end
Base.size(list::ReusableLinkedList{T}) where T = (length(list),)
function Base.length(list::ReusableLinkedList{T}) where T
    node = list.head
    if node === nothing
        return 0
    end
    len = 1
    while node.next !== nothing
        len += 1
        node = node.next
    end
    return len
end
function Base.iterate(list::ReusableLinkedList{T}) where T
    node = list.head
    if node === nothing
        return nothing
    end
    return (node.value, node)
end
function Base.iterate(list::ReusableLinkedList{T}, state::Union{Nothing,ReusableNode{T}}) where T
    if state === nothing
        return nothing
    end
    node = state.next
    if node === nothing
        return nothing
    end
    return (node.value, node)
end
function Base.in(list::ReusableLinkedList{T}, value::T) where T
    node = list.head
    while node !== nothing
        if node.value == value
            return true
        end
    end
    return false
end
function Base.findfirst(f::Function, list::ReusableLinkedList)
    node = list.head
    idx = 1
    while node !== nothing
        if f(node.value)
            return idx
        end
        node = node.next
        idx += 1
    end
    return nothing
end
Base.sizehint!(list::ReusableLinkedList, len::Integer) = nothing
function Base.empty!(list::ReusableLinkedList{T}) where T
    if list.tail !== nothing
        fill!(list, list.null)
        list.tail.next = list.free_nodes
        list.free_nodes = list.head
        list.head = list.tail = nothing
    end
    return list
end
function Base.fill!(list::ReusableLinkedList{T}, value::T) where T
    node = list.head
    while node !== nothing
        node.value = value
        node = node.next
    end
    return list
end
function Base.resize!(list::ReusableLinkedList, N::Integer)
    while length(list) < N
        push!(list, list.null)
    end
    while length(list) > N
        pop!(list)
    end
    return list
end
function Base.deleteat!(list::ReusableLinkedList, idx::Integer)
    checkbounds(list, idx)
    if idx == 1
        deleted = list.head
        list.head = list.head.next
        deleted.next = list.free_nodes
        list.free_nodes = deleted
        deleted.value = list.null
        return list
    end
    node = list.head
    for _ in 1:(idx-2)
        if node === nothing
            throw(BoundsError(idx))
        end
        node = node.next
    end
    if idx == length(list)
        list.tail = node
    end
    deleted = node.next
    node.next = deleted.next
    deleted.next = list.free_nodes
    list.free_nodes = deleted
    deleted.value = list.null
    return list
end
function Base.map!(f, list_out::ReusableLinkedList{T}, list_in::ReusableLinkedList{V}; N=length(list_in)) where {T,V}
    node_out = list_out.head
    node_in = list_in.head
    ctr = 0
    while node_in !== nothing
        node_out.value = f(node_in.value)
        node_in = node_in.next
        node_out = node_out.next
        ctr += 1
        if ctr >= N
            break
        end
    end
    return list_out
end
function Base.copyto!(list_out::ReusableLinkedList{T}, list_in::ReusableLinkedList{T}) where T
    Base.map!(identity, list_out, list_in)
end

struct ReusableSet{T} <: AbstractSet{T}
    list::ReusableLinkedList{T}
end
function ReusableSet(T, null, N)
    return ReusableSet{T}(ReusableLinkedList{T}(null, N))
end
function Base.push!(set::ReusableSet{T}, value::T) where T
    if !(value in set)
        push!(set.list, value)
    end
    return set
end
function Base.pop!(set::ReusableSet{T}, value) where T
    value_conv = convert(T, value)
    idx = findfirst(==(value_conv), set)
    if idx === nothing
        throw(KeyError(value_conv))
    end
    deleteat!(set, idx)
    return value
end
Base.length(set::ReusableSet) = length(set.list)
function Base.iterate(set::ReusableSet)
    return iterate(set.list)
end
function Base.iterate(set::ReusableSet, state)
    return iterate(set.list, state)
end
function Base.empty!(set::ReusableSet{T}) where T
    empty!(set.list)
    return set
end

struct ReusableDict{K,V} <: AbstractDict{K,V}
    keys::ReusableLinkedList{K}
    values::ReusableLinkedList{V}
end
function ReusableDict{K,V}(null_key, null_value, N::Integer) where {K,V}
    keys = ReusableLinkedList{K}(null_key, N)
    values = ReusableLinkedList{V}(null_value, N)
    return ReusableDict{K,V}(keys, values)
end
function Base.getindex(dict::ReusableDict{K,V}, key) where {K,V}
    key_conv = convert(K, key)
    idx = findfirst(==(key_conv), dict.keys)
    if idx === nothing
        throw(KeyError(key_conv))
    end
    return dict.values[idx]
end
function Base.setindex!(dict::ReusableDict{K,V}, value, key) where {K,V}
    key_conv = convert(K, key)
    value_conv = convert(V, value)
    idx = findfirst(==(key_conv), dict.keys)
    if idx === nothing
        push!(dict.keys, key_conv)
        push!(dict.values, value_conv)
    else
        dict.values[idx] = value_conv
    end
    return value
end
function Base.delete!(dict::ReusableDict{K,V}, key) where {K,V}
    key_conv = convert(K, key)
    idx = findfirst(==(key_conv), dict.keys)
    if idx === nothing
        throw(KeyError(key_conv))
    end
    deleteat!(dict.keys, idx)
    deleteat!(dict.values, idx)
    return dict
end
function Base.haskey(dict::ReusableDict{K,V}, key) where {K,V}
    key_conv = convert(K, key)
    return key_conv in dict.keys
end
function Base.iterate(dict::ReusableDict)
    key = dict.keys.head
    if key === nothing
        return nothing
    end
    value = dict.values.head
    return (key.value => value.value, (key, value))
end
Base.length(dict::ReusableDict) = length(dict.keys)
function Base.iterate(dict::ReusableDict, state)
    if state === nothing
        return nothing
    end
    key, value = state
    key = key.next
    if key === nothing
        return nothing
    end
    value = value.next
    return (key.value => value.value, (key, value))
end
Base.keys(dict::ReusableDict) = dict.keys
Base.values(dict::ReusableDict) = dict.values
function Base.empty!(dict::ReusableDict{K,V}) where {K,V}
    empty!(dict.keys)
    empty!(dict.values)
    return dict
end

macro reusable_vector(name, T, null, N)
    vec_name = Symbol("__$(name)_TLV_ReusableVector")
    if !hasproperty(__module__, vec_name)
        __module__.eval(:(#=const=# $vec_name = $TaskLocalValue{$Vector{$T}}(()->$Vector{$T}())))
    end
    return :($(esc(vec_name))[])
end
macro reusable_dict(name, K, V, null_key, null_value, N)
    dict_name = Symbol("__$(name)_TLV_ReusableDict")
    if !hasproperty(__module__, dict_name)
        __module__.eval(:(#=const=# $dict_name = $TaskLocalValue{$Dict{$K,$V}}(()->$Dict{$K,$V}())))
    end
    return :($(esc(dict_name))[])
end

mutable struct ReusableTaskCache
    tasks::Vector{Task}
    chans::Vector{Channel{Any}}
    ready::Vector{Threads.Atomic{Bool}}
    setup_f::Function
    N::Int
    init::Bool
    function ReusableTaskCache(N::Integer)
        tasks = Vector{Task}(undef, N)
        chans = Vector{Channel{Any}}(undef, N)
        ready = [Threads.Atomic{Bool}(true) for _ in 1:N]
        for idx in 1:N
            chans[idx] = Channel{Any}(1)
            chan, r = chans[idx], ready[idx]
            tasks[idx] = @task reusable_task_loop(chan, r)
        end
        cache = new(tasks, chans, ready, t->nothing, N, false)
        finalizer(cache) do cache
            # Ask tasks to shut down
            for idx in 1:N
                Threads.atomic_xchg!(cache.ready[idx], false)
                close(cache.chans[idx])
            end
        end
        return cache
    end
end
function reusable_task_cache_init!(setup_f::Function, cache::ReusableTaskCache)
    cache.init && return
    cache.setup_f = setup_f
    for idx in 1:cache.N
        task = cache.tasks[idx]
        setup_f(task)
        schedule(task)
        Sch.errormonitor_tracked("reusable_task_$idx", task)
    end
    cache.init = true
    return
end
function reusable_task_loop(chan::Channel{Any}, ready::Threads.Atomic{Bool})
    r = rand(1:128)
    while true
        f = try
            take!(chan)
        catch
            if !isopen(chan)
                return
            else
                rethrow()
            end
        end
        try
            @invokelatest f()
        catch err
            @error "[$r] Error in reusable task" exception=(err, catch_backtrace())
        end
        Threads.atomic_xchg!(ready, true)
    end
end
function (cache::ReusableTaskCache)(f, name::String)
    idx = findfirst(getindex, cache.ready)
    if idx !== nothing
        @assert Threads.atomic_xchg!(cache.ready[idx], false)
        put!(cache.chans[idx], f)
        Sch.errormonitor_tracked_set!(name, cache.tasks[idx])
        return cache.tasks[idx]
    else
        t = @task try
            @invokelatest f()
        catch err
            @error "[$r] Error in non-reusable task" exception=(err, catch_backtrace())
        end
        cache.setup_f(t)
        schedule(t)
        Sch.errormonitor_tracked(name, t)
        return t
    end
    return
end

macro reusable_tasks(name, N, setup_ex, task_name, task_ex)
    cache_name = Symbol("__$(name)_TLV_ReusableTaskCache")
    if !hasproperty(__module__, cache_name)
        __module__.eval(:(#=const=# $cache_name = $TaskLocalValue{$ReusableTaskCache}(()->$ReusableTaskCache($N))))
    end
    return esc(quote
        $reusable_task_cache_init!($setup_ex, $cache_name[])
        $cache_name[]($task_ex, $task_name)
    end)
end
