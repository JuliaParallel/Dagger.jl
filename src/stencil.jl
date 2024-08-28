# FIXME: Remove me
const Read = In
const Write = Out
const ReadWrite = InOut

function load_neighbor_edge(arr, dim, dir, neigh_dist)
    if dir == -1
        start_idx = CartesianIndex(ntuple(i -> i == dim ? (lastindex(arr, i) - neigh_dist + 1) : firstindex(arr, i), ndims(arr)))
        stop_idx = CartesianIndex(ntuple(i -> i == dim ? lastindex(arr, i) : lastindex(arr, i), ndims(arr)))
    elseif dir == 1
        start_idx = CartesianIndex(ntuple(i -> i == dim ? firstindex(arr, i) : firstindex(arr, i), ndims(arr)))
        stop_idx = CartesianIndex(ntuple(i -> i == dim ? (firstindex(arr, i) + neigh_dist - 1) : lastindex(arr, i), ndims(arr)))
    end
    return move(thunk_processor(), arr[start_idx:stop_idx])
end
function load_neighbor_corner(arr, corner_side, neigh_dist)
    start_idx = CartesianIndex(ntuple(i -> corner_side[i] == 0 ? (lastindex(arr, i) - neigh_dist + 1) : firstindex(arr, i), ndims(arr)))
    stop_idx = CartesianIndex(ntuple(i -> corner_side[i] == 0 ? lastindex(arr, i) : (firstindex(arr, i) + neigh_dist - 1), ndims(arr)))
    return move(thunk_processor(), arr[start_idx:stop_idx])
end
function select_neighborhood_chunks(chunks, idx, neigh_dist, boundary)
    @assert neigh_dist isa Integer && neigh_dist > 0 "Neighborhood distance must be an Integer greater than 0"

    # FIXME: Depends on neigh_dist and chunk size
    chunk_dist = 1
    # Get the center
    accesses = Any[chunks[idx]]

    # Get the edges
    for dim in 1:ndims(chunks)
        for dir in (-1, +1)
            new_idx = idx + CartesianIndex(ntuple(i -> i == dim ? dir*chunk_dist : 0, ndims(chunks)))
            if is_past_boundary(size(chunks), new_idx)
                if boundary_has_transition(boundary)
                    new_idx = boundary_transition(boundary, new_idx, size(chunks))
                else
                    new_idx = idx
                end
                chunk = chunks[new_idx]
                push!(accesses, Dagger.@spawn load_boundary_edge(boundary, chunk, dim, dir, neigh_dist))
            else
                chunk = chunks[new_idx]
                push!(accesses, Dagger.@spawn load_neighbor_edge(chunk, dim, dir, neigh_dist))
            end
        end
    end

    # Get the corners
    for corner_num in 1:(2^ndims(chunks))
        corner_side = CartesianIndex(reverse(ntuple(ndims(chunks)) do i
            ((corner_num-1) >> (((ndims(chunks) - i) + 1) - 1)) & 1
        end))
        corner_new_idx = CartesianIndex(ntuple(ndims(chunks)) do i
            corner_shift = iszero(corner_side[i]) ? -1 : 1
            return idx[i] + corner_shift
        end)
        if is_past_boundary(size(chunks), corner_new_idx)
            if boundary_has_transition(boundary)
                corner_new_idx = boundary_transition(boundary, corner_new_idx, size(chunks))
            else
                corner_new_idx = idx
            end
            chunk = chunks[corner_new_idx]
            push!(accesses, Dagger.@spawn load_boundary_corner(boundary, chunk, corner_side, neigh_dist))
        else
            chunk = chunks[corner_new_idx]
            push!(accesses, Dagger.@spawn load_neighbor_corner(chunk, corner_side, neigh_dist))
        end
    end

    @assert length(accesses) == 1+2*ndims(chunks)+2^ndims(chunks) "Accesses mismatch: $(length(accesses))"
    return accesses
end
function build_halo(neigh_dist, boundary, center, all_neighbors...)
    N = ndims(center)
    edges = all_neighbors[1:(2*N)]
    corners = all_neighbors[((2^N)+1):end]
    @assert length(edges) == 2*N && length(corners) == 2^N "Halo mismatch: edges=$(length(edges)) corners=$(length(corners))"
    return HaloArray(center, (edges...,), (corners...,), ntuple(_->neigh_dist, N))
end
function load_neighborhood(arr::HaloArray{T,N}, idx) where {T,N}
    @assert all(arr.halo_width .== arr.halo_width[1])
    neigh_dist = arr.halo_width[1]
    start_idx = idx - CartesianIndex(ntuple(_->neigh_dist, ndims(arr)))
    stop_idx = idx + CartesianIndex(ntuple(_->neigh_dist, ndims(arr)))
    return @view arr[start_idx:stop_idx]
end
function inner_stencil!(f, output, read_vars)
    processor = thunk_processor()
    inner_stencil_proc!(processor, f, output, read_vars)
end
# Non-KA (for CPUs)
function inner_stencil_proc!(::ThreadProc, f, output, read_vars)
    for idx in CartesianIndices(output)
        f(idx, output, read_vars)
    end
    return
end

is_past_boundary(size, idx) = any(ntuple(i -> idx[i] < 1 || idx[i] > size[i], length(size)))

struct Wrap end
boundary_has_transition(::Wrap) = true
boundary_transition(::Wrap, idx, size) =
    CartesianIndex(ntuple(i -> mod1(idx[i], size[i]), length(size)))
load_boundary_edge(::Wrap, arr, dim, dir, neigh_dist) = load_neighbor_edge(arr, dim, dir, neigh_dist)
load_boundary_corner(::Wrap, arr, corner_side, neigh_dist) = load_neighbor_corner(arr, corner_side, neigh_dist)

struct Pad{T}
    padval::T
end
boundary_has_transition(::Pad) = false
function load_boundary_edge(pad::Pad, arr, dim, dir, neigh_dist)
    if dir == -1
        start_idx = CartesianIndex(ntuple(i -> i == dim ? (lastindex(arr, i) - neigh_dist + 1) : firstindex(arr, i), ndims(arr)))
        stop_idx = CartesianIndex(ntuple(i -> i == dim ? lastindex(arr, i) : lastindex(arr, i), ndims(arr)))
    elseif dir == 1
        start_idx = CartesianIndex(ntuple(i -> i == dim ? firstindex(arr, i) : firstindex(arr, i), ndims(arr)))
        stop_idx = CartesianIndex(ntuple(i -> i == dim ? (firstindex(arr, i) + neigh_dist - 1) : lastindex(arr, i), ndims(arr)))
    end
    edge_size = ntuple(i -> length(start_idx[i]:stop_idx[i]), ndims(arr))
    # FIXME: return Fill(pad.padval, edge_size)
    return move(thunk_processor(), fill(pad.padval, edge_size))
end
function load_boundary_corner(pad::Pad, arr, corner_side, neigh_dist)
    start_idx = CartesianIndex(ntuple(i -> corner_side[i] == 0 ? (lastindex(arr, i) - neigh_dist + 1) : firstindex(arr, i), ndims(arr)))
    stop_idx = CartesianIndex(ntuple(i -> corner_side[i] == 0 ? lastindex(arr, i) : (firstindex(arr, i) + neigh_dist - 1), ndims(arr)))
    corner_size = ntuple(i -> length(start_idx[i]:stop_idx[i]), ndims(arr))
    # FIXME: return Fill(pad.padval, corner_size)
    return move(thunk_processor(), fill(pad.padval, corner_size))
end

"""
    @stencil begin body end

Allows the specification of stencil operations within a `spawn_datadeps`
region. The `idx` variable is used to iterate over `range`, which must be a
`DArray`. An example usage may look like:

```julia
import Dagger: @stencil, Wrap

A = zeros(Blocks(3, 3), Int, 9, 9)
A[5, 5] = 1
B = zeros(Blocks(3, 3), Int, 9, 9)
Dagger.@spawn_datadeps() do
    @stencil begin
        # Sum values of all neighbors with self
        A[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        # Decrement all values by 1
        A[idx] -= 1
        # Copy A to B
        B[idx] = A[idx]
    end
end
```

Each expression within an `@stencil` region that performs an in-place indexing
expression like `A[idx] = ...` is transformed into a set of tasks that operate
on each chunk of `A` or any other arrays specified as `A[idx]`, and within each
task, elements of that chunk of `A` can be accessed. Elements of multiple
`DArray`s can be accessed, such as `B[idx]`, so long as `B` has the same size,
shape, and chunk layout as `A`.

Additionally, the `@neighbors` macro can be used to access a neighborhood of
values around `A[idx]`, at a configurable distance (in this case, 1 element
distance) and with various kinds of boundary conditions (in this case, `Wrap()`
specifies wrapping behavior on the boundaries). Neighborhoods are computed with
respect to neighboring chunks as well - if a neighborhood would overflow from
the current chunk into one or more neighboring chunks, values from those
neighboring chunks will be included in the neighborhood.

Note that, while `@stencil` may look like a `for` loop, it does not follow the
same semantics; in particular, an expression within `@stencil` occurs "all at
once" (across all indices) before the next expression occurs. This means that
`A[idx] = sum(@neighbors(A[idx], 1, Wrap()))` will write the sum of
neighbors for all `idx` values into `A[idx]` before `A[idx] -= 1` decrements
the values `A` by 1, and that occurs before any of the values are copied to `B`
in `B[idx] = A[idx]`. Of course, pipelining and other optimizations may still
occur, so long as they respect the sequential nature of `@stencil` (just like
with other operations in `spawn_datadeps`).
"""
macro stencil(orig_ex)
    @assert Meta.isexpr(orig_ex, :block) "Invalid stencil block: $orig_ex"

    # Collect access pattern information
    inners = []
    all_accessed_vars = Set{Symbol}()
    for inner_ex in orig_ex.args
        inner_ex isa LineNumberNode && continue
        @assert @capture(inner_ex, write_ex_ = read_ex_) "Invalid update expression: $inner_ex"
        @assert @capture(write_ex, write_var_[write_idx_]) "Update expression requires a write: $write_ex"
        accessed_vars = Set{Symbol}()
        read_vars = Set{Symbol}()
        neighborhoods = Dict{Symbol, Tuple{Any, Any}}()
        push!(accessed_vars, write_var)
        prewalk(read_ex) do read_inner_ex
            if @capture(read_inner_ex, read_var_[read_idx_]) && read_idx == write_idx
                push!(accessed_vars, read_var)
                push!(read_vars, read_var)
            elseif @capture(read_inner_ex, @neighbors(read_var_[read_idx_], neigh_dist_, boundary_))
                @assert read_idx == write_idx "Neighborhood access must be at the same index as the write: $read_inner_ex"
                push!(accessed_vars, read_var)
                push!(read_vars, read_var)
                neighborhoods[read_var] = (neigh_dist, boundary)
            end
            return read_inner_ex
        end
        union!(all_accessed_vars, accessed_vars)
        push!(inners, (;inner_ex, accessed_vars, write_var, write_idx, read_ex, read_vars, neighborhoods))
    end

    # Codegen update functions
    final_ex = Expr(:block)
    @gensym chunk_idx
    for (;inner_ex, accessed_vars, write_var, write_idx, read_ex, read_vars, neighborhoods) in inners
        # Generate a variable for chunk access
        @gensym chunk_idx

        # Generate function with transformed body
        @gensym inner_vars inner_index_var
        new_inner_ex_body = prewalk(inner_ex) do old_inner_ex
            if @capture(old_inner_ex, read_var_[read_idx_]) && read_idx == write_idx
                # Direct access
                if read_var == write_var
                    return :($write_var[$inner_index_var])
                else
                    return :($inner_vars.$read_var[$inner_index_var])
                end
            elseif @capture(old_inner_ex, @neighbors(read_var_[read_idx_], neigh_dist_, boundary_))
                # Neighborhood access
                return :($load_neighborhood($inner_vars.$read_var, $inner_index_var))
            end
            return old_inner_ex
        end
        new_inner_f = :(($inner_index_var, $write_var, $inner_vars)->$new_inner_ex_body)
        new_inner_ex = quote
            $inner_vars = (;$(read_vars...))
            $inner_stencil!($new_inner_f, $write_var, $inner_vars)
        end
        inner_fn = Expr(:->, Expr(:tuple, Expr(:parameters, write_var, read_vars...)), new_inner_ex)

        # Generate @spawn call with appropriate vars and deps
        deps_ex = Any[]
        if write_var in read_vars
            push!(deps_ex, Expr(:kw, write_var, :($ReadWrite($chunks($write_var)[$chunk_idx]))))
        else
            push!(deps_ex, Expr(:kw, write_var, :($Write($chunks($write_var)[$chunk_idx]))))
        end
        neighbor_copy_all_ex = Expr(:block)
        for read_var in read_vars
            if read_var in keys(neighborhoods)
                # Generate a neighborhood copy operation
                neigh_dist, boundary = neighborhoods[read_var]
                deps_inner_ex = Expr(:block)
                @gensym neighbor_copy_var
                push!(neighbor_copy_all_ex.args, :($neighbor_copy_var = Dagger.@spawn $build_halo($neigh_dist, $boundary, map($Read, $select_neighborhood_chunks($chunks($read_var), $chunk_idx, $neigh_dist, $boundary))...)))
                push!(deps_ex, Expr(:kw, read_var, :($Read($neighbor_copy_var))))
            else
                push!(deps_ex, Expr(:kw, read_var, :($Read($chunks($read_var)[$chunk_idx]))))
            end
        end
        spawn_ex = :(Dagger.@spawn $inner_fn(;$(deps_ex...)))

        # Generate loop
        push!(final_ex.args, quote
            for $chunk_idx in $CartesianIndices($chunks($write_var))
                $neighbor_copy_all_ex
                $spawn_ex
            end
        end)
    end


    return esc(final_ex)
end
