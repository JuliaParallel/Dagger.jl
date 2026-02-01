# FIXME: Remove me
const Read = In
const Write = Out
const ReadWrite = InOut

function validate_neigh_dist(neigh_dist)
    if !(neigh_dist isa Integer) && !(neigh_dist isa Tuple)
        throw(ArgumentError("Neighborhood distance ($neigh_dist) must be an Integer or Tuple"))
    end
    if any(neigh_dist .<= 0)
        throw(ArgumentError("Neighborhood distance ($neigh_dist) must be greater than 0"))
    end
end
function validate_neigh_dist(neigh_dist, size)
    validate_neigh_dist(neigh_dist)
    if any(size .< neigh_dist)
        throw(ArgumentError("Neighborhood distance ($neigh_dist) must not be larger than the chunk size ($size)"))
    end
end

get_neigh_dist(neigh_dist::Integer, i::Int) = neigh_dist
get_neigh_dist(neigh_dist::Tuple, i::Int) = neigh_dist[i]


# Load a halo region from a neighboring chunk
# region_code: N-tuple where each element is -1 (low), 0 (full extent), or +1 (high)
# For dimensions with code 0, we take the full extent of the array
# For dimensions with code -1, we take the last neigh_dist elements (to go to neighbor's low side)
# For dimensions with code +1, we take the first neigh_dist elements (to go to neighbor's high side)
function load_neighbor_region(arr, region_code::NTuple{N,Int}, neigh_dist) where N
    validate_neigh_dist(neigh_dist, size(arr))
    start_idx = CartesianIndex(ntuple(N) do i
        if region_code[i] == -1
            lastindex(arr, i) - get_neigh_dist(neigh_dist, i) + 1
        else
            firstindex(arr, i)
        end
    end)
    stop_idx = CartesianIndex(ntuple(N) do i
        if region_code[i] == +1
            firstindex(arr, i) + get_neigh_dist(neigh_dist, i) - 1
        else
            lastindex(arr, i)
        end
    end)
    # FIXME: Don't collect
    return move(task_processor(), collect(@view arr[start_idx:stop_idx]))
end
function select_neighborhood_chunks(chunks, idx, neigh_dist, boundary)
    validate_neigh_dist(neigh_dist)

    N = ndims(chunks)
    # FIXME: Depends on neigh_dist and chunk size
    chunk_dist = 1

    # Get the center
    accesses = Any[chunks[idx]]

    # Iterate over all 3^N - 1 halo regions (excluding center)
    # Each region is identified by a code tuple where each element is -1, 0, or +1
    for i in 0:(3^N - 1)
        region_code = ntuple(N) do d
            ((i ÷ 3^(d-1)) % 3) - 1  # Maps 0,1,2 -> -1,0,+1
        end
        all(==(0), region_code) && continue  # Skip center

        # Compute the chunk offset for this region
        # For each dimension: -1 means go to previous chunk, +1 means go to next chunk, 0 means same chunk
        chunk_offset = CartesianIndex(ntuple(N) do d
            region_code[d] * chunk_dist
        end)
        new_idx = idx + chunk_offset

        if is_past_boundary(size(chunks), new_idx)
            if boundary_has_transition(boundary)
                new_idx = boundary_transition(boundary, new_idx, size(chunks))
            else
                new_idx = idx
            end
            chunk = chunks[new_idx]
            push!(accesses, Dagger.@spawn load_boundary_region(boundary, chunk, region_code, neigh_dist))
        else
            chunk = chunks[new_idx]
            push!(accesses, Dagger.@spawn load_neighbor_region(chunk, region_code, neigh_dist))
        end
    end

    @assert length(accesses) == 3^N "Accesses mismatch: expected $(3^N), got $(length(accesses))"
    return accesses
end
function build_halo(neigh_dist, boundary, center, all_halos...)
    N = ndims(center)
    expected_halos = 3^N - 1
    @assert length(all_halos) == expected_halos "Halo mismatch: N=$N expected $expected_halos halos, got $(length(all_halos))"
    return HaloArray(center, (all_halos...,), ntuple(i->get_neigh_dist(neigh_dist, i), N))
end
function load_neighborhood(arr::HaloArray{T,N}, idx) where {T,N}
    start_idx = idx - CartesianIndex(ntuple(i->arr.halo_width[i], ndims(arr)))
    stop_idx = idx + CartesianIndex(ntuple(i->arr.halo_width[i], ndims(arr)))
    return @view arr[start_idx:stop_idx]
end
function inner_stencil!(f, output, read_vars)
    processor = task_processor()
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
load_boundary_region(::Wrap, arr, region_code, neigh_dist) = load_neighbor_region(arr, region_code, neigh_dist)

struct Pad{T}
    padval::T
end
boundary_has_transition(::Pad) = false
function load_boundary_region(pad::Pad, arr, region_code::NTuple{N,Int}, neigh_dist) where N
    # Compute the size of this halo region
    # For dimensions with code 0, use full array size
    # For dimensions with code -1 or +1, use neigh_dist
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end
    # FIXME: return Fill(pad.padval, region_size)
    return move(task_processor(), fill(pad.padval, region_size))
end

"""
    @stencil begin body end

Allows the execution of stencil operations within a `spawn_datadeps` region.
The `idx` variable is used to iterate over one or more `DArray`s. An example
usage may look like:

```julia
import Dagger: @stencil, Wrap

A = zeros(Blocks(3, 3), Int, 9, 9)
A[5, 5] = 1
B = zeros(Blocks(3, 3), Int, 9, 9)
Dagger.spawn_datadeps() do
    @stencil begin
        # Increment all values by 1
        A[idx] = A[idx] + 1
        # Sum values of all neighbors with self and write to B
        B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        # Copy B back to A
        A[idx] = B[idx]
    end
end
```

Each expression within an `@stencil` region that performs an in-place indexing
expression like `A[idx] = ...` is transformed into a set of tasks that operate
on each chunk of `A` or any other arrays specified as `A[idx]`; within each
task, elements of that chunk of `A` can be accessed. Elements of multiple
`DArray`s can be accessed, such as `B[idx]`, so long as `B` has the same size,
shape, and chunk layout as `A`.

Additionally, the `@neighbors` macro can be used to access a neighborhood of
values around `A[idx]`, at a configurable distance (in this case, 1 element
distance) and with various kinds of boundary conditions (in this case, `Wrap()`
specifies wrapping behavior on the boundaries). Neighborhoods are computed with
respect to neighboring chunks as well - if a neighborhood would overflow from
the current chunk into a neighboring chunk, values from that neighboring chunk
will be included in the neighborhood.

Note that, while `@stencil` may look like a `for` loop, it does not follow the
same semantics; in particular, an expression within `@stencil` occurs "all at
once" (across all indices) before the next expression occurs. This means that
`A[idx] = A[idx] + 1` increments the values `A` by 1, which occurs before
`B[idx] = sum(@neighbors(A[idx], 1, Wrap()))` writes the sum of neighbors for
all `idx` values into `B[idx]`, and that occurs before any of the values are
copied to `A` in `A[idx] = B[idx]`. Of course, pipelining and other optimizations
may still occur, so long as they respect the sequential nature of `@stencil`
(just like with other operations in `spawn_datadeps`). Due to this behavior,
expressions like `A[idx] = sum(@neighbors(A[idx], 1, Wrap()))` are not valid,
as that would currently cause race conditions and lead to undefined behavior.
"""
macro stencil(orig_ex)
    if !Meta.isexpr(orig_ex, :block)
        throw(ArgumentError("Invalid stencil block: $orig_ex"))
    end

    # Collect access pattern information
    inners = []
    all_accessed_vars = Set{Symbol}()
    for inner_ex in orig_ex.args
        inner_ex isa LineNumberNode && continue
        if !@capture(inner_ex, write_ex_ = read_ex_)
            throw(ArgumentError("Invalid update expression: $inner_ex"))
        end
        if !@capture(write_ex, write_var_[write_idx_])
            throw(ArgumentError("Update expression requires a write: $write_ex"))
        end
        accessed_vars = Set{Symbol}()
        read_vars = Set{Symbol}()
        neighborhoods = Dict{Symbol, Tuple{Any, Any}}()
        push!(accessed_vars, write_var)
        prewalk(read_ex) do read_inner_ex
            if @capture(read_inner_ex, read_var_[read_idx_]) && read_idx == write_idx
                push!(accessed_vars, read_var)
                push!(read_vars, read_var)
            elseif @capture(read_inner_ex, @neighbors(read_var_[read_idx_], neigh_dist_, boundary_))
                if read_idx != write_idx
                    throw(ArgumentError("Neighborhood access must be at the same index as the write: $read_inner_ex"))
                end
                if write_var == read_var
                    throw(ArgumentError("Cannot write to the same variable as the neighborhood access: $read_inner_ex"))
                end
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
                push!(neighbor_copy_all_ex.args, :($neighbor_copy_var = Dagger.@spawn name="stencil_build_halo" $build_halo($neigh_dist, $boundary, map($Read, $select_neighborhood_chunks($chunks($read_var), $chunk_idx, $neigh_dist, $boundary))...)))
                push!(deps_ex, Expr(:kw, read_var, :($Read($neighbor_copy_var))))
            else
                push!(deps_ex, Expr(:kw, read_var, :($Read($chunks($read_var)[$chunk_idx]))))
            end
        end
        spawn_ex = :(Dagger.@spawn name="stencil_inner_fn" $inner_fn(;$(deps_ex...)))

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
