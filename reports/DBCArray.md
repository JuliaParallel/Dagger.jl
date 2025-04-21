# GSoC 2025 Report: Distributed Linear Algebra with Dagger.jl

**Author**: Akhil Akkapelli

**Mentor**: Julian Samaroo, Rabab Alomairy

## Main Goal

The objective of this project is to add distributed linear algebra capabilities to Dagger.jl. This involved implementing operations such as matrix multiplication and factorizations using various data distribution schemes (cyclic, block-cyclic, 2D, 3D) that can run efficiently across multiple devices using the Dagger.jl APIs.

## Steps Toward Implementation

### 1. Background Study and Design

- Study the Dagger.jl documentation to understand its architecture and design principles.
- Explore relevant source code files and become familiar with the internal mechanisms and how different components interact.

### 2. Matrix Distribution Infrastructure

- Develop a system to distribute `DArray` chunks using a block-cyclic layout to specific processor blocks.
- Introduce new constructors and helper functions to ensure that each chunk runs exclusively on its assigned processor.
- Update the scheduling logic to maintain fixed processor scopes during execution.
- Integrate the distribution logic with Dagger's scheduler to manage task dependencies and execute operations correctly across the processor grid.

### 3. Matrix Operations

- Implement fundamental matrix operations such as `Adjoint`, `Transpose`, `*`, `+`, and `MatMul`, ensuring that operations respect processor assignments.
- Add support for essential linear algebra routines like `norm2`, `issymmetric`, `ishermitian`, and factorizations including `lu` (with and without pivoting) and `cholesky`.
- Ensure accurate indexing and chunk-to-processor mapping throughout execution.

### 4. Testing and Performance Evaluation

- Develop a comprehensive test suite to validate the correctness of operations for various matrix shapes, sizes, and distribution patterns.
- Ensure that all operations execute efficiently and correctly across multiple devices, including GPUs.

### 5. Documentation and Examples

- Write detailed documentation with examples demonstrating supported operations and various data layouts.
- Add inline comments within the codebase to clarify the implementation logic and data flow.

# Explicit Processor Mapping of DArray Blocks in Block-Cyclic Manner with `DBCArray`

## Objective

In block-cyclic layouts, block-to-processor assignments are typically handled by the scheduler, which may result in inefficient allocations. The goal here is to explicitly control block allocation by setting a processor "scope" during the block assignment process.

## Approaches

There are two possible approaches:

1. **Integrate directly into `DArray`**: Modify the existing `DArray` structure and core logic to support explicit processor mapping, which requires substantial changes.

2. **Create a new struct `DBCArray`**: Define a new structure extending `DArray`, reusing its features while adding new functions with minimal changes to existing code.

I am currently pursuing the second method as it is modular, less invasive, easier to test and maintain, and can later be integrated into `DArray` if needed.

## Implementation

Development is maintained in my fork of Dagger.jl at [https://github.com/AkhilAkkapelli/Dagger.jl](https://github.com/AkhilAkkapelli/Dagger.jl), with all changes made in the `dbcarray.jl` file under the `src/array` folder. The `DBCArray` implementation incrementally incorporates functionality from various files in the codebase as outlined below. Some functions are not yet implemented/tested and are currently commented out.

### darray.jl

We enhance DArray by introducing a new struct, `DBCArray`, which holds `darray` and `pdomain`. The `pdomain` is an array of `Dagger.Processor` type. Each block in the `darray` is mapped to these processors in a block-cyclic pattern. Below is the `DBCArray` struct definition:

```julia
# Define DBCArray struct, wrapping a DArray and processor domain
mutable struct DBCArray{T,N,B,F} <: ArrayOp{T, N}
    darray::DArray{T,N,B,F}
    pdomain::AbstractArray{Dagger.Processor, N}
    # function DBCArray{T,N,B,F}(domain, subdomains, chunks, partitioning::B, concat::Function, pdomain) where {T,N,B,F}
    #     new{T,N,B,F}(domain, subdomains, chunks, partitioning, concat, pdomain)
    # end
end
```

Other definitions like `DBCVector`, `DBCMatrix` are implemented here similar to `DVector` and `DMatrix`:

```julia
# Type aliases for convenience
const WrappedDBCArray{T,N} = Union{<:DBCArray{T,N}, Transpose{<:DBCArray{T,N}}, Adjoint{<:DBCArray{T,N}}}
const WrappedDBCMatrix{T} = WrappedDBCArray{T,2}
const WrappedDBCVector{T} = WrappedDBCArray{T,1}
const DBCMatrix{T} = DBCArray{T,2}
const DBCVector{T} = DBCArray{T,1}
```

Constructors of `DArray` can be implemented similarly for `DBCArray`. Additionally, a new constructor is defined that takes `darray` and `pdomain` as inputs, and creates a `DBCArray` by assigning blocks to its processors using scope in `@spawn`:

```julia
# DBCArray{T, N}(domain, subdomains, chunks, partitioning, concat=cat, pdomain) where {T,N} = DBCArray(T, domain, subdomains, chunks, partitioning, concat)

# function DArray(T, domain::DArrayDomain{N},
#     subdomains::AbstractArray{DArrayDomain{N}, N},
#     chunks::AbstractArray{<:Any, N}, partitioning::B, concat=cat) where {N,B<:AbstractBlocks{N}}
# DArray{T,N,B,typeof(concat)}(domain, subdomains, chunks, partitioning, concat)
# end

# function DArray(T, domain::DArrayDomain{N},
    #     subdomains::DArrayDomain{N},
    #     chunks::Any, partitioning::B, concat=cat) where {N,B<:AbstractSingleBlocks{N}}
    # _subdomains = Array{DArrayDomain{N}, N}(undef, ntuple(i->1, N)...)
    # _subdomains[1] = subdomains
    # _chunks = Array{Any, N}(undef, ntuple(i->1, N)...)
    # _chunks[1] = chunks
    # DArray{T,N,B,typeof(concat)}(domain, _subdomains, _chunks, partitioning, concat)
# end

# Constructor for DBCArray from DArray and processor domain
function DBCArray(A::DArray{T,N,B,F}, pdomain::AbstractArray{Dagger.Processor, N}) where {T,N,B,F}
    all_procs = collect(Iterators.flatten(Dagger.get_processors(OSProc(w)) for w in procs()))
    missing = filter(p -> p ∉ all_procs, pdomain)
    isempty(missing) || error("Missing processors: $missing")

    Ac = fetch(A.chunks)
    Ac_copy = similar(A.chunks)

    # Assign blocks to processors using ExactScope
    for idx in CartesianIndices(A.chunks)
        proc = pdomain[mod1.(Tuple(idx), size(pdomain))...]
        Ac_copy[idx] = Dagger.@spawn scope=Dagger.ExactScope(proc) identity(Ac[idx])
    end

    A_copy = DArray{T,N,B,F}(A.domain, A.subdomains, Ac_copy, A.partitioning, A.concat)
    return DBCArray{T,N,B,F}(A_copy, pdomain)
end
```

The following utility functions are adapted to support the `DBCArray` type:

```julia
# Delegate various properties to inner DArray
domain(d::DBCArray) = domain(d.darray)
chunks(d::DBCArray) = chunks(d.darray)
domainchunks(d::DBCArray) = domainchunks(d.darray)
size(x::DBCArray) = size(domain(x))
stage(ctx, c::DBCArray) = stage(ctx, c.darray)

# processor domain of DBCArray
pdomain(A::DBCArray) = A.pdomain
```

The standard Base interface methods `collect`, `wait`, `show`, `similar`, `copy`, `/`, `view`, `fetch`, and `==` are overloaded for `DBCArray`:

```julia
# Collect method
function Base.collect(d::DBCArray; tree=false)
    return collect(d.darray; tree=tree)
end

# Wait method
Base.wait(A::DBCArray) = wait(A.darray.chunks)

# Show method
function Base.show(io::IO, ::MIME"text/plain", A::DBCArray{T,N,B,F}) where {T,N,B,F}
    nparts = N > 0 ? size(A.darray.chunks) : 1
    partsize = N > 0 ? A.darray.partitioning.blocksize : 1
    nprocs = N > 0 ? size(A.pdomain) : 1
    write(io, " with $(join(nparts, 'x')) partitions of size $(join(partsize, 'x')) distributed to $(join(nprocs, 'x')) processors:")
    pct_complete = 100 * (sum(c->c isa Chunk ? true : isready(c), A.darray.chunks) / length(A.darray.chunks))
    if pct_complete < 100
        println(io)
        printstyled(io, "~$(round(Int, pct_complete))% completed"; color=:yellow)
    end
    println(io)
    Base.print_array(IOContext(io, :compact=>true), ColorArray(A.darray))
end

# Copy method
Base.copy(x::DBCArray{T,N,B,F}) where {T,N,B,F} =  DBCArray{T,N,B,F}(x.darray, x.pdomain)

# Division method
Base.:/(x::DBCArray{T,N,B,F}, y::U) where {T<:Real, U<:Real, N, B, F} = DBCArray(x.darray / y, x.pdomain)

# View method
# function Base.view(c::DArray, d)
#     subchunks, subdomains = lookup_parts(c, chunks(c), domainchunks(c), d)
#     d1 = alignfirst(d)
#     DArray(eltype(c), d1, subdomains, subchunks, c.partitioning, c.concat)
# end

# Fetch method
# Base.fetch(c::DBCArray{T,N,B,F}) where {T,N,B,F} = c

# Equality checks
# function Base.:(==)(x::ArrayOp{T,N}, y::AbstractArray{S,N}) where {T,S,N}
#     collect(x) == y
# end
# function Base.:(==)(x::AbstractArray{T,N}, y::ArrayOp{S,N}) where {T,S,N}
#     return collect(x) == y
# end
```

Lastly, `logs_annotate!` for `DBCArray`:

```julia
# Annotate the logs of DBCArray
function logs_annotate!(ctx::Context, A::DBCArray, name::Union{String,Symbol})
    for (idx, chunk) in enumerate(A.darray.chunks)
        sd = A.subdomains[idx]
        Dagger.logs_annotate!(ctx, chunk, name*'['*join(sd.indexes, ',')*']')
    end
end
```

### matrix.jl

The Adjoint and Transpose operations for `DBCArray` have been implemented similarly to `DArray`, ensuring that computations are carried out on the respective processors.

```julia
# Define adjoint/transpose copy function
function copydiag(f, A::DBCArray{T, 2}) where T
    Ac = A.darray.chunks
    Ac_copy = Matrix{Any}(undef, size(Ac, 2), size(Ac, 1))
    _copytile(f, Ac) = copy(f(Ac))
    for idx in CartesianIndices(Ac)
        proc = A.pdomain[mod1.(Tuple(idx), size(A.pdomain))...]
        Ac_copy[idx'] = Dagger.@spawn scope=Dagger.ExactScope(proc) _copytile(f, Ac[idx])
    end
    Ad_copy = DArray{T,N,B,F}(ArrayDomain(1:size(A,2), 1:size(A,1)), A.darray.subdomains', Ac_copy, A.darray.partitioning, A.darray.concat)
    return DBCArray(Ad_copy, A.pdomain)
end

# Overload fetch, copy, collect for Adjoint/Transpose
Base.fetch(A::Adjoint{T, <:DBCArray{T, 2}}) where T = copydiag(Adjoint, parent(A))
Base.fetch(A::Transpose{T, <:DBCArray{T, 2}}) where T = copydiag(Transpose, parent(A))
Base.copy(A::Adjoint{T, <:DBCArray{T, 2}}) where T = fetch(A)
Base.copy(A::Transpose{T, <:DBCArray{T, 2}}) where T = fetch(A)
Base.collect(A::Adjoint{T, <:DBCArray{T, 2}}) where T = collect(copy(A))
Base.collect(A::Transpose{T, <:DBCArray{T, 2}}) where T = collect(copy(A))
```

Matrix-vector multiplication and power operations can be done just like `DArray` for distributed `DBCArray`.

```julia
# Matrix-vector multiplication
(*)(a::DBCArray, b::Vector) = DBCArray((a.darray)*b, a.pdomain)

# Power operation
# Base.power_by_squaring(x::DBCArray{T,N,B,F}, i::Int) where {T,N,B,F} = foldl(*, ntuple(_ -> x, i))
```

Other Matrix-Matrix operations like `+` , `*` , `MatMul` , and like `scale`, `Concat`, `cat`, `hcat`, `vcat` are planned for future implementation.

### indexing.jl

Indexing in `DBCArray` is handled by overloading the `getindex` and `setindex!` functions, allowing read and write access just like regular arrays:

```julia
# Indexing of DBCArray 
Base.getindex(A::DBCArray{T,N}, idx::NTuple{N,Int}) where {T,N} = getindex(A.darray, idx)
Base.getindex(A::DBCArray, idx::Integer...) = getindex(A.darray, idx)
Base.getindex(A::DBCArray, idx::Integer) = getindex(A.darray, idx)
Base.getindex(A::DBCArray, idx::CartesianIndex) = getindex(A.darray, idx)
Base.getindex(A::DBCArray{T,N}, idxs::Dims{S}) where {T,N,S} = getindex(A.darray, idxs)

Base.setindex!(A::DBCArray{T,N}, value, idx::NTuple{N,Int}) where {T,N} = setindex!(A.darray, value, idx)
Base.setindex!(A::DBCArray, value, idx::Integer...) = setindex!(A.darray, value, idx)
Base.setindex!(A::DArray, value, idx::Integer) = setindex!(A.darray, value,idx)
Base.setindex!(A::DArray, value, idx::CartesianIndex) = setindex!(A.darray, value, idx)
Base.setindex!(A::DBCArray{T,N}, value, idxs::Dims{S}) where {T,N,S} = setindex!(A.darray, value, idxs)
```

## Usage of `dbcarray.jl`:

First, install the necessary packages and start three more worker processes:

```julia
import Pkg
Pkg.add("Distributed"); using Distributed
addprocs(3)
Pkg.add(url="https://github.com/AkhilAkkapelli/Dagger.jl.git")
@everywhere using Dagger
```

Then, define `pdomain` as a 2×2 matrix of Dagger processors for block-cyclic distribution:

```julia
@everywhere pdomain = reshape(collect(Dagger.all_processors()), 2, 2)
```

Create a `DBCArray` using the constructor with a random matrix:

```julia
A = rand(Blocks(3, 3), 15, 15);
Adbc = DBCArray(A, pdomain);
```

Check if the constructed object is of the right type:

```julia
isa(Adbc, DBCMatrix)
```

Use the available utility functions to query properties:

```julia
domain(Adbc)        # Returns the domain of the array
Dagger.chunks(Adbc)        # Returns the individual blocks
Dagger.domainchunks(Adbc)  # Returns domain-specific chunk mapping
size(Adbc)          # Returns array size
Dagger.pdomain(Adbc)       # Returns processor domain
```

Use standard Base methods similar to how they work with DArray:

```julia
collect(Adbc)       # Gather the full array
Adbc2 = copy(Adbc)  # Make a deep copy
Adbc3 = Adbc / 3    # Perform element-wise division
```

Transpose and Adjoint operations also work:

```julia
Adbc4 = transpose(Adbc)
Adbc5 = adjoint(Adbc)
Adbc6 = Adbc'  # Adjoint using shorthand
```

## Note

To verify which processors each block of a `DBCArray` is assigned to, I used the below code to fetch processor info block-wise:

```julia
# Print processor mapped to each block in the DBCArray
chunk_procs = [Dagger.processor(Adbc.darray.chunks[idx].future.future.v.value[2])
               for idx in CartesianIndices(size(Dagger.domainchunks(Adbc)))]
```

To log these assignments in detail, including task mappings and dependencies, the following snippet was used:

```julia
# Activate logging and create a DBCArray to capture block assignments
using GraphViz

@everywhere Dagger.enable_logging!(
    taskfuncnames=true, tasknames=true, taskdeps=true, taskargs=true,
    taskargmoves=true, taskresult=true, timeline=true,
    all_task_deps=true, taskuidtotid=true, tasktochunk=true)

A = rand(Blocks(3, 3), 15, 15)

Adbc = Dagger.DBCArray(A, pdomain)

@everywhere logs = Dagger.fetch_logs!()

open(raw"graph.dot", "w") do io
    Dagger.show_logs(io, logs, :graphviz; disconnected=true)
end
```

However, the DOT graph generated didn't show processor assignment visually.

## Conclusion

The `DBCArray` extends `DArray` by allowing block-cyclic distribution of data blocks over a given processor layout. Each block is explicitly assigned to a `Dagger.Processor` using `@spawn` with `ExactScope`, giving better control on locality and processor affinity, important for distributed computing performance. It keeps `DArray`'s familiar interface while overloading standard Base methods and adding helper functions for `DBCArray`.&#x20;

Though tools like `fetch_logs!()` show processor assignment, this is not yet reflected visually in DOT graphs. Future work will include adding support for matrix-matrix operations and advanced linear algebra utilities.
