### Multi-span copy (CPU loop or KernelAbstractions)

# Copy many byte spans in one shot. Absolute pointers are converted to element
# offsets from each parent base; lengths must be multiples of sizeof(T). On
# Arrays this is a tight unsafe_copyto! loop; on GPU arrays it is a single
# KernelAbstractions launch over the packed element count.
#
# Argument order is destination-first everywhere in this file, matching Base's
# `copyto!(dst, src)`: `(dst, src)`, `(dst_ptrs, src_ptrs)`, `(to, from)` and
# `(to_ainfo, from_ainfo)`.

@kernel function _multi_span_copy_kernel!(dst, src, dst_offs, src_offs, prefix)
    i = @index(Global, Linear)
    # prefix[s] < i <= prefix[s+1], with prefix[1] == 0
    lo = 1
    hi = length(prefix) - 1
    s = 1
    @inbounds while lo <= hi
        mid = (lo + hi) >>> 1
        if i <= prefix[mid]
            hi = mid - 1
        elseif i > prefix[mid + 1]
            lo = mid + 1
        else
            s = mid
            break
        end
    end
    @inbounds begin
        local_i = i - prefix[s]
        dst[dst_offs[s] + local_i] = src[src_offs[s] + local_i]
    end
end

storage_array(x) = x
storage_array(x::AbstractArray) = x
storage_array(x::SubArray) = storage_array(parent(x))
storage_array(x::UpperTriangular) = storage_array(parent(x))
storage_array(x::LowerTriangular) = storage_array(parent(x))
storage_array(x::UnitUpperTriangular) = storage_array(parent(x))
storage_array(x::UnitLowerTriangular) = storage_array(parent(x))
storage_array(x::Transpose) = storage_array(parent(x))
storage_array(x::Adjoint) = storage_array(parent(x))
storage_array(x::Diagonal) = storage_array(parent(x))

# Narrow, internal helper: pick which of two already-unwrapped dense storage
# arrays (at least one of which is a GPU array) to take the KA backend from.
# This is *not* used to decide whether the multi-span path is needed — that
# decision keys off the memory space (`is_device_space`), which stays correct
# for composite objects whose own type is not a GPU array type.
is_device_array(x) = x isa GPUArraysCore.AbstractGPUArray

# Whether `space` is device (VRAM) memory. Uses the existing per-backend
# `gpu_memory_kind` signal (`:CPU` for host RAM, `:CUDA`/`:ROC`/… for devices),
# so it is authoritative even for wrappers like a `HaloArray` holding device
# buffers, or views of device arrays, whose element/container type is not itself
# an `AbstractGPUArray`.
is_device_space(space::MemorySpace) = gpu_memory_kind(space) !== :CPU

function _ptr_to_elem_off(base::UInt64, ptr::UInt64, elsize::Int)
    delta = ptr - base
    @assert delta % elsize == 0 "Span pointer is not element-aligned: $ptr vs base $base (elsize $elsize)"
    return UInt32(delta ÷ elsize) # 0-based
end

function _build_span_descriptors(dst::AbstractArray{T}, src::AbstractArray{T},
                                 dst_ptrs, src_ptrs, lens) where T
    n = length(dst_ptrs)
    @assert n == length(src_ptrs) == length(lens)
    elsize = sizeof(T)
    dst_vec = reshape(dst, :)
    src_vec = reshape(src, :)
    dst_base = UInt64(pointer(dst_vec))
    src_base = UInt64(pointer(src_vec))
    dst_offs = Vector{UInt32}(undef, n)
    src_offs = Vector{UInt32}(undef, n)
    prefix = Vector{UInt32}(undef, n + 1)
    prefix[1] = 0
    for i in 1:n
        len = UInt64(lens[i])
        @assert len % elsize == 0 "Span length is not an integer multiple of the element size: $len / $elsize"
        ne = UInt32(len ÷ elsize)
        dst_offs[i] = _ptr_to_elem_off(dst_base, UInt64(dst_ptrs[i]), elsize)
        src_offs[i] = _ptr_to_elem_off(src_base, UInt64(src_ptrs[i]), elsize)
        prefix[i + 1] = prefix[i] + ne
    end
    return dst_vec, src_vec, dst_offs, src_offs, prefix
end

function _device_u32(backend, data::Vector{UInt32})
    d = KernelAbstractions.allocate(backend, UInt32, length(data))
    copyto!(d, data)
    return d
end

"""
    multi_span_copy!(dst, src, dst_ptrs, src_ptrs, lens)

Copy `lens[i]` bytes from `src` at absolute pointer `src_ptrs[i]` into `dst` at
`dst_ptrs[i]`, for each span `i`. `dst` and `src` must share element type `T`
and be the dense storage backing those pointers (see `storage_array`).
"""
function multi_span_copy!(dst::AbstractArray{T}, src::AbstractArray{T},
                          dst_ptrs, src_ptrs, lens) where T
    isempty(dst_ptrs) && return
    dst_vec, src_vec, dst_offs, src_offs, prefix = _build_span_descriptors(dst, src, dst_ptrs, src_ptrs, lens)
    total = Int(prefix[end])
    total == 0 && return

    if dst isa Array && src isa Array
        GC.@preserve dst_vec src_vec begin
            for i in eachindex(dst_offs)
                n = Int(prefix[i + 1] - prefix[i])
                n == 0 && continue
                unsafe_copyto!(pointer(dst_vec, Int(dst_offs[i]) + 1),
                               pointer(src_vec, Int(src_offs[i]) + 1), n)
            end
        end
        return
    end

    backend = KernelAbstractions.get_backend(is_device_array(dst) ? dst : src)
    dst_offs_d = _device_u32(backend, dst_offs)
    src_offs_d = _device_u32(backend, src_offs)
    prefix_d = _device_u32(backend, prefix)
    kern = _multi_span_copy_kernel!(backend)
    kern(dst_vec, src_vec, dst_offs_d, src_offs_d, prefix_d; ndrange=total)
    # This synchronize is required (independent of any following DtoH): the
    # kernel reads the device-resident descriptor arrays allocated just above
    # (`dst_offs_d`/`src_offs_d`/`prefix_d`), which become GC-eligible on return.
    # Without the sync they could be freed while the kernel is still in flight
    # (use-after-free). Letting the copy run stream-ordered/async with other
    # kernels would require descriptors with caller-managed, stream-scoped
    # lifetime; that is deferred for now.
    KernelAbstractions.synchronize(backend)
    return
end

# `spans[i] == (src_span, dst_span)` (source first, matching RemainderAliasing).
function multi_span_copy!(dst::AbstractArray{T}, src::AbstractArray{T},
                          spans::Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}) where T
    n = length(spans)
    dst_ptrs = Vector{UInt64}(undef, n)
    src_ptrs = Vector{UInt64}(undef, n)
    lens = Vector{UInt64}(undef, n)
    for i in eachindex(spans)
        src_span, dst_span = spans[i]
        @assert src_span.len == dst_span.len
        dst_ptrs[i] = dst_span.ptr
        src_ptrs[i] = src_span.ptr
        lens[i] = src_span.len
    end
    return multi_span_copy!(dst, src, dst_ptrs, src_ptrs, lens)
end

# Gather `src`'s spans into a contiguous `dst` starting at element 1 / byte 0.
# `src_spans` may be any iterable of spans (e.g. a lazy `Iterators.map`).
#
# N.B. This is an internal helper (distinct name), *not* a `multi_span_gather!`
# method: when `T === UInt8` a data `Vector{UInt8}` would be indistinguishable
# from the `buf::Vector{UInt8}` byte-buffer method below, so the buffer method's
# internal call would re-dispatch to itself and recurse forever.
function _multi_span_gather_packed!(dst::AbstractArray{T}, src::AbstractArray{T}, src_spans) where T
    n = length(src_spans)
    dst_ptrs = Vector{UInt64}(undef, n)
    src_ptrs = Vector{UInt64}(undef, n)
    lens = Vector{UInt64}(undef, n)
    dst_base = UInt64(pointer(reshape(dst, :)))
    offset = UInt64(0)
    for (i, span) in enumerate(src_spans)
        len = UInt64(span_len(span))
        dst_ptrs[i] = dst_base + offset
        src_ptrs[i] = UInt64(span_start(span))
        lens[i] = len
        offset += len
    end
    return multi_span_copy!(dst, src, dst_ptrs, src_ptrs, lens)
end

# Scatter a contiguous `src` into `dst`'s spans. Internal helper (distinct name)
# for the same reason as `_multi_span_gather_packed!` above.
function _multi_span_scatter_packed!(dst::AbstractArray{T}, src::AbstractArray{T}, dst_spans) where T
    n = length(dst_spans)
    dst_ptrs = Vector{UInt64}(undef, n)
    src_ptrs = Vector{UInt64}(undef, n)
    lens = Vector{UInt64}(undef, n)
    src_base = UInt64(pointer(reshape(src, :)))
    offset = UInt64(0)
    for (i, span) in enumerate(dst_spans)
        len = UInt64(span_len(span))
        dst_ptrs[i] = UInt64(span_start(span))
        src_ptrs[i] = src_base + offset
        lens[i] = len
        offset += len
    end
    return multi_span_copy!(dst, src, dst_ptrs, src_ptrs, lens)
end

# Whether `x`'s storage is a single, pointer-addressable dense buffer that the
# packed (`unsafe_copyto!` / KA) span path can operate on: a host `Array`, or a
# device array (VRAM buffer, or a wrapper whose memory space is a device). A host
# `AbstractArray` that is *not* an `Array` (e.g. `SparseMatrixCSC`, whose data
# lives across separate `nzval`/`colptr`/`rowval` buffers) is not, and must use
# the per-span remainder path, which resolves each span to its owning buffer.
_span_dense_storage(x) =
    x isa Array || is_device_array(x) || is_device_space(memory_space(x))

# Per-span remainder gather/scatter: resolve each span to the concrete buffer
# holding it (see `find_object_holding_ptr`) and copy field-by-field. Used for
# objects without a single dense backing buffer (RefValue, GPURef,
# SparseMatrixCSC, etc.).
function _gather_spans_remainder!(buf::Vector{UInt8}, src, src_spans)
    offset = UInt64(1)
    for span in src_spans
        ptr = UInt64(span_start(span))
        len = UInt64(span_len(span))
        read_remainder!(buf, offset, src, ptr, len)
        offset += len
    end
    return
end
function _scatter_spans_remainder!(dst, buf::Vector{UInt8}, dst_spans)
    offset = UInt64(1)
    for span in dst_spans
        ptr = UInt64(span_start(span))
        len = UInt64(span_len(span))
        write_remainder!(buf, offset, dst, ptr, len)
        offset += len
    end
    return
end

# Host UInt8 buffer gather/scatter used by remainder and MPI packing paths.
function multi_span_gather!(buf::Vector{UInt8}, src::AbstractArray{T}, src_spans) where T
    src_s = storage_array(src)
    # Host `AbstractArray` without a single dense buffer (e.g. SparseMatrixCSC):
    # fall back to the per-span remainder path rather than the packed/KA path,
    # which would call `pointer` on a non-pointer-addressable array.
    _span_dense_storage(src_s) || return _gather_spans_remainder!(buf, src, src_spans)
    elsize = sizeof(T)
    nbytes = sum(s -> UInt64(span_len(s)), src_spans; init=UInt64(0))
    @assert UInt64(length(buf)) >= nbytes
    @assert nbytes % elsize == 0
    nelem = Int(nbytes ÷ elsize)
    host = unsafe_wrap(Vector{T}, Ptr{T}(pointer(buf)), nelem)
    if src_s isa Array
        _multi_span_gather_packed!(host, src_s, src_spans)
        return
    end
    with_context!(memory_space(src_s))
    backend = KernelAbstractions.get_backend(src_s)
    packed = KernelAbstractions.allocate(backend, T, nelem)
    _multi_span_gather_packed!(packed, src_s, src_spans)
    copyto!(host, packed)
    return
end
function multi_span_gather!(buf::Vector{UInt8}, src, src_spans)
    # RefValue, GPURef, SparseMatrixCSC, etc.: keep the per-span remainder path
    return _gather_spans_remainder!(buf, src, src_spans)
end

function multi_span_scatter!(dst::AbstractArray{T}, buf::Vector{UInt8}, dst_spans) where T
    dst_s = storage_array(dst)
    # Host `AbstractArray` without a single dense buffer (e.g. SparseMatrixCSC):
    # fall back to the per-span remainder path (see `multi_span_gather!`).
    _span_dense_storage(dst_s) || return _scatter_spans_remainder!(dst, buf, dst_spans)
    elsize = sizeof(T)
    nbytes = sum(s -> UInt64(span_len(s)), dst_spans; init=UInt64(0))
    @assert UInt64(length(buf)) >= nbytes
    @assert nbytes % elsize == 0
    nelem = Int(nbytes ÷ elsize)
    host = unsafe_wrap(Vector{T}, Ptr{T}(pointer(buf)), nelem)
    if dst_s isa Array
        _multi_span_scatter_packed!(dst_s, host, dst_spans)
        return
    end
    with_context!(memory_space(dst_s))
    backend = KernelAbstractions.get_backend(dst_s)
    packed = KernelAbstractions.allocate(backend, T, nelem)
    copyto!(packed, host)
    _multi_span_scatter_packed!(dst_s, packed, dst_spans)
    return
end
function multi_span_scatter!(dst, buf::Vector{UInt8}, dst_spans)
    return _scatter_spans_remainder!(dst, buf, dst_spans)
end

# Whether a `move!` must take the multi-span (KA) path instead of plain
# `copyto!`. `copyto!` is fine for host memory (any AbstractArray) and for
# device-to-device between contiguous `DenseArray`s; anything else on device
# memory (views, triangular/diagonal wrappers, composite device containers, …)
# needs the span-based KA copy, since `copyto!` would require illegal scalar
# indexing. Device-ness comes from the memory space, not the array type.
function needs_multi_span_copy(to_space::MemorySpace, from_space::MemorySpace,
                               to::AbstractArray, from::AbstractArray)
    device = is_device_space(to_space) || is_device_space(from_space)
    return device && !(to isa DenseArray && from isa DenseArray)
end

# Copy `from`'s spans into `to`'s spans (matched pairwise by order). `to_ainfo`/
# `from_ainfo` select which spans to move and default to the whole arrays.
function multi_span_move!(to::AbstractArray{T}, from::AbstractArray{T},
                          to_ainfo=to, from_ainfo=from) where T
    to_spans = memory_spans(to_ainfo)
    from_spans = memory_spans(from_ainfo)
    @assert length(from_spans) == length(to_spans) "Mismatched span counts for multi-span move: $(length(from_spans)) vs $(length(to_spans))"
    n = length(from_spans)
    dst_ptrs = Vector{UInt64}(undef, n)
    src_ptrs = Vector{UInt64}(undef, n)
    lens = Vector{UInt64}(undef, n)
    for i in 1:n
        @assert span_len(from_spans[i]) == span_len(to_spans[i])
        dst_ptrs[i] = span_start(to_spans[i])
        src_ptrs[i] = span_start(from_spans[i])
        lens[i] = span_len(from_spans[i])
    end
    return multi_span_copy!(storage_array(to), storage_array(from), dst_ptrs, src_ptrs, lens)
end

# GPU-aware AbstractArray move! (CPU spaces). Device-space methods in GPU
# extensions call needs_multi_span_copy / multi_span_move! the same way.
function move!(to_space::MemorySpace, from_space::MemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if needs_multi_span_copy(to_space, from_space, to, from)
        multi_span_move!(to, from)
    else
        copyto!(to, from)
    end
    return
end

function move!(::Type{<:Diagonal}, to_space::MemorySpace, from_space::MemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if is_device_space(to_space) || is_device_space(from_space)
        multi_span_move!(to, from, aliasing(to, Diagonal), aliasing(from, Diagonal))
    else
        copyto!(view(to, diagind(to)), view(from, diagind(from)))
    end
    return
end
