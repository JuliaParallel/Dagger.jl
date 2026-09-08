module DelimitedFilesExt

using DelimitedFiles
using Dagger

"""
    DelimitedFiles.readdlm(input, part::Blocks, args...; kwargs...) -> DArray

`readdlm` then `distribute` with `part`. This is the dense-array hook;
sparse operators should use `MatrixMarket.mmread`.
"""
function DelimitedFiles.readdlm(input::AbstractString, part::Dagger.Blocks,
                                args...; kwargs...)
    return Dagger.distribute(DelimitedFiles.readdlm(input, args...; kwargs...), part)
end

"""
    DelimitedFiles.writedlm(output, A::DArray, args...; kwargs...)

Write a dense `DArray` via `collect`. A sparse-backed `DMatrix` throws
(would densify); use `MatrixMarket.mmwrite`.
"""
function DelimitedFiles.writedlm(output, A::Dagger.DArray, args...; kwargs...)
    if A isa Dagger.DMatrix && Dagger.is_sparse_backed(A)
        throw(ArgumentError(
            "DelimitedFiles.writedlm would densify a sparse DMatrix; \
             use MatrixMarket.mmwrite"))
    end
    return DelimitedFiles.writedlm(output, collect(A), args...; kwargs...)
end

end
