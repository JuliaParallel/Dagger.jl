function LinearAlgebra.norm2(A::DArray{T,2}) where T
    Ac = A.chunks
    norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac]::Matrix{DTask}
    return sqrt(sum(map(norm->fetch(norm)::real(T), norms)))
end
function LinearAlgebra.norm2(A::UpperTriangular{T,<:DArray{T,2}}) where T
    Ac = parent(A).chunks
    Ac_upper = []
    for i in 1:size(Ac, 1)
        append!(Ac_upper, Ac[i, (i+1):end])
    end
    upper_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_upper]
    Ac_diag = [Dagger.spawn(UpperTriangular, Ac[i,i]) for i in 1:size(Ac, 1)]
    diag_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_diag]
    return sqrt(sum(map(fetch, upper_norms)) + sum(map(fetch, diag_norms)))
end
function LinearAlgebra.norm2(A::LowerTriangular{T,<:DArray{T,2}}) where T
    Ac = parent(A).chunks
    Ac_lower = []
    for i in 1:size(Ac, 1)
        append!(Ac_lower, Ac[(i+1):end, i])
    end
    lower_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_lower]
    Ac_diag = [Dagger.spawn(LowerTriangular, Ac[i,i]) for i in 1:size(Ac, 1)]
    diag_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_diag]
    return sqrt(sum(map(fetch, lower_norms)) + sum(map(fetch, diag_norms)))
end

is_cross_symmetric(A1, A2) = A1 == A2'
function LinearAlgebra.issymmetric(A::DArray{T,2}) where T
    Ac = A.chunks
    if size(Ac, 1) != size(Ac, 2)
        return false
    end

    to_check = [Dagger.@spawn issymmetric(Ac[i, i]) for i in 1:size(Ac, 1)]
    for i in 2:(size(Ac, 1)-1)
        j_pre_diag = i - 1
        for j in 1:j_pre_diag
            push!(to_check, Dagger.@spawn is_cross_symmetric(Ac[i, j], Ac[j, i]))
        end
    end

    return all(fetch, to_check)
end
function LinearAlgebra.ishermitian(A::DArray{T,2}) where T
    Ac = A.chunks
    if size(Ac, 1) != size(Ac, 2)
        return false
    end

    to_check = [Dagger.@spawn ishermitian(Ac[i, i]) for i in 1:size(Ac, 1)]
    for i in 2:(size(Ac, 1)-1)
        j_pre_diag = i - 1
        for j in 1:j_pre_diag
            push!(to_check, Dagger.@spawn is_cross_symmetric(Ac[i, j], Ac[j, i]))
        end
    end

    return all(fetch, to_check)
end
