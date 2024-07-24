using Random

function Random.randn!(A::DArray{T}) where T
    Ac = A.chunks

    Dagger.spawn_datadeps() do
        for chunk in Ac
            Dagger.@spawn randn!(InOut(chunk))
        end
    end

    return A
end
