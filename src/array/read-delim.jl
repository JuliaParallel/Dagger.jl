export dist_readdlm

include("../lib/block-io.jl")

immutable ReadDelim <: Computation
    file::AbstractString
    delim::Char
    numparts::Int
    cleanup::Function
end

dist_readdlm(file, delim::Char, numparts::Int, cleanup=x->x) = ReadDelim(file, delim, numparts, cleanup)

function stage(ctx, rd::ReadDelim)
    ranges = split_range(1:filesize(rd.file), rd.numparts)
    thunks = map(ranges) do range
        Thunk((range,)) do r
            open(rd.file, "r") do f
                rd.cleanup(
                    readdlm(BlockIO(f, r, '\n'), rd.delim)
                )
            end
        end
    end
    # figure out number of columns
    Thunk((thunks...); meta=true) do ps...
        ps_arr = [ps...]
        ds = map(domain, ps_arr)
        ncols = size(ds[1], 2)
        nrows_parts = map(d->size(d, 1), ds)
        cumlength = cumsum(nrows_parts)
        nrows = cumlength[end]

        dmn = if ncols == 1
            pieces = BlockedDomains((1,), (cumlength,))
            DomainSplit(DenseDomain((1:nrows,)), pieces)
        else
            pieces = BlockedDomains((1,1), (cumlength,[ncols]))
            DomainSplit(DenseDomain(1:nrows, 1:ncols), pieces)
        end
        Cat(parttype(ps[1]), dmn, ps_arr)
    end
end
