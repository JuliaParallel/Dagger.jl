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
        nrows = sum(nrows_parts)
        rowsums = cumsum(nrows_parts)
        starts = vcat(0, rowsums[1:end-1]) .+1
        row_ranges = map(UnitRange, starts,rowsums)

        p,dmn = if ncols == 1
            BlockPartition((floor(Int, nrows/length(ds)),)),
            DomainSplit(DenseDomain((1:nrows,)), map(r -> DenseDomain((r,)), row_ranges))
        else
            BlockPartition((floor(Int, nrows/length(ds)), ncols)),
            DomainSplit(DenseDomain(1:nrows, 1:ncols), map(r -> DenseDomain(r, 1:ncols), row_ranges))
        end
        Cat(p, parttype(ps[1]), dmn, ps_arr)
    end
end
