
abstract Processor

immutable OSProc <: Processor
    pid::Int
end

"""
Affinity maps processors to various parts of a chunk.
"""
immutable Affinity
    procs::Vector
    partition::PartitionScheme
    parts::DomainBranch
end


"""
    move(ctx, affinity::Affinity, data::AbstractChunk)

move subparts of `data` according to `affinity`, returns a
SuperChunk where each child chunks is at the corresponding processor
"""
