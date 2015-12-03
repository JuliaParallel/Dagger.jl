export Context

type Context
end

function chunk_targets(ctx::Context, node)
    map(Proc, procs()) # For now, the targets are all the available procs
end
