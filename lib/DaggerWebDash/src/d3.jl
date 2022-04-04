using Mux, MemPool, Distributed, Sockets
import Dagger.Tables

struct LinePlot
    core_key::Symbol
    data_key::Symbol
    xlabel::String
    ylabel::String
end
StructTypes.StructType(::Type{LinePlot}) = StructTypes.CustomStruct()
StructTypes.lower(lp::LinePlot) = "linePlot(svg_container, \"$(lp.core_key)\", \"$(lp.data_key)\", \"$(lp.xlabel)\", \"$(lp.ylabel)\")"

struct GanttPlot
    core_key::Symbol
    id_key::Symbol
    timeline_key::Symbol
    esat_key::Symbol
    psat_key::Symbol
    title::String
end
StructTypes.StructType(::Type{GanttPlot}) = StructTypes.CustomStruct()
StructTypes.lower(lp::GanttPlot) = "ganttPlot(svg_container, \"$(lp.core_key)\", \"$(lp.id_key)\", \"$(lp.timeline_key)\", \"$(lp.esat_key)\", \"$(lp.psat_key)\", \"$(lp.title)\")"

struct GraphPlot
    core_key::Symbol
    id_key::Symbol
    timeline_key::Symbol
    profile_key::Symbol
    title::String
end
StructTypes.StructType(::Type{GraphPlot}) = StructTypes.CustomStruct()
StructTypes.lower(lp::GraphPlot) = "graphPlot(svg_container, \"$(lp.core_key)\", \"$(lp.id_key)\", \"$(lp.timeline_key)\", \"$(lp.profile_key)\", \"$(lp.title)\")"

struct ProfileViewer
    core_key::Symbol
    profile_key::Symbol
    title::String
end
StructTypes.StructType(::Type{ProfileViewer}) = StructTypes.CustomStruct()
StructTypes.lower(lp::ProfileViewer) = "profileViewer(svg_container, \"$(lp.core_key)\", \"$(lp.profile_key)\", \"$(lp.title)\")"

## Core globals
const D3R_RUNNING = Dict{Int,Bool}()
const D3R_LOGS = Dict{Int,Dict{Symbol,Any}}()
const D3R_ACTUAL_PORT = Dict{Int,Int}()

## Client/forwarding globals
# Port -> Worker -> Sockets
# TODO: Concrete socket types
const D3R_CLIENT_SOCKETS = Dict{Int,Dict{Int,Vector{Any}}}()

const D3R_WORKER_HOST_MAP = Dict{Int,IPAddr}()
const D3R_WORKER_PORT_MAP = Dict{Int,Int}()

struct D3Renderer
    port::Int
    port_range::UnitRange{Int}
    config::Vector{Any}
    config_updated::Ref{Bool}
    update_cond::Threads.Condition
    seek_store
end
D3Renderer(port::Int, port_range=50000:59999; seek_store=nothing) =
    D3Renderer(port,
               port_range,
               Vector{Any}(),
               Ref{Bool}(false),
               Threads.Condition(),
               seek_store)
Dagger.init_similar(d3r::D3Renderer) =
    D3Renderer(d3r.port,
               d3r.port_range,
               d3r.config,
               Ref{Bool}(false),
               Threads.Condition(),
               d3r.seek_store)

function (d3r::D3Renderer)(logs)
    D3R_LOGS[d3r.port] = logs
    d3r_init(d3r.port, d3r.port_range, d3r.config_updated, d3r.config, d3r.seek_store)
    lock(d3r.update_cond) do
        notify(d3r.update_cond)
    end
end

function Base.push!(d3r::D3Renderer, x)
    push!(d3r.config, x)
    d3r.config_updated[] = true
end

function d3r_init(port::Int, port_range::UnitRange, config_updated::Ref{Bool}, config, seek_store)
    get(D3R_RUNNING, port, false) && return

    # Ensure some variables are setup
    get!(()->Dict{Symbol,Any}(), D3R_LOGS, port)
    get!(()->[], get!(()->Dict{Int,Vector{Any}}(), D3R_CLIENT_SOCKETS, port), myid())

    # Server configuration
    d3_app = (
        Mux.prod_defaults,
        page("/", req->Dict(
            :status=>301,
            :headers=>Dict(:Location=>"/index.html"),
            :body=>"Moved permanently"
        )),
        page("/index.html", req->begin
            html = String(read(joinpath(@__DIR__, "index.html")))
            hdrs = Dict(
                Symbol("Content-Type")=>"text/html",
                # For interactive development
                Symbol("Cache-Control")=>"no-store",
            )
            Dict(
                :body=>html,
                :headers=>hdrs,
            )
        end),
        page("/fs/:path", req->begin
            path = req[:params][:path]
            path = "/tmp/" * path
            if ispath(path)
                Dict(:body=>String(read(path)))
            else
                Dict(:status=>404)
            end
        end),
        page("/worker_id", respond("$(myid())")),
        page("/worker/:id/:location", req->begin
            id = parse(Int, req[:params][:id])
            location = req[:params][:location]
            worker_host, worker_port = worker_host_port(id, port, port_range)
            res = HTTP.get("http://$worker_host:$worker_port/$location")
            return Dict(
                :status=>res.status,
                :body=>String(res.body),
            )
        end),
        Mux.notfound()
    )
    d3_wapp = (
        Mux.wdefaults,
        route("/data_feed", req->begin
            sock = req[:socket]
            client_handler(sock, myid(), port, port_range, config_updated, config, seek_store)
        end),
        route("/worker/:id/data_feed", req->begin
            sock = req[:socket]
            id = parse(Int, req[:params][:id])
            client_handler(sock, id, port, port_range, config_updated, config, seek_store)
        end),
        Mux.wclose,
        Mux.notfound()
    )

    actual_port = get_actual_port(port, port_range)

    # Start D3R server
    Mux.serve(Mux.App(mux(d3_app...)),
              Mux.App(mux(d3_wapp...)),
              actual_port)
    @debug "D3R initialized for port $port at actual port $actual_port"
    D3R_ACTUAL_PORT[port] = actual_port
    D3R_RUNNING[port] = true
end
function client_handler(sock, id, port, port_range, config_updated, config, seek_store)
    fsock = nothing
    if id != myid()
        fsock_ready = Base.Event()
        worker_host, worker_port = worker_host_port(id, port, port_range)
        Mux.HTTP.WebSockets.open("ws://$worker_host:$worker_port/data_feed") do _fsock
            fsock = _fsock
            @debug "D3R forwarder for $id ready"
            notify(fsock_ready)
            while !eof(fsock) && isopen(fsock)
                try
                    bytes = readavailable(fsock)
                    if length(bytes) == 0
                        sleep(0.1)
                        continue
                    end
                    data = String(bytes)
                    #@info "D3R forwarder for $id received data"
                    write(sock, data)
                catch err
                    if err isa Mux.WebSockets.WebSocketClosedError || err isa Base.IOError
                        # Force-close client and forwarder
                        @async close(sock)
                        @async close(fsock)
                        break
                    end
                    @error "D3R forwarder for $id error!" exception=(err,catch_backtrace())
                    rethrow(err)
                end
            end
            @debug "D3R forwarder for $id closed"
        end
        wait(fsock_ready)
    end
    if id == myid()
        @debug "D3R client for $id sending initial config"
        write(sock, JSON3.write((;cmd="data", payload=sanitize(D3R_LOGS[port]))))
        _workers = workers()
        if !(myid() in _workers)
            # FIXME: Get this from the Context
            _workers = vcat(myid(), _workers)
        end
        write(sock, JSON3.write((;cmd="config", payload=sanitize((;myid=myid(),workers=_workers,ctxs=config)))))
    end
    push!(get!(()->[], D3R_CLIENT_SOCKETS[port], id), sock)
    @debug "D3R client for $id ready"
    while !eof(sock) && isopen(sock)
        try
            data = String(read(sock))
            @debug "D3R client for $id received: $data"
            if id == myid()
                #= FIXME
                if config_updated[]
                    config_updated[] = false
                    write(sock, JSON3.write((;cmd="config", payload=sanitize(config))))
                end
                =#
                if seek_store !== nothing
                    if data == "fulldata"
                        raw_logs = seek_store[0:typemax(UInt64)]
                        logs = Dict{Symbol,Vector{Any}}()
                        for (idx,key) in enumerate(Tables.columnnames(raw_logs))
                            logs[key] = Tables.columns(raw_logs)[idx]
                        end
                        write(sock, JSON3.write((;cmd="data", payload=sanitize(logs))))
                        continue
                    end
                    m = match(r"seek\(([0-9]*),([0-9]*)\)", data)
                    if m !== nothing
                        ts_start, ts_stop = parse.(Ref(UInt64), m.captures)
                        raw_logs = seek_store[ts_start:ts_stop]
                        logs = Dict{Symbol,Vector{Any}}()
                        for (idx,key) in enumerate(Tables.columnnames(raw_logs))
                            logs[key] = Tables.columns(raw_logs)[idx]
                        end
                        write(sock, JSON3.write((;cmd="data", payload=sanitize(logs))))
                        continue
                    end
                end
                if data == "data"
                    write(sock, JSON3.write((;cmd="data", payload=sanitize(D3R_LOGS[port]))))
                end
            else
                @debug "D3R client sending to forwarder: $data"
                write(fsock, data)
            end
        catch err
            if err isa Mux.WebSockets.WebSocketClosedError || err isa Base.IOError
                idx = findfirst(x->x==sock, D3R_CLIENT_SOCKETS[port][id])
                if idx !== nothing
                    deleteat!(D3R_CLIENT_SOCKETS[port][id], idx)
                end
                # Force-close client and forwarder
                @async close(sock)
                @async close(fsock)
                break
            end
            @error "D3R client for $id error!" exception=(err,catch_backtrace())
            rethrow(err)
        end
    end
    @debug "D3R client for $id closed"
end
function Dagger.Events.creation_hook(d3r::D3Renderer, log)
    for sock in get!(()->[], get!(()->Dict{Int,Vector{Any}}(), D3R_CLIENT_SOCKETS, d3r.port), myid())
        try
            if isopen(sock)
                write(sock, JSON3.write((;cmd="add", payload=sanitize(log))))
            end
        catch err
            if err isa Mux.WebSockets.WebSocketClosedError
                continue
            end
            rethrow(err)
        end
    end
end
function Dagger.Events.deletion_hook(d3r::D3Renderer, idx)
    for sock in get!(()->[], get!(()->Dict{Int,Vector{Any}}(), D3R_CLIENT_SOCKETS, d3r.port), myid())
        try
            if isopen(sock)
                write(sock, JSON3.write((;cmd="delete", payload=idx)))
            end
        catch err
            if err isa Mux.WebSockets.WebSocketClosedError
                continue
            end
            rethrow(err)
        end
    end
end
function worker_host_port(id::Int, port::Int, port_range::UnitRange)
    worker_host = get!(D3R_WORKER_HOST_MAP, port) do
        worker_host, worker_port = remotecall_fetch(id, port) do port
            (MemPool.host, get_actual_port(port, port_range))
        end
        D3R_WORKER_PORT_MAP[port] = worker_port
        worker_host
    end
    worker_port = D3R_WORKER_PORT_MAP[port]
    return worker_host, worker_port
end
function get_actual_port(port::Int, port_range::UnitRange)
    get!(D3R_ACTUAL_PORT, port) do
        # Check if the requested port is available
        # If not, pick another port in the provided range
        actual_port = port
        while true
            port_available = try
                close(listen(actual_port))
                true
            catch err
                err isa Base.IOError || rethrow(err)
                if err.code == -98
                    false
                else
                    rethrow(err)
                end
            end
            if port_available
                if port != actual_port && myid() == 1
                    @warn "Port $port not available, serving on $actual_port"
                end
                break
            end
            actual_port = rand(port_range)
        end
        actual_port
    end
end
