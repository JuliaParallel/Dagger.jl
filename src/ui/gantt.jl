import .Luxor: Drawing, finish, Point, background, sethue, fontsize, rect, text

getname(p::Dagger.OSProc) = "OS Process on worker $(p.pid)"
getname(p::Dagger.ThreadProc) = "Thread $(p.tid) on worker $(p.owner)"
getname(p) = sprint(Base.show, p)

function proclt(p1::T, p2::R) where {T,R}
    if p1.owner != p2.owner
        return p1.owner < p2.owner
    else
        return repr(T) < repr(R)
    end
end
function proclt(p1::T, p2::T) where {T}
    if p1.owner != p2.owner
        return p1.owner < p2.owner
    else
        for field in fieldnames(T)
            f1 = getfield(p1, field)
            f2 = getfield(p2, field)
            if f1 != f2
                return f1 < f2
            end
        end
    end
    false
end
proclt(p1::Dagger.OSProc, p2::Dagger.OSProc) = p1.pid < p2.pid
proclt(p1::Dagger.OSProc, p2) = p1.pid < p2.owner
proclt(p1, p2::Dagger.OSProc) = p1.owner < p2.pid

prof_to_svg(::Any, ::Any, ::Any; kwargs...) = nothing

combine_gantt_images(::Any, ::Any, ::Any) = NamedTuple()

continue_rendering = Ref{Bool}(true)
render_results = Channel()

function draw_gantt(ctx, svg_path, prof_path; delay=2, width=1000, height=640, window_length=20)
    root_time = time_ns()
    window_logs = []
    if window_length !== nothing
        window_start = -window_length
    else
        window_start = 0
    end
    window_finish = 0
    procs = Set()
    for gp in ctx.procs
        for proc in Dagger.get_processors(gp)
            push!(procs, proc)
        end
    end
    if (height/length(procs)) < 50
        height = length(procs) * 50
        @warn "SVG height too small; resizing to $height pixels"
    end

    image_idx = 0
    while continue_rendering[]
        sleep(delay)
        logs = Dagger.get_logs!(ctx.log_sink, true) # get raw events
        if !isempty(logs)
            for id in keys(logs)
                append!(window_logs, map(x->(x,), filter(x->x.category==:compute, logs[id])))
            end
        end
        for idx in length(window_logs):-1:1
            log = window_logs[idx]
            if length(log) == 2
                # Clear out finished events older than window start
                log_finish_s = (log[2].timestamp-root_time)/(1000^3)
                if log_finish_s < window_start
                    @debug "Gantt: Deleted event"
                    deleteat!(window_logs, idx)
                end
            elseif log[1] isa Dagger.Event{:finish}
                # Pair finish events with start events
                sidx = findfirst(x->length(x) == 1 &&
                                    x[1] isa Dagger.Event{:start} &&
                                    x[1].id==log[1].id, window_logs)
                if sidx === nothing
                    @debug "Gantt: Removed unpaired finish"
                    deleteat!(window_logs, idx)
                    continue
                end
                window_logs[sidx] = (window_logs[sidx][1], log[1])
                @debug "Gantt: Paired event"
                deleteat!(window_logs, idx)
            end
        end
        isempty(window_logs) && continue

        # Concatenate and render profile data
        prof_data = UInt64[]
        prof_dict = Dict{UInt64, Vector{Base.StackTraces.StackFrame}}()
        for log in filter(x->length(x)==2, window_logs)
            append!(prof_data, log[2].profiler_samples.samples)
            merge!(prof_dict, log[2].profiler_samples.lineinfo)
        end
        prof_to_svg(prof_path, prof_data, prof_dict, image_idx; width=width)

        for proc in unique(map(x->x[1].timeline[2], window_logs))
            push!(procs, proc)
        end
        colors = Colors.distinguishable_colors(length(procs))
        procs_len = length(procs)
        proc_height = height/(3procs_len)
        @debug "Gantt: Start"
        if isfile(svg_path)
            Drawing(width, height, svg_path)
        else
            Drawing(width, height, joinpath(svg_path, repr(image_idx) * ".svg"))
        end
        background("white")
        for (proc_idx, proc) in enumerate(sort(collect(procs); lt=proclt))
            ypos = (proc_idx-0.5)*(height/procs_len)
            sethue("grey15")
            fontsize(round(Int,proc_height))
            text(getname(proc), Point(width/2,ypos-(proc_height/2)))
            rect(Point(1,ypos+(proc_height/3)),width-2,proc_height,:stroke)
            fontsize(8)
            text("$(window_start) s", Point(0,ypos); halign=:left)
            text("$(window_finish) s", Point(width-8,ypos); halign=:right)
            proc_color = colors[proc_idx]
            for log in filter(x->x[1].timeline[2]==proc, window_logs)
                length(log) == 1 && log[1] isa Dagger.Event{:finish} && error("Unpaired finish!")
                log_start_s = (log[1].timestamp-root_time)/(1000^3)
                log_finish_s = if length(log) == 2
                    log_finish_s = (log[2].timestamp-root_time)/(1000^3)
                else
                    window_finish+1
                end
                xstart = ((log_start_s-window_start)/(window_finish-window_start))*width
                xfinish = ((log_finish_s-window_start)/(window_finish-window_start))*width
                sethue(proc_color)
                rect(Point(xstart,ypos+(proc_height/3)+1),xfinish-xstart,proc_height-2,:fill)
                sethue("black")
                rect(Point(xstart,ypos+(proc_height/3)+1),xfinish-xstart,proc_height-2,:stroke)
            end
        end
        finish()
        @debug "Gantt: Finish"
        window_finish = (time_ns()-root_time)/(1000^3)
        if window_length !== nothing
            window_start = window_finish-window_length
        end
        image_idx += 1
    end
    if isdir(svg_path)
        final_paths = try
            combine_gantt_images(svg_path, prof_path, delay)
        catch err
            @error "Image-to-video failed" exception=err
            ("", "")
        end
        continue_rendering[] = true
        put!(render_results, final_paths)
    end
end

function show_gantt(ctx; delay=2, port=8000, width=1000, height=640, window_length=20, live=false)
    svg_prefix = "/tmp/dagger-gantt-$(String(rand(UInt8(97):UInt8(122), 16)))"
    prof_prefix = "/tmp/dagger-gantt-$(String(rand(UInt8(97):UInt8(122), 16)))"
    if live
        # Create SVG file
        # TODO: Ensure this gets cleaned up
        svg_path = svg_prefix*".svg"
        prof_path = prof_prefix*".svg"
        open(svg_path, "w") do io
            write(io, "<h2>Waiting for Gantt data...</h2>")
        end
        open(prof_path, "w") do io
            write(io, "<h2>Waiting for profile data...</h2>")
        end
    else
        svg_path = svg_prefix
        prof_path = prof_prefix
        mkpath(svg_path)
        mkpath(prof_path)
    end

    # Start drawing task
    Threads.@spawn begin
        try
            draw_gantt(ctx, svg_path, prof_path; delay=delay, width=width,
                       height=height, window_length=window_length)
        catch err
            Base.showerror(stderr, err)
            Base.show_backtrace(stderr, catch_backtrace())
            println(stderr)
        end
    end

    if live
        serve_gantt(svg_path, prof_path; port=port)
    end
end
