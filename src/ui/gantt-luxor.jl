import .Luxor: Drawing, finish, Point, background, sethue, fontsize, rect, text

prof_to_svg(::Any, ::Any, ::Any; kwargs...) = nothing

combine_gantt_images(::Any, ::Any, ::Any, ::Any) = ("", "")

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
        update_window_logs!(window_logs, logs; root_time=root_time, window_start=window_start)
        isempty(window_logs) && continue

        # Concatenate and render profile data
        if ctx.profile
            prof_data, prof_lidict = logs_to_stackframes(window_logs)
            prof_to_svg(prof_path, prof_data, prof_lidict, image_idx; width=width)
        end

        for proc in unique(map(x->x[1].timeline[2], filter(x->x[1].category==:compute, window_logs)))
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
            for log in filter(x->x[1].timeline[2]==proc, filter(x->x[1].category==:compute, window_logs))
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
            for log in filter(x->x[1].category==:scheduler_init, window_logs)
                log_start_s = (log[1].timestamp-root_time)/(1000^3)
                log_finish_s = if length(log) == 2
                    log_finish_s = (log[2].timestamp-root_time)/(1000^3)
                else
                    window_finish+1
                end
                xstart = ((log_start_s-window_start)/(window_finish-window_start))*width
                xfinish = ((log_finish_s-window_start)/(window_finish-window_start))*width
                sethue("red")
                rect(Point(xstart,0),#=xfinish-xstart=#1,height)
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
            combine_gantt_images(ctx, svg_path, prof_path, delay)
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
            @error exception=(err,catch_backtrace())
        end
    end

    if live
        serve_gantt(svg_path, prof_path; port=port, delay=delay)
    end
end
