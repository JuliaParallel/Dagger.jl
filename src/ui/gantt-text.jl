function draw_gantt_text(ctx; delay=2, width=40, height=20, window_length=20)
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

    while continue_rendering[]
        sleep(delay)
        logs = Dagger.get_logs!(ctx.log_sink, true) # get raw events
        update_window_logs!(window_logs, logs; root_time=root_time, window_start=window_start)

        #= TODO: Concatenate and render profile data
        if ctx.profile
            prof_data, prof_lidict = logs_to_stackframes(window_logs)
            _prof_to_svg(prof_path, prof_data, prof_lidict, image_idx; width=width)
        end
        =#

        for proc in unique(map(x->x[1].timeline[2], filter(x->x[1].category==:compute, window_logs)))
            push!(procs, proc)
        end
        procs_len = length(procs)
        print("\e[2J\e[1;1H")
        for (proc_idx, proc) in enumerate(sort(collect(procs); lt=proclt))
            proc_row = ((proc_idx-1)*2)+1
            print("\e[$(proc_row)H")
            print("$(getname(proc)), $(window_start) s - $(window_finish) s")
            for log in filter(x->x[1].timeline[2]==proc, filter(x->x[1].category==:compute, window_logs))
                log_start_s = (log[1].timestamp-root_time)/(1000^3)
                log_finish_s = if length(log) == 2
                    log_finish_s = (log[2].timestamp-root_time)/(1000^3)
                else
                    window_length+window_start
                end
                log_start_s > (window_length+window_start) && continue # skip premature entries
                log_finish_s < window_start && continue # skip finished entries
                xstart = floor(Int, ((log_start_s-window_start)/(window_finish-window_start))*width)
                xfinish = ceil(Int, ((log_finish_s-window_start)/(window_finish-window_start))*width)
                xstart = max(xstart, 1)
                xfinish = min(xfinish, width)
                print("\e[$(proc_row+1);$(xstart)H")
                print(repeat("X", xfinish-xstart))
            end
            println()
        end
        #= TODO: Scheduler init
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
        =#
        window_finish = (time_ns()-root_time)/(1000^3)
        if window_length !== nothing
            window_start = window_finish-window_length
        end
    end
end
