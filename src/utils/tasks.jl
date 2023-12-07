function set_task_tid!(task::Task, tid::Integer)
    task.sticky = true
    ctr = 0
    while true
        ret = ccall(:jl_set_task_tid, Cint, (Any, Cint), task, tid-1)
        if ret == 1
            break
        elseif ret == 0
            yield()
        else
            error("Unexpected retcode from jl_set_task_tid: $ret")
        end
        ctr += 1
        if ctr > 10
            @warn "Setting task TID to $tid failed, giving up!"
            return
        end
    end
    @assert Threads.threadid(task) == tid "jl_set_task_tid failed!"
end
