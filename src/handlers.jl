function on_scaleset_empty(state, scaleset) :: Vector{Action}
    actions = Action[]
    for (id, info) in get(state.instances, scaleset, Dict())
        if info.worker_pid > 0
            push!(actions, DeregisterWorker(info.worker_pid))
        end
    end
    push!(actions, DeleteScaleSet(scaleset))
    actions
end

function on_preempted(state::ManagerState, pid::Int, ss::ScaleSet, instanceid::Int) :: Vector{Action}
    [DeregisterWorker(pid), QueuePendingDown(ss, instanceid)]
end

function on_worker_exit(_::ManagerState, pid::Int) :: Vector{Action}
    [DeregisterWorker(pid)]
end

