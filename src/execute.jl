function execute!(state::ManagerState, action::Reimage)
    reimage_vms(state, action.scaleset, action.instanceids)
    for id in action.instanceids
        state.instances[action.scaleset][id] = InstanceInfo(REIMAGED, now(Dates.UTC), 0)
    end
end

function execute!(state::ManagerState, action::DeleteInstances)
    delete_vms(state, action.scaleset, action.instanceids)
    for id in action.instanceids
        delete!(state.instances[action.scaleset], id)
    end
    state.scalesets[action.scaleset] = max(0, state.scalesets[action.scaleset] - length(action.instanceids))
end

function execute!(state::ManagerState, action::DeleteScaleSet)
    rmgroup(state, action.scaleset)
    delete!(state.scalesets, action.scaleset)
    delete!(state.instances, action.scaleset)
end

function execute!(state::ManagerState, action::DeregisterWorker)
    Distributed.set_worker_state(Distributed.map_pid_wrkr[action.pid], Distributed.W_TERMINATED)
    Distributed.deregister_worker(action.pid)
end

function execute!(state::ManagerState, action::QueuePendingDown)
    info = get(get(state.instances, action.scaleset, Dict()), action.instanceid, nothing)
    if info !== nothing
        state.instances[action.scaleset][action.instanceid] = InstanceInfo(PENDING_DOWN, info.first_seen, info.worker_pid)
    end
end

function execute!(state::ManagerState, action::SyncCapacity)
    state.scalesets[action.scaleset] = action.new_capacity
end

function execute!(state::ManagerState, action::RegisterWorkers)
    addprocs(azmanager(); wconfigs=action.wconfigs)
end

function execute!(state::ManagerState, action::NoAction)
    nothing
end

function execute!(state::ManagerState, actions::Vector{<:Action})
    for action in actions
        try
            @info "executing" action=action_desc(action)
            execute!(state, action)
        catch e
            @error "action failed" action=action_desc(action)
            logerror(e, Logging.Debug)
        end
    end
end
