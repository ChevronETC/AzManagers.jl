function reconcile(state::ManagerState) :: Vector{Action}
    actions = Action[]

    # --- Fetch Azure state (one API round per scaleset) ---
    vm_lists = Dict{ScaleSet,Vector}()
    nic_maps = Dict{ScaleSet,Dict{String,String}}()

    for scaleset in keys(state.scalesets)
        vm_lists[scaleset] = list_scaleset_vms(state, scaleset)
        nic_maps[scaleset] = list_scaleset_nics(state, scaleset)
    end

    # --- Pass 1: ghost workers (registered in Distributed, VM gone)
    append!(actions, find_ghost_workers(state, vm_lists))

    # --- Pass 2: stuck/failed VMs (VM exists, never joined or failed)
    append!(actions, find_stuck_vms(state, vm_lists, nic_maps))

    # --- Pass 3: empty scalesets
    append!(actions, find_empty_scalesets(state))

    # --- Pass 4: capacity drift
    append!(actions, find_capacity_drift(state))

    actions

end

function find_ghost_workers(state, vm_lists) :: Vector{Action}
    actions = Action[]

    # collect all vm names currently alive in azure
    live_vms = Set{String}()
    for (scaleset, vms) in vm_lists
        for vm in vms
            prov_state = lowercase(get(get(vm, "properties", Dict()), "provisioningState", "none"))
            if prov_state in ("creating", "updating", "succeeded")
                push!(live_vms, vm["name"])
            end
        end
    end

    # any registered worker not backed by a live VM -> deregistered
    for (pid, wrkr) in Distributed.map_pid_wrkr
        pid == 1 && continue
        wrkr.state in (Distributed.W_TERMINATED, Distributed.W_TERMINATING) && continue
        name = get(get(wrkr.config, :userdata, Dict()), "name", nothing)
        name === nothing && continue
        if !(name in live_vms)
            push!(actions, DeregisterWorker(pid))
        end
    end

    actions
end

function find_stuck_vms(state, vm_lists, nic_maps) :: Vector{Action}
    actions = Action[]
    join_timeout = Second(parse(Int, get(ENV, "AZMANAGERS_VM_JOIN_TIMEOUT", "120")))

    for (scaleset, vms) in vm_lists
        instances = get(state.instances, scaleset, Dict())

        for vm in vms
            instanceid = split(vm["id"], '/')[end]
            info = get(instances, instanceid, nothing)

            # skip instances we're already tracking for action
            info !== nothing && info.state in (PENDING_DOWN, PENDING_REIMAGE) && continue

            # skip active workers
            info !== nothing && info.state == ACTIVE && continue

            vm_state = lowercase(get(get(vm, "properties", Dict()), "provisioningState", "none"))
            nic_state = get(get(nic_maps, scaleset, Dict()), instanceid, "unknown")
            failed = vm_state == "failed" || nic_state == "failed"
            was_reimaged = info !== nothing && info.state == REIMAGED

            first_seen = info !== nothing ? info.first_seen : now(Dates.UTC)
            age = now(Dates.UTC) - first_seen

            if failed && !was_reimaged
                # first strike -> reimage
                push!(actions, Reimage(scaleset, [instanceid]))
            elseif (age > join_timeout) || (failed && was_reimaged)
                # timed out or second failure -> delete
                push!(actions, QueuePendingDown(scaleset, instanceid))
                push!(actions, DeleteInstances(scaleset, [instanceid]))
            end
        end
    end

    actions
end

function find_empty_scalesets(state) :: Vector{Action}
    actions = Action[]
    for (scaleset, capacity) in state.scalesets
        if capacity == 0
            append!(actions, on_scaleset_empty(state, scaleset))
        end
    end

    actions
end

function find_capacity_drift(state) :: Vector{Action}
    actions = Action[]
    for scaleset in keys(state.scalesets)
        server_capacity = scaleset_capacity(state, scaleset)
        if state.scalesets[scaleset] != server_capacity
            push!(actions, SyncCapacity(scaleset, server_capacity))
        end
    end

    actions
end

# ─── Reconcile timer ───

const RECONCILE_INTERVAL = parse(Float64, get(ENV, "AZMANAGERS_RECONCILE_INTERVAL", "30.0"))

function reconcile_timer(state::ManagerState)
    while true
        try
            cluster_status(state)
            actions = reconcile(state)
            if !isempty(actions)
                @info "reconcile produced actions" count=length(actions)
                execute!(state, actions)
            end
        catch e
            @error "reconcile_timer error" exception=(e, catch_backtrace())
        end
        sleep(RECONCILE_INTERVAL)
    end
end
