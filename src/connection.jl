# ─── Connection validation ───

const VALIDATION_TIMEOUT = parse(Float64, get(ENV, "AZMANAGERS_VALIDATION_TIMEOUT", "30.0"))

function validate_connection(state::ManagerState, socket)
    tsk = @async _read_worker_config(socket)

    watchdog = Timer(VALIDATION_TIMEOUT) do _
        istaskdone(tsk) || @async Base.throwto(tsk, InterruptException())
    end

    try
        return fetch(tsk)
    catch e
        peer = try string(getpeername(socket)) catch; "unknown" end
        @warn "connection validation failed, discarding socket" peer exception=(e, catch_backtrace())
        close(socket)
        return nothing
    finally
        close(watchdog)
    end
end

function _read_worker_config(socket)
    _cookie = read(socket, Distributed.HDR_COOKIE_LEN)
    String(_cookie) == Distributed.cluster_cookie() || error("Invalid cookie from remote worker")

    connection_string = String(base64decode(readline(socket)))
    vm = JSON.parse(connection_string)

    wconfig = WorkerConfig()
    wconfig.io = socket
    wconfig.bind_addr = vm["bind_addr"]
    wconfig.count = vm["ppi"]
    wconfig.exename = "julia"
    wconfig.exeflags = `$(vm["exeflags"])`
    wconfig.userdata = vm["userdata"]
    wconfig
end

# ─── Accept loop ───
# Pure IO — accepts sockets, validates, feeds the channel.
# No Actions returned; this is the boundary between the network and the event system.

function accept_loop(state::ManagerState)
    while true
        try
            socket = accept(state.server)
            @async try
                wconfig = validate_connection(state, socket)
                if wconfig !== nothing
                    u = wconfig.userdata
                    ss = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
                    instanceid = u["instanceid"]

                    lock(state.lock) do
                        instances = get!(state.instances, ss, Dict{Int,InstanceInfo}())
                        instances[instanceid] = InstanceInfo(IN_FLIGHT, now(Dates.UTC), 0)
                    end

                    put!(state.pending_validated, wconfig)
                    @debug "validated connection queued" instanceid
                end
            catch e
                peer = try string(getpeername(socket)) catch; "unknown" end
                @warn "connection validation error" peer
                @debug "connection validation error" exception=(e, catch_backtrace())
            end
        catch e
            @error "accept_loop error" exception=(e, catch_backtrace())
        end
    end
end

# ─── Batch loop ───
# Drains pending_validated channel, batches by rate/size, then executes RegisterWorkers.

const MAX_BATCH_SIZE = parse(Int, get(ENV, "AZMANAGERS_MAX_BATCH_SIZE", "64"))
const MIN_INSTANCES_PER_SECOND = parse(Float64, get(ENV, "AZMANAGERS_MIN_INSTANCES_PER_MINUTE", "10")) / 60
const MIN_CADENCE = parse(Float64, get(ENV, "AZMANAGERS_PENDING_CADENCE", "5"))

function batch_loop(state::ManagerState)
    wconfigs = WorkerConfig[]
    tic = time()

    while true
        try
            # Block on first item, then drain non-blocking
            if isempty(wconfigs)
                push!(wconfigs, take!(state.pending_validated))
                tic = time()
            elseif isready(state.pending_validated) && length(wconfigs) < MAX_BATCH_SIZE
                push!(wconfigs, take!(state.pending_validated))
            else
                sleep(0.1)
            end

            elapsed = time() - tic
            rate = length(wconfigs) / max(elapsed, 0.001)

            # Keep collecting if arriving fast and under max
            if elapsed < MIN_CADENCE && rate > MIN_INSTANCES_PER_SECOND && length(wconfigs) < MAX_BATCH_SIZE
                continue
            end
        catch e
            @error "batch_loop: error draining channel" exception=(e, catch_backtrace())
            continue
        end

        @info "batch ready" batch_size=length(wconfigs) elapsed=round(time() - tic, digits=1)

        batch = copy(wconfigs)
        empty!(wconfigs)
        tic = time()

        execute!(state, RegisterWorkers(batch))
    end
end

# ─── Post-registration ───

function after_registered(state::ManagerState, pids::Vector{Int}, wconfigs::Vector{WorkerConfig})
    lock(state.lock) do
        for (pid, wconfig) in zip(pids, wconfigs)
            u = wconfig.userdata
            ss = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
            instanceid = u["instanceid"]

            if haskey(state.instances, ss) && haskey(state.instances[ss], instanceid)
                state.instances[ss][instanceid] = InstanceInfo(ACTIVE, state.instances[ss][instanceid].first_seen, pid)
            end

            # Start preempt watch for spot instances
            if lowercase(get(u, "priority", "")) == "spot"
                @async spot_preempt_watch(state, pid, ss, instanceid)
            end
        end
    end
end

# ─── Preemption watch ───

function spot_preempt_watch(state::ManagerState, pid::Int, ss::ScaleSet, instanceid::Int)
    try
        ch = remotecall(Channel{Bool}, pid, 1)
        state.preempt_channels[pid] = ch
        remotecall_fetch(machine_preempt_loop, pid, ch)
    catch e
        if isa(e, RemoteException) && isa(e.captured.ex, TaskFailedException)
            inner = e.captured.ex.task.result.ex
            if isa(inner, SpotPreemptException)
                notbefore = DateTime(inner.notbefore, dateformat"e, dd u yyyy HH:MM:SS \G\M\T")
                _now = now(Dates.UTC)
                notbefore > _now && sleep(notbefore - _now)

                actions = on_preempted(state, pid, ss, instanceid)
                execute!(state, actions)
                return
            end
        end
        @warn "spot_preempt_watch failed" pid exception=(e, catch_backtrace())
    end
end
