function run_event_loop(manager)
    for event in manager.events
        try
            handle(manager, event)
        catch e
            if e isa InvalidStateException && !isopen(manager.events)
                @debug "event loop shutting down"
                break
            end
            @error "event loop error handling $(typeof(event))" exception=(e, catch_backtrace())
        end
    end
end

# fallback
handle(manager, event::ManagerEvent) = @warn "unhandled event type: $(typeof(event))"

function handle(manager, ::PruneTick)
    try
        all_vms = list_all_scaleset_vms(manager)
        prune_cluster(all_vms)
        prune_scalesets(all_vms)
    catch e
        @debug "scaleset pruning error" exception=(e, catch_backtrace())
    end
end

function handle(manager, ::CleanTick)
    try
        delete_pending_down_vms()
        delete_empty_scalesets()
        scaleset_sync()
        all_vms = list_all_scaleset_vms(manager)
        prune_cluster(all_vms)
    catch e
        @debug "scaleset cleaning error" exception=(e, catch_backtrace())
    end

    check_pending_deletions(manager)
end

function handle(manager, event::DeletionStarted)
    if !isdefined(manager, :pending_deletions)
        manager.pending_deletions = @NamedTuple{vmname::String, url::String, session::AzSessionAbstract, started::Float64}[]
    end
    push!(manager.pending_deletions, (vmname=event.vmname, url=event.async_url, session=event.session, started=time()))
    @debug "tracking VM deletion" vmname=event.vmname
end

function check_pending_deletions(manager)
    !isdefined(manager, :pending_deletions) && return
    isempty(manager.pending_deletions) && return

    completed = Int[]
    for (i, entry) in enumerate(manager.pending_deletions)
        try
            r = HTTP.request("GET", entry.url,
                ["Authorization" => "Bearer $(token(entry.session))"];
                readtimeout=10, retry=false, redirect=false)
            body = JSON.parse(String(r.body))
            status = get(body, "status", "Unknown")

            if status == "Succeeded"
                @info "VM '$(entry.vmname)' deletion confirmed"
                push!(completed, i)
            elseif status == "Failed"
                @warn "VM '$(entry.vmname)' deletion reported failure" body
                push!(completed, i)
            else
                elapsed = time() - entry.started
                if elapsed > 600
                    @warn "VM '$(entry.vmname)' deletion still in progress after $(round(elapsed, digits=0))s"
                end
            end
        catch e
            # 404 means the operation completed and the resource is gone
            if e isa HTTP.StatusError && e.status == 404
                @info "VM '$(entry.vmname)' deletion confirmed (resource gone)"
                push!(completed, i)
            else
                @debug "error checking deletion status for $(entry.vmname)" exception=(e, catch_backtrace())
            end
        end
    end

    deleteat!(manager.pending_deletions, sort!(completed))
end

function handle(manager, event::SocketAccepted)
    push!(manager.socket_batch, event.socket)
    @debug "socket added to batch" batch_size=length(manager.socket_batch)

    # Flush immediately if batch is full
    if length(manager.socket_batch) >= manager.batch_max
        flush_socket_batch(manager)
        return
    end

    # Arm flush timer on first socket in a new batch
    if length(manager.socket_batch) == 1
        flush_delay = parse(Float64, get(ENV, "JULIA_AZMANAGERS_BATCH_FLUSH_DELAY",
            get(ENV, "JULIA_AZMANAGERS_PENDING_CADENCE", "5.0")))
        manager.timer_batch_flush = Timer(flush_delay) do _
            try put!(manager.events, BatchFlushTick()) catch end
        end
    end
end

function handle(manager, ::BatchFlushTick)
    flush_socket_batch(manager)
end

function handle(manager, event::WorkersChanged)
    lock(manager.workers_changed) do
        notify(manager.workers_changed)
    end
end

function handle(manager, event::WorkerPreempted)
    notbefore = DateTime(event.notbefore, dateformat"e, dd u yyyy HH:MM:SS \G\M\T")
    @info "caught preempt for pid=$(event.pid), removing not before $notbefore UTC"
    _now = now(UTC)
    if notbefore > _now
        @info "sleeping for $(notbefore - _now)"
        sleep(notbefore - _now)
    end

    if haskey(Distributed.map_pid_wrkr, event.pid)
        wrkr = Distributed.map_pid_wrkr[event.pid]
        if isdefined(wrkr, :config) && isdefined(wrkr.config, :userdata)
            u = wrkr.config.userdata
            try
                scaleset = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
                add_instance_to_preempted_list(manager, scaleset, u["instanceid"])
            catch e
                @info "error adding instance to preempted list"
            end
        end
    end

    deregister_worker_safe(event.pid)
end

function handle(manager, event::WorkerLost)
    if haskey(Distributed.map_pid_wrkr, event.pid)
        @warn "worker $(event.pid) lost unexpectedly (not a graceful preemption), deregistering"
    end
    deregister_worker_safe(event.pid)
end

function handle(manager, ::ShutdownRequested)
    @info "shutdown requested, cleaning up..."

    # Stop periodic timers
    isdefined(manager, :timer_prune) && close(manager.timer_prune)
    isdefined(manager, :timer_clean) && close(manager.timer_clean)
    isdefined(manager, :timer_batch_flush) && isopen(manager.timer_batch_flush) && close(manager.timer_batch_flush)

    # Close the server socket to stop accepting connections
    isdefined(manager, :server) && isopen(manager.server) && close(manager.server)

    # Delete all scalesets
    try
        delete_scalesets()
    catch e
        @error "error during shutdown delete_scalesets" exception=(e, catch_backtrace())
    end

    # Close the event channel to terminate the event loop
    close(manager.events)
end
