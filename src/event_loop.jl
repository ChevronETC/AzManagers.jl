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
        prune_cluster()
        prune_scalesets()
    catch e
        @debug "scaleset pruning error" exception=(e, catch_backtrace())
    end
end

function handle(manager, ::CleanTick)
    try
        delete_pending_down_vms()
        delete_empty_scalesets()
        scaleset_sync()
        prune_cluster()
    catch e
        @debug "scaleset cleaning error" exception=(e, catch_backtrace())
    end
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
