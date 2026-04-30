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
