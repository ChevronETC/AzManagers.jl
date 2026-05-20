# ─── Error formatting ───

function logerror(e, loglevel=Logging.Info)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")
    for (exc, bt) in current_exceptions()
        showerror(io, exc, bt)
        println(io)
    end
    @logmsg loglevel String(take!(io))
    close(io)
end

# ─── Action descriptions ───

action_desc(a::Reimage)          = "Reimage $(length(a.instanceids)) VMs on $(a.scaleset.name)"
action_desc(a::DeleteInstances)  = "Delete $(length(a.instanceids)) VMs from $(a.scaleset.name)"
action_desc(a::DeleteScaleSet)   = "Delete scaleset $(a.scaleset.name)"
action_desc(a::DeregisterWorker) = "Deregister worker pid=$(a.pid)"
action_desc(a::QueuePendingDown) = "Queue pending_down $(a.scaleset.name)/$(a.instanceid)"
action_desc(a::SyncCapacity)     = "Sync capacity $(a.scaleset.name) → $(a.new_capacity)"
action_desc(a::RegisterWorkers)  = "Register $(length(a.wconfigs)) workers"
action_desc(::NoAction)          = "NoAction"

# ─── Cluster snapshot ───

function cluster_status(state::ManagerState)
    counts = Dict{InstanceState,Int}()
    for (_, instances) in state.instances
        for (_, info) in instances
            counts[info.state] = get(counts, info.state, 0) + 1
        end
    end

    total_capacity = sum(values(state.scalesets); init=0)
    distributed_workers = nprocs() - 1

    @info "cluster status" scalesets=length(state.scalesets) capacity=total_capacity distributed_workers in_flight=get(counts, IN_FLIGHT, 0) active=get(counts, ACTIVE, 0) pending_reimage=get(counts, PENDING_REIMAGE, 0) reimaged=get(counts, REIMAGED, 0) pending_down=get(counts, PENDING_DOWN, 0) preempted=get(counts, PREEMPTED, 0)
end

# ─── Remote worker logging setup ───

function logging(; level=Logging.Info)
    logger = Logging.SimpleLogger(stderr, level)
    Logging.global_logger(logger)
    nothing
end