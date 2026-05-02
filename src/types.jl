struct ScaleSet
    subscriptionid
    resourcegroup
    scalesetname
    ScaleSet(subscriptionid, resourcegroup, scalesetname) = new(lowercase(subscriptionid), lowercase(resourcegroup), lowercase(scalesetname))
end
Base.Dict(scaleset::ScaleSet) = Dict("subscriptionid"=>scaleset.subscriptionid, "resourcegroup"=>scaleset.resourcegroup, "name"=>scaleset.scalesetname)

mutable struct AzManager <: ClusterManager
    session::AzSessionAbstract
    nretry::Int
    verbose::Int
    save_cloud_init_failures::Bool
    show_quota::Bool
    scalesets::Dict{ScaleSet,Int}
    pending_down::Dict{ScaleSet,Set{String}}
    deleted::Dict{ScaleSet,Dict{String,DateTime}}
    pruned::Dict{ScaleSet,Set{String}}
    preempted::Dict{ScaleSet,Set{String}}
    preempt_channel_futures::Dict{Int,Future}
    preempt_channel_ready::Dict{Int,Base.Event}
    port::UInt16
    server::Sockets.TCPServer
    worker_socket::TCPSocket
    task_accept::Task
    task_event_loop::Task
    timer_prune::Timer
    timer_clean::Timer
    timer_batch_flush::Timer
    lock::ReentrantLock
    scaleset_request_counter::Int
    ssh_user::String
    workers_changed::Threads.Condition
    events::Channel{ManagerEvent}
    socket_batch::Vector{TCPSocket}
    batch_max::Int
    pending_deletions::Vector{@NamedTuple{vmname::String, url::String, session::AzSessionAbstract, started::Float64}}

    AzManager() = new()
end

const _manager = AzManager()

function azmanager!(session, ssh_user, nretry, verbose, save_cloud_init_failures, show_quota)
    _manager.session = session
    _manager.nretry = nretry
    _manager.verbose = verbose
    _manager.save_cloud_init_failures = save_cloud_init_failures
    _manager.show_quota = show_quota
    _manager.ssh_user = ssh_user

    if isdefined(_manager, :events)
        return _manager
    end

    _manager.port,_manager.server = listenany(getipaddr(), 9000)
    _manager.pending_down = Dict{ScaleSet,Set{String}}()
    _manager.deleted = Dict{ScaleSet,Dict{String,DateTime}}()
    _manager.pruned = Dict{ScaleSet,Set{String}}()
    _manager.preempted = Dict{ScaleSet,Set{String}}()
    _manager.preempt_channel_futures = Dict{Int,Future}()
    _manager.preempt_channel_ready = Dict{Int,Base.Event}()
    _manager.scalesets = Dict{ScaleSet,Int}()
    _manager.lock = ReentrantLock()
    _manager.scaleset_request_counter = 0
    _manager.workers_changed = Threads.Condition()
    _manager.events = Channel{ManagerEvent}(256)
    _manager.socket_batch = TCPSocket[]
    _manager.batch_max = 64
    _manager.pending_deletions = @NamedTuple{vmname::String, url::String, session::AzSessionAbstract, started::Float64}[]

    _manager.task_accept = errormonitor(@async accept_connections(_manager))
    _manager.task_event_loop = errormonitor(@async run_event_loop(_manager))

    prune_interval = parse(Int, get(ENV, "JULIA_AZMANAGERS_PRUNE_POLL_INTERVAL", "120"))
    clean_interval = parse(Int, get(ENV, "JULIA_AZMANAGERS_CLEAN_POLL_INTERVAL", "60"))
    _manager.timer_prune = Timer(0.0; interval=prune_interval) do _
        try put!(_manager.events, PruneTick()) catch end
    end
    _manager.timer_clean = Timer(Float64(clean_interval); interval=clean_interval) do _
        try put!(_manager.events, CleanTick()) catch end
    end

    _manager
end

azmanager() = _manager

function __init__()
    if myid() == 1
        atexit() do
            manager = azmanager()
            if isdefined(manager, :events) && isopen(manager.events)
                try
                    put!(manager.events, ShutdownRequested())
                    # Give the event loop a moment to process shutdown
                    sleep(0.5)
                catch
                end
            else
                # Fallback if event loop isn't running
                try delete_scalesets() catch end
            end
        end
    end
end
