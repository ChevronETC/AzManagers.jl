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
    pending_up::Channel{TCPSocket}
    pending_down::Dict{ScaleSet,Set{String}}
    deleted::Dict{ScaleSet,Dict{String,DateTime}}
    pruned::Dict{ScaleSet,Set{String}}
    preempted::Dict{ScaleSet,Set{String}}
    preempt_channel_futures::Dict{Int,Future}
    port::UInt16
    server::Sockets.TCPServer
    worker_socket::TCPSocket
    task_add::Task
    task_process::Task
    lock::ReentrantLock
    scaleset_request_counter::Int
    ssh_user::String
    workers_changed::Threads.Condition

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

    if isdefined(_manager, :pending_up)
        return _manager
    end

    _manager.port,_manager.server = listenany(getipaddr(), 9000)
    _manager.pending_up = Channel{TCPSocket}(64)
    _manager.pending_down = Dict{ScaleSet,Set{String}}()
    _manager.deleted = Dict{ScaleSet,Dict{String,DateTime}}()
    _manager.pruned = Dict{ScaleSet,Set{String}}()
    _manager.preempted = Dict{ScaleSet,Set{String}}()
    _manager.preempt_channel_futures = Dict{Int,Future}()
    _manager.scalesets = Dict{ScaleSet,Int}()
    _manager.task_add = @async add_pending_connections()
    _manager.task_process = @async process_pending_connections()
    _manager.lock = ReentrantLock()
    _manager.scaleset_request_counter = 0
    _manager.workers_changed = Threads.Condition()

    @async scaleset_pruning()
    @async scaleset_cleaning()

    _manager
end

azmanager() = _manager

function __init__()
    if myid() == 1
        atexit(AzManagers.delete_scalesets)
    end
end
