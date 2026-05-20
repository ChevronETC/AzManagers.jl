struct ScaleSet
    subscriptionid::String
    resourcegroup::String
    name::String
    ScaleSet(sub, rg, name) = new(lowercase(sub), lowercase(rg), lowercase(name))
end

@enum InstanceState begin
    IN_FLIGHT
    ACTIVE
    PENDING_REIMAGE
    REIMAGED
    PENDING_DOWN
    PREEMPTED
end

struct InstanceInfo
    state::InstanceState
    first_seen::DateTime
    worker_pid::Int
end

mutable struct ManagerState
    # --- auth & config ---
    session::AzSessionAbstract
    ssh_user::String
    nretry::Int
    verbose::Int
    save_cloud_init_failures::Bool
    show_quota::Bool

    # --- cluster state ---
    scalesets::Dict{ScaleSet,Int}
    instances::Dict{ScaleSet,Dict{Int,InstanceInfo}}

    # --- connection pipeline ---
    server::Sockets.TCPServer
    port::UInt16
    pending_validated::Channel{WorkerConfig}

    # --- spot ---
    preempt_channels::Dict{Int,Future}

    lock::ReentrantLock
end

abstract type Action end

struct Reimage <: Action
    scaleset::ScaleSet
    instanceids::Vector{Int}
end

struct DeleteInstances <: Action
    scaleset::ScaleSet
    instanceids::Vector{Int}
end

struct DeleteScaleSet <: Action
    scaleset::ScaleSet
end

struct DeregisterWorker <: Action
    pid::Int
end

struct QueuePendingDown <: Action
    scaleset::ScaleSet
    instanceid::Int
end

struct SyncCapacity <: Action
    scaleset::ScaleSet
    new_capacity::Int
end

struct RegisterWorkers <: Action
    wconfigs::Vector{WorkerConfig}
end

struct NoAction <: Action end

