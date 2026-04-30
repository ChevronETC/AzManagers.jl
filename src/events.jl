abstract type ManagerEvent end

struct SocketAccepted <: ManagerEvent
    socket::TCPSocket
end

struct BatchFlushTick <: ManagerEvent end

struct PruneTick <: ManagerEvent end

struct CleanTick <: ManagerEvent end

struct WorkerPreempted <: ManagerEvent
    pid::Int
    instanceid::String
    notbefore::String
end

struct WorkerLost <: ManagerEvent
    pid::Int
end

struct WorkersChanged <: ManagerEvent
    count::Int
end

struct ShutdownRequested <: ManagerEvent end
