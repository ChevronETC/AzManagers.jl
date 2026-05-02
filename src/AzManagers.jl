module AzManagers

using AzSessions, Base64, CodecZlib, Dates, Distributed, HTTP, JSON, JWTs, LibCURL, LibGit2, Logging, Pkg, Printf, Random, Serialization, Sockets, TOML

include("errors.jl")
include("utilities.jl")
include("telemetry.jl")
include("events.jl")
include("types.jl")
include("templates.jl")
include("scaleset.jl")
include("worker.jl")
include("connections.jl")
include("progress.jl")
include("event_loop.jl")
include("detached_service.jl")
include("detached.jl")

export default_manager, DetachedJob, addproc, machine_preempt_channel_future, metrics, nphysical_cores, nworkers_provisioned, preempted, rmproc, scalesets, status, variablebundle, variablebundle!, vm, @detach, @detachat

if !isdefined(Base, :get_extension)
    include("../ext/MPIExt.jl")
end

end
