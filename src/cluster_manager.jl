struct ScaleSet
    subscriptionid
    resourcegroup
    scalesetname

    ScaleSet(subscriptionid, resourcegroup, scalesetname) = new(
        lowercase(subscriptionid),
        lowercase(resourcegroup),
        lowercase(scalesetname))
end

Base.Dict(scaleset::ScaleSet) = Dict(
    "subscriptionid" => scaleset.subscriptionid,
    "resourcegroup" => scaleset.resourcegroup,
    "name" => scaleset.scalesetname)

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

    _manager.port, _manager.server = listenany(getipaddr(), 9000)
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

function scaleset_pruning()
    interval = parse(Int, get(ENV, "JULIA_AZMANAGERS_PRUNE_POLL_INTERVAL", "600"))

    while true
        try
            #=
            The following seems required for an over-provisioned scaleset. it
            is not clear why this is needed.
            =#
            prune_cluster()
            #=
            The following handles vms that are provisioined, but that fail to
            join the cluster.
            =#
            prune_scalesets()
        catch e
            @error "scaleset pruning error"
            logerror(e, Logging.Debug)
        finally
            sleep(interval)
        end
    end
end

function scaleset_cleaning()
    interval = parse(Int, get(ENV, "JULIA_AZMANAGERS_CLEAN_POLL_INTERVAL", "60"))

    while true
        try
            sleep(interval)
            delete_pending_down_vms()
            delete_empty_scalesets()
            scaleset_sync()
        catch e
            @error "scaleset cleaning error"
            logerror(e, Logging.Debug)
        end
    end
end

scalesets(manager::AzManager) = isdefined(manager, :scalesets) ? manager.scalesets : Dict{ScaleSet,Int}()
scalesets() = scalesets(azmanager())
pending_down(manager::AzManager) = isdefined(manager, :pending_down) ? manager.pending_down : Dict{ScaleSet,Set{String}}()
pending_down(manager::AzManager, scaleset::ScaleSet) = get(pending_down(manager), scaleset, Set{String}())

function delete_scaleset(manager, scaleset)
    @debug "deleting scaleset, $scaleset"
    try
        rmgroup(manager, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, manager.nretry, manager.verbose, manager.show_quota)
    catch e
        @warn "unable to remove scaleset $(scaleset.resourcegroup), $(scaleset.scalesetname)"
    end
    delete!(scalesets(manager), scaleset)
end

function delete_empty_scalesets()
    manager = azmanager()
    lock(manager.lock)
    try
        _scalesets = scalesets(manager)
        for (scaleset, capacity) in _scalesets
            if capacity == 0
                # double-check capacity in case there is client/server mis-match
                _scalesets[scaleset] = scaleset_capacity(manager, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, manager.nretry, manager.verbose)
            end
            if _scalesets[scaleset] == 0
                delete_scaleset(manager, scaleset)
            end
        end
    catch e
        throw(e)
    finally
        unlock(manager.lock)
    end
end

function delete_pending_down_vms()
    manager = azmanager()
    lock(manager.lock)

    for (scaleset, ids) in pending_down(manager)
        @debug "deleting pending down vms $ids in $scaleset"
        try
            delete_vms(manager, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, ids, manager.nretry, manager.verbose)
            new_capacity = max(0, scalesets(manager)[scaleset] - length(ids))
            scalesets(manager)[scaleset] = new_capacity
            @debug "new scaleset capacity for $scaleset is $new_capacity"
            delete!(pending_down(manager), scaleset)
        catch e
            if status(e) == 404
                @debug "scaleset $(scaleset.scalesetname) not found when attempting to delete vms, assuming it was already deleted."
                # the resource is already deleted, nothing else to do except empty the pending down list for this scaleset
                if haskey(pending_down(manager), scaleset)
                    delete!(pending_down(manager), scaleset)
                end
            else
                @error "error deleting scaleset vms, manual clean-up may be required."
                logerror(e, Logging.Debug)
            end
        end
    end
    unlock(manager.lock)
    nothing
end

# sync server and client side views of the resources
function scaleset_sync()
    manager = azmanager()
    lock(manager.lock)
    try
        _pending_down = pending_down(manager)
        pending_down_count = isempty(_pending_down) ? 0 : mapreduce(length, +, values(_pending_down))
        if nprocs()-1+pending_down_count != nworkers_provisioned()
            @debug "client/server scaleset book-keeping mismatch, synching client to server."
            _scalesets = scalesets(manager)
            for scaleset in keys(_scalesets)
                _scalesets[scaleset] = scaleset_capacity(manager, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, manager.nretry, manager.verbose)
            end
        end
    catch e
        @error "scaleset syncing error"
        logerror(e, Logging.Debug)
    finally
        unlock(manager.lock)
    end
end

function prune_cluster()
    manager = azmanager()

    # list of workers registered with Distributed.jl
    wrkrs = Dict{Int,Dict}()
    for (pid,wrkr) in Distributed.map_pid_wrkr
        if pid != 1 && isdefined(wrkr, :id) && isdefined(wrkr, :config) && isa(wrkr, Distributed.Worker)
            if isdefined(wrkr.config, :userdata) && isa(wrkr.config.userdata, Dict)
                wrkrs[pid] = wrkr.config.userdata
            end
        end
    end

    # remove from list workers that have a corresponding scale-set vm instance.  What remains can be deleted from the cluster.
    _scalesets = scalesets(manager)
    for scaleset in keys(_scalesets)
        vms = list_scaleset_vms(manager, scaleset)

        vm_names = String[]
        for vm in vms
            status = get(get(vm, "properties", Dict()), "provisioningState", "none")
            if lowercase(status) ∈ ("creating", "updating", "succeeded")
                push!(vm_names, vm["name"])
            end
        end

        for (id,wrkr) in wrkrs
            _scaleset = ScaleSet(get(wrkr, "subscriptionid", ""), get(wrkr, "resourcegroup", ""), get(wrkr, "scalesetname", ""))
            if _scaleset == scaleset && get(wrkr, "name", "") ∈ vm_names
                delete!(wrkrs, id)
            end
        end
    end

    # remove from list workers that are already scheduled for removal from the cluster via the pending_down set
    for (scaleset, instanceids) in pending_down(manager)
        for (pid, wrkr) in wrkrs
            _scaleset = ScaleSet(get(wrkr, "subscriptionid", ""), get(wrkr, "resourcegroup", ""), get(wrkr, "scalesetname", ""))
            if _scaleset == scaleset && get(wrkr, "instanceid", "") ∈ instanceids
                delete!(wrkrs, pid)
            end
        end
    end

    # remove from list workers that are in TERMINATED or TERMINATING cluster state
    for (id,wrkr) in Distributed.map_pid_wrkr
        if isdefined(wrkr, :state) && wrkr.state ∈ (Distributed.W_TERMINATED, Distributed.W_TERMINATING)
            delete!(wrkrs, id)
        end
    end

    # remove workers that do not have a corresponding scale-set vm instance
    for pid in keys(wrkrs)
        @info "Removing worker $pid from the Julia cluster since it is no longer in the scaleset." wrkrs[pid]
        # We can't use rmprocs here since the worker process is gone.  The worker process would usually do the
        # following two lines (see the Distributed.message_handler_loop function)
        Distributed.set_worker_state(Distributed.map_pid_wrkr[pid], Distributed.W_TERMINATED)
        Distributed.deregister_worker(pid)
    end
end

function prune_scalesets()
    worker_timeout = Second(parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "720")))
    manager = azmanager()

    _scalesets = scalesets(manager)

    # list of workers registered with Distributed.jl, organized by scale-set
    instanceids = Dict{ScaleSet,Array{String}}()
    for wrkr in values(Distributed.map_pid_wrkr)
        if isdefined(wrkr, :id) && isdefined(wrkr, :config) && isa(wrkr, Distributed.Worker)
            if isdefined(wrkr.config, :userdata) && isa(wrkr.config.userdata, Dict)
                userdata = wrkr.config.userdata
                if haskey(userdata, "instanceid") && haskey(userdata, "scalesetname") && haskey(userdata, "resourcegroup") && haskey(userdata, "subscriptionid")
                    ss = ScaleSet(userdata["subscriptionid"], userdata["resourcegroup"], userdata["scalesetname"])
                    if haskey(instanceids, ss)
                        push!(instanceids[ss], userdata["instanceid"])
                    else
                        instanceids[ss] = [userdata["instanceid"]]
                    end
                end
            end
        end
    end

    for scaleset in keys(_scalesets)
        # update scale-set instances
        _vms = list_scaleset_vms(manager, scaleset)

        for _vm in _vms
            instanceid = split(_vm["id"],'/')[end]

            # if the instanceid corresponds to a registered worker, do nothing
            if scaleset ∈ keys(instanceids) && instanceid ∈ instanceids[scaleset]
                continue
            end

            # otherwise, decide if we should remove the instance from the scale-set
            time_touched = get(get(manager.deleted, scaleset, Dict()), instanceid, DateTime(_vm["properties"]["timeCreated"][1:23], DateFormat("yyyy-mm-ddTHH:MM:SS.s")))
            time_elapsed = now(Dates.UTC) - time_touched
            vm_state = lowercase(get(get(_vm, "properties", Dict()), "provisioningState", "none"))
            is_worker_deleting = scaleset ∈ keys(manager.pending_down) && instanceid ∈ manager.pending_down[scaleset]
            is_vm_deleting = lowercase(vm_state) == "deleting"
            ispruned_already = scaleset ∈ keys(manager.pruned) && instanceid ∈ manager.pruned[scaleset]

            doprune = time_elapsed > worker_timeout && !is_worker_deleting && !is_vm_deleting && !ispruned_already
            if doprune
                @info "Putting machine with instance id $instanceid in $(scaleset.scalesetname) onto the deletion queue because it failed to join the Julia cluster after $(round(time_elapsed, Second)), vm_state=$vm_state."
                if manager.save_cloud_init_failures
                    @info "copying cloud init output log to '$(pwd())/cloud-init-output-$(instanceid).log'."
                    try
                        ipaddress = get_ipaddress_for_scaleset_vm(manager, _vm)
                        run(`scp -i $(homedir())/.ssh/azmanagers_rsa $(manager.ssh_user)@$(ipaddress):/var/log/cloud-init-output.log ./cloud-init-output-$(instanceid).log`)
                    catch e
                        @warn "failed to copy cloud init log from VM $(instanceid)."
                        logerror(e, Logging.Debug)
                    end
                end
                add_instance_to_pruned_list(manager, scaleset, instanceid)
                add_instance_to_pending_down_list(manager, scaleset, instanceid)
            end
        end
    end
end

function delete_scalesets()
    manager = azmanager()
    @sync for scaleset in keys(scalesets(manager))
        @async rmgroup(manager, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, manager.nretry, manager.verbose, manager.show_quota)
    end
end

function add_pending_connections()
    manager = azmanager()
    while true
        try
            let s = accept(manager.server)
                @debug "pushing new socket onto manger.pending_up" manager.pending_up.n_avail_items
                put!(manager.pending_up, s)
                @debug "done pushing new socket onto manger.pending_up" manager.pending_up.n_avail_items
            end
        catch e
            @error "AzManagers, error adding pending connection"
            logerror(e, Logging.Debug)
        end
    end
end

function Distributed.addprocs(manager::AzManager; sockets)
    pids = []
    try
        Distributed.init_multi()
        Distributed.cluster_mgmt_from_master_check()
        lock(Distributed.worker_lock)
        pids = Distributed.addprocs_locked(manager; sockets)
    catch e
        @debug "AzManagers, error processing pending connection"
        logerror(e, Logging.Debug)
    finally
        unlock(Distributed.worker_lock)
    end
    pids
end

function addprocs_with_timeout(manager; sockets)
    # Distributed.setup_launched_worker also uses Distributed.worker_timeout, so we add a grace period
    # to allow for the Distributed.setup_launched_worker to hit its timeout.
    timeout = Distributed.worker_timeout() + 30
    tsk_addprocs = @async addprocs(manager; sockets)
    tic = time()
    pids = []
    interrupted = false
    while true
        if time() - tic > timeout && !interrupted
            @warn "AzManagers, interrupting addprocs due to a timeout"
            @async Base.throwto(tsk_addprocs, InterruptException())
            interrupted = true
        end
        if istaskdone(tsk_addprocs) && istaskfailed(tsk_addprocs)
            @warn "AzManagers, failed to process pending connections"
            try
                fetch(tsk_addprocs)
            catch e
                logerror(e, Logging.Debug)
            finally
                break
            end
        end
        if istaskdone(tsk_addprocs) && !istaskfailed(tsk_addprocs)
            pids = fetch(tsk_addprocs)
            break
        end
        sleep(1)
    end
    pids
end

function process_pending_connections()
    manager = azmanager()
    sockets = TCPSocket[]

    max_sockets = manager.pending_up.sz_max
    min_instances_per_second = parse(Float64, get(ENV, "JULIA_AZMANAGERS_MIN_INSTANCES_PER_MINUTE", "10")) / 60 # if we drop below N new instances per minute, then we trigger addprocs
    min_cadence = parse(Int, get(ENV, "JULIA_AZMANAGERS_PENDING_CADENCE", "60"))
    tic = time()
    while true
        try
            if isempty(sockets)
                @debug "taking from manager.pending_up" manager.pending_up.n_avail_items
                push!(sockets, take!(manager.pending_up))
                @debug "done taking from manager.pending_up" manager.pending_up.n_avail_items length(sockets)
            elseif isready(manager.pending_up) && length(sockets) < max_sockets
                @debug "taking from manager.pending_up" manager.pending_up.n_avail_items
                push!(sockets, take!(manager.pending_up))
                @debug "done taking from manager.pending_up" manager.pending_up.n_avail_items length(sockets)
            else
                sleep(0.1)
            end

            elapsedtime = time() - tic
            instances_per_second = length(sockets)/elapsedtime
            if length(sockets) == 0 || (elapsedtime < min_cadence && instances_per_second > min_instances_per_second && length(sockets) < max_sockets)
                continue
            else
                @debug "triggered adding machines" elapsedtime min_cadence instances_per_second min_instances_per_second length(sockets) max_sockets nworkers_provisioned()
            end
        catch e
            @error "AzManagers, error retrieving pending connection"
            logerror(e, Logging.Debug)
            continue
        end

        @debug "calling addprocs_with_timeout from process_pending_connections"
        pids = addprocs_with_timeout(manager; sockets)
        @debug "done calling addprocs_with_timeout from process_pending_connections"
        empty!(sockets)
        tic = time()

        @debug "starting preempt loops" pids
        for pid in pids
            @async begin
                wrkr = Distributed.map_pid_wrkr[pid]
                if isdefined(wrkr, :config) && isdefined(wrkr.config, :userdata) && lowercase(get(wrkr.config.userdata, "priority", "")) == "spot"
                    try
                        manager.preempt_channel_futures[pid] = remotecall(Channel{Bool}, pid, 1)
                        remotecall_fetch(machine_preempt_loop, pid, manager.preempt_channel_futures[pid])
                    catch e
                        if isa(e, RemoteException) && isa(e.captured.ex, TaskFailedException) && isa(e.captured.ex.task.result.ex, SpotPreemptException)
                            ex = e.captured.ex.task.result.ex
                            notbefore = DateTime(ex.notbefore, dateformat"e, dd u yyyy HH:MM:SS \G\M\T")
                            @info "caught preempt exception for $(ex.clusterid), removing not before $notbefore UTC"
                            _now = now(UTC)
                            if notbefore > _now
                                @info "sleeping for $(notbefore - _now)"
                                sleep(notbefore - _now)
                            end
                            u = wrkr.config.userdata
                            try
                                scaleset = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
                                add_instance_to_preempted_list(manager, scaleset, u["instanceid"])
                            catch e
                                @info "error adding instance to preempted list"
                            end

                            try
                                lock(Distributed.worker_lock)
                                if haskey(Distributed.map_pid_wrkr, pid)
                                    # We can't use rmprocs here since the worker process might already be gone due to preemption.  The
                                    # worker process would usually do the following two lines (see the Distributed.message_handler_loop function)
                                    Distributed.set_worker_state(Distributed.map_pid_wrkr[pid], Distributed.W_TERMINATED)
                                    Distributed.deregister_worker(pid)
                                end
                            catch
                            finally
                                unlock(Distributed.worker_lock)
                            end
                        end
                    end
                end
            end
        end
        @debug "done starting preempt loops"
    end
end

function Distributed.setup_launched_worker(manager::AzManager, wconfig, launched_q)
    # Distributed.create_worker also uses Distributed.worker_timeout, so we add a grace period
    # to allow for the Distributed.create_worker to hit its timeout.
    timeout = Distributed.worker_timeout() + 10
    interrupted = false
    local pid
    try
        tsk_create_worker = @async Distributed.create_worker(manager, wconfig)
        tic = time()
        while true
            if istaskdone(tsk_create_worker)
                pid = fetch(tsk_create_worker)
                break
            end
            if time() - tic > timeout && !interrupted
                @async Base.throwto(tsk_create_worker, InterruptException())
                interrupted = true
            end
            sleep(1)
        end
    catch e
        @warn "unable to create worker within $timeout seconds, adding vm to pending down list"
        logerror(e, Logging.Debug)
        u = wconfig.userdata
        scaleset = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
        add_instance_to_pending_down_list(manager, scaleset, u["instanceid"])
        add_instance_to_deleted_list(manager, scaleset, u["instanceid"])

        #=
        We don't rethrow the exception because we don't want addprocs_locked to throw.
        Instead, we want it to add whatever machines are successfull, and ignore those
        that are not.
        =#
        return
    end

    push!(launched_q, pid)

    # When starting workers on remote multi-core hosts, `launch` can (optionally) start only one
    # process on the remote machine, with a request to start additional workers of the
    # same type. This is done by setting an appropriate value to `WorkerConfig.cnt`.
    cnt = something(wconfig.count, 1)
    if cnt === :auto
        cnt = wconfig.environ[:cpu_threads]
    end
    cnt = cnt - 1   # Removing self from the requested number

    if cnt > 0
        Distributed.launch_n_additional_processes(manager, pid, wconfig, cnt, launched_q)
    end
end

function Distributed.manage(manager::AzManager, id::Integer, config::WorkerConfig, op::Symbol)
    if op == :register
        remote_do(AzManagers.logging, id)
    end
    if op == :interrupt
        # TODO
    end
    if op == :finalize
        # TODO
    end
    if op == :deregister || op == :interrupt
        # TODO
    end
end

macro spawn_interactive(ex::Expr)
    if VERSION > v"1.9"
        esc(:(Threads.@spawn :interactive $ex))
    else
        esc(:(Threads.@spawn $ex))
    end
end

struct SpotPreemptException <: Exception
    instanceid::String
    clusterid::Int
    notbefore::String
end
Base.showerror(io::IO, e::SpotPreemptException) = print(io, "spot preemption on process '$(e.clusterid)' ($(e.instanceid)), not before '$(e.notbefore)'")

function machine_preempt_loop(preempt_channel_future)
    if VERSION >= v"1.9" && Threads.nthreads(:interactive) > 0
        tsk = @spawn_interactive begin
            preempt_channel = fetch(preempt_channel_future)::Channel{Bool}
            instanceid = get_instanceid()
            clusterid = myid()
            @debug "starting preempt loop on $clusterid, $instanceid"

            while true
                ispreempted, notbefore = preempted(instanceid, clusterid)
                if ispreempted
                    @debug "putting onto preempt_channel"
                    put!(preempt_channel, true)
                    @debug "done putting onto preempt_channel"
                    # pid=1 will catch this exception, and remove the worker from the Julia cluster.
                    throw(SpotPreemptException(instanceid, clusterid, notbefore))
                end
                sleep(1)
            end
        end
        fetch(tsk)
    else
        @warn "AzManagers is not running the preempt loop for pid=$(myid()) since it requires at least one interactive thread on worker machines."
    end
end

"""
    f = machine_preempt_channel_future(pid)

If it exists, return a Future for a Channel allocation on the process with id `pid`, and that is used to
communicate VM preemptions on `pid`.  When a worker is preempted, a Bool is put onto the channel.  Thefore,
code can detect when this happens and take appropriate action before the machine corresponding to `pid` is
deleted.

# Example
```julia
addproc2(template, 2; spot=true)

f = machine_preempt_channel_future(workers()[1])

remote_do(pid, f) do
    c = fetch(f)::Channel{Bool}
    while true
        if isready(c)
            @info "the VM is being preempted"
            break
        end
        sleep(1)
    end
end
```
"""
function machine_preempt_channel_future(pid)
    manager = azmanager()
    timeout = parse(Int, get(ENV, "JULIA_WORKER_TIMEOUT", "60"))
    tic = time()
    while true
        if haskey(manager.preempt_channel_futures, pid)
            return manager.preempt_channel_futures[pid]
        end
        if time() - tic > timeout
            @warn "unble to obtain preemption channel from worker $pid in $timeout seconds"
            return nothing
        end
        sleep(1)
    end
end
