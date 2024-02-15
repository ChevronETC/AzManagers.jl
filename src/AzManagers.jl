module AzManagers

using AzSessions, Base64, CodecZlib, Dates, Distributed, HTTP, JSON, JSONWebTokens, LibGit2, Logging, MPI, Pkg, Printf, Random, Serialization, Sockets, TOML

function logerror(e, loglevel=Logging.Info)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")

    my_catch_stack = VERSION < v"1.7" ? Base.catch_stack : current_exceptions

    for (exc, bt) in my_catch_stack()
        showerror(io, exc, bt)
        println(io)
    end
    @logmsg loglevel String(take!(io))
    close(io)
end

const _manifest = Dict("resourcegroup"=>"", "ssh_user"=>"", "ssh_private_key_file"=>"", "ssh_public_key_file"=>"", "subscriptionid"=>"")

manifestpath() = joinpath(homedir(), ".azmanagers")
manifestfile() = joinpath(manifestpath(), "manifest.json")

"""
    AzManagers.write_manifest(;resourcegroup="", subscriptionid="", ssh_user="", ssh_public_key_file="~/.ssh/azmanagers_rsa.pub", ssh_private_key_file="~/.ssh/azmanagers_rsa")

Write an AzManagers manifest file (~/.azmanagers/manifest.json).  The
manifest file contains information specific to your Azure account.
"""
function write_manifest(;
        resourcegroup="",
        subscriptionid="",
        ssh_user="",
        ssh_private_key_file=joinpath(homedir(), ".ssh", "azmanagers_rsa"),
        ssh_public_key_file=joinpath(homedir(), ".ssh", "azmanagers_rsa.pub"))
    manifest = Dict("resourcegroup"=>resourcegroup, "subscriptionid"=>subscriptionid, "ssh_user"=>ssh_user, "ssh_private_key_file"=>ssh_private_key_file, "ssh_public_key_file"=>ssh_public_key_file)
    try
        isdir(manifestpath()) || mkdir(manifestpath(); mode=0o700)
        write(manifestfile(), json(manifest, 1))
        chmod(manifestfile(), 0o600)
    catch e
        @error "Failed to write manifest file, $(AzManagers.manifestfile())"
        throw(e)
    end
end

function load_manifest()
    if isfile(manifestfile())
        try
            manifest = JSON.parse(read(manifestfile(), String))
            for key in keys(_manifest)
                _manifest[key] = get(manifest, key, "")
            end
        catch e
            @error "Manifest file ($(AzManagers.manifestfile())) is not valid JSON"
            throw(e)
        end
    else
        @error "Manifest file ($(AzManagers.manifestfile())) does not exist.  Use AzManagers.write_manifest to generate a manifest file."
    end
end

const RETRYABLE_HTTP_ERRORS = (
    409,  # Conflict
    429,  # Too many requests
    500)  # Internal server error

function isretryable(e::HTTP.StatusError)
    e.status ∈ RETRYABLE_HTTP_ERRORS && (return true)
    false
end
isretryable(e::Base.IOError) = true
isretryable(e::HTTP.Exceptions.ConnectError) = true
isretryable(e::HTTP.Exceptions.HTTPError) = true
isretryable(e::HTTP.Exceptions.RequestError) = true
isretryable(e::HTTP.Exceptions.TimeoutError) = true
isretryable(e::Base.EOFError) = true
isretryable(e::Sockets.DNSError) = true
isretryable(e) = false

status(e::HTTP.StatusError) = e.status
status(e) = 999

function retrywarn(i, retries, s, e)
    if isa(e, HTTP.ExceptionRequest.StatusError)
        @debug "$(e.status): $(String(e.response.body)), retry $i of $retries, retrying in $s seconds"
        if e.status == 429
            remaining_resource = nothing
            for header in e.response.headers
                if header[1] == "x-ms-ratelimit-remaining-resource"
                    remaining_resource = header
                    break
                end
            end
            @warn "The Azure service is throttling the request, asking to retry after $s seconds.  Quota information:" remaining_resource[2]
        elseif e.status == 500
            b = JSON.parse(String(e.response.body))
            errorcode = get(get(b, "error", Dict()), "code", "")
            @warn "errorcode: $errorcode, retry $i, retrying in $s seconds"
        else
            @warn "status=$(e.status), retry $i, retrying in $s seconds"
        end
    else
        @warn "warn: $(typeof(e)) -- retry $i, retrying in $s seconds"
        logerror(e, Logging.Warn)
    end
end

macro retry(retries, ex::Expr)
    quote
        r = nothing
        for i = 0:$(esc(retries))
            try
                r = $(esc(ex))
                break
            catch e
                (i < $(esc(retries)) && isretryable(e)) || throw(e)
                maximum_backoff = 256
                s = min(2.0^(i-1), maximum_backoff) + rand()
                if status(e) ∈ (429,500)
                    for header in e.response.headers
                        if lowercase(header[1]) == "retry-after"
                            s = parse(Int, header[2]) + rand()
                            break
                        end
                    end
                end
                retrywarn(i, $(esc(retries)), s, e)
                sleep(s)
            end
        end
        r
    end
end

function azrequest(rtype, verbose, url, headers, body=nothing)
    if contains(url, "virtualMachineScaleSets")
        manager = azmanager()
        if isdefined(manager, :scaleset_request_counter)
            manager.scaleset_request_counter += 1
        else
            manager.scaleset_request_counter = 1
        end
    end

    options = (retry=false, status_exception=false)
    if body === nothing
        r = HTTP.request(rtype, url, headers; verbose=verbose, options...)
    else
        r = HTTP.request(rtype, url, headers, body; verbose=verbose, options...)
    end
    
    if r.status >= 300
        throw(HTTP.Exceptions.StatusError(r.status, r.request.method, r.request.target, r))
    end
    
    r
end

function scaleset_request_counter()
    manager = azmanager()
    if isdefined(manager, :scaleset_request_counter)
        return manager.scaleset_request_counter
    else
        return 1
    end
end

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

    _manager.port,_manager.server = listenany(getipaddr(), 9000)
    _manager.pending_up = Channel{TCPSocket}(64)
    _manager.pending_down = Dict{ScaleSet,Set{String}}()
    _manager.deleted = Dict{ScaleSet,Dict{String,DateTime}}()
    _manager.pruned = Dict{ScaleSet,Set{String}}()
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
            logerror(e, Logging.Error)
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
            logerror(e, Logging.Error)
        end
    end
end

scalesets(manager::AzManager) = isdefined(manager, :scalesets) ? manager.scalesets : Dict{ScaleSet,Int}()
scalesets() = scalesets(azmanager())
pending_down(manager::AzManager) = isdefined(manager, :pending_down) ? manager.pending_down : Dict{ScaleSet,Set{String}}()

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
    unlock(manager.lock)
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
            delete!(pending_down(manager), scaleset)
        catch e
            if status(e) == 404
                # the resource is already deleted, nothing else to do
            else
                @error "error deleting scaleset vms, manual clean-up may be required."
                logerror(e, Logging.Error)
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
        logerror(e)
    end
    unlock(manager.lock)
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

    # remove from list workers that are already scheduled for removal from the cluster
    for id in pending_down(manager)
        delete!(wrkrs, id)
    end

    # remove from list workers that are in TERMINATED or TERMINATING cluster state
    for (id,wrkr) in Distributed.map_pid_wrkr
        if isdefined(wrkr, :state) && wrkr.state ∈ (Distributed.W_TERMINATED, Distributed.W_TERMINATING)
            delete!(wrkrs, id)
        end
    end

    # remove from list workers that are in Distributed's deletion pool
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
    worker_timeout = Second(parse(Int, get(ENV, "JULIA_WORKER_TIMEOUT", "720")))
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

            doprune = (time_elapsed > worker_timeout || vm_state == "failed") && !is_worker_deleting && !is_vm_deleting && !ispruned_already
            if doprune
                @info "Putting machine with instance id $instanceid in $(scaleset.scalesetname) onto the deletion queue because it failed to join the Julia cluster after $(round(time_elapsed, Second)), vm_state=$vm_state."
                if manager.save_cloud_init_failures
                    @info "copying cloud init output log to '$(pwd())/cloud-init-output-$(instanceid).log'."
                    try
                        ipaddress = get_ipaddress_for_scaleset_vm(manager, _vm)
                        run(`scp -i $(homedir())/.ssh/azmanagers_rsa $(manager.ssh_user)@$(ipaddress):/var/log/cloud-init-output.log ./cloud-init-output-$(instanceid).log`)
                    catch e
                        @warn "failed to copy cloud init log from VM $(instanceid)."
                        logerror(e, Logging.Warn)
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
                push!(manager.pending_up, s)
            end
        catch
            @error "AzManagers, error adding pending connection"
            logerror(e, Logging.Error)
        end
    end
end

function Distributed.addprocs(manager::AzManager; sockets)
    try
        Distributed.init_multi()
        Distributed.cluster_mgmt_from_master_check()
        lock(Distributed.worker_lock)
        Distributed.addprocs_locked(manager; sockets)
    catch
        if manager.verbose > 0
            @error "AzManagers, error processing pending connection"
            logerror(e, Logging.Error)
        end
    finally
        unlock(Distributed.worker_lock)
    end
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
                push!(sockets, take!(manager.pending_up))
            elseif isready(manager.pending_up)
                push!(sockets, take!(manager.pending_up))
            else
                sleep(0.1)
            end

            elapsedtime = time() - tic
            instances_per_second = length(sockets)/elapsedtime
            if length(sockets) == 0 || (elapsedtime < min_cadence && instances_per_second > min_instances_per_second && length(sockets) < max_sockets)
                continue
            else
                tic = time()
                @debug "triggered adding machines" elapsedtime min_cadence instances_per_second min_instances_per_second length(sockets) max_sockets nworkers_provisioned()
            end
        catch e
            if manager.verbose > 0
                @error "AzManagers, error retrieving pending connection"
                logerror(e, Logging.Error)
            end
            return
        end

        pids = addprocs(manager; sockets)
        empty!(sockets)

        @sync for pid in pids
            @async begin
                wrkr = Distributed.map_pid_wrkr[pid]
                if isdefined(wrkr, :config) && isdefined(wrkr.config, :userdata) && lowercase(get(wrkr.config.userdata, "priority", "")) == "spot"
                    remote_do(machine_prempt_loop, pid) # monitor for Azure spot evictions on each machine
                end
            end
        end
        pids
    end
end

include("templates.jl")

spin(spincount, elapsed_time) = ['◐','◓','◑','◒','✓'][spincount]*@sprintf(" %.2f",elapsed_time)*" seconds"
function spinner(n_target_workers)
    local ws,spincount,starttime,elapsed_time,tic,_nworkers
    try
        ws = repeat(" ", 5)
        spincount = 1
        starttime = time()
        elapsed_time = 0.0
        tic = time()
        _nworkers = nprocs() == 1 ? 0 : nworkers()
    catch e
        @warn "error during startup:"
        logerror(e, Logging.Warn)
    end
    while nprocs() == 1 || nworkers() != n_target_workers
        try
            elapsed_time = time() - starttime
            if time() - tic > 10
                _nworkers = nprocs() == 1 ? 0 : nworkers()
                tic = time()
            end
            write(stdout, spin(spincount, elapsed_time)*", $_nworkers/$n_target_workers up. $ws\r")
            flush(stdout)
            spincount = spincount == 4 ? 1 : spincount + 1
            yield()
            sleep(.25)
        catch e
            @warn "error during startup:"
            logerror(e, Logging.Warn)
        end
    end
    _nworkers = nprocs() == 1 ? 0 : nworkers()
    write(stdout, spin(5, elapsed_time)*", $_nworkers/$n_target_workers are running. $ws\r")
    write(stdout,"\n")
    nothing
end

function nthreads_filter(nthreads)
    _nthreads = split(string(nthreads), ',')
    nthreads_default = length(_nthreads) > 0 ? parse(Int, _nthreads[1]) : 1
    nthreads_interactive = length(_nthreads) > 1 ? parse(Int, _nthreads[2]) : 0

    nthreads_interactive > 0 ? string("$nthreads_default,$nthreads_interactive") : string(nthreads_default)
end

"""
    addprocs(template, ninstances[; kwargs...])

Add Azure scale set instances where template is either a dictionary produced via the `AzManagers.build_sstemplate`
method or a string corresponding to a template stored in `~/.azmanagers/templates_scaleset.json.`

# key word arguments:
* `subscriptionid=template["subscriptionid"]` if exists, or `AzManagers._manifest["subscriptionid"]` otherwise.
* `resourcegroup=template["resourcegroup"]` if exists, or `AzManagers._manifest["resourcegroup"]` otherwise.
* `sigimagename=""` The name of the SIG image[1].
* `sigimageversion=""` The version of the `sigimagename`[1].
* `imagename=""` The name of the image (alternative to `sigimagename` and `sigimageversion` used for development work).
* `osdisksize=60` The size of the OS disk in GB.
* `customenv=false` If true, then send the current project environment to the workers where it will be instantiated.
* `session=AzSession(;lazy=true)` The Azure session used for authentication.
* `group="cbox"` The name of the Azure scale set.  If the scale set does not yet exist, it will be created.
* `overprovision=true` Use Azure scle-set overprovisioning?
* `ppi=1` The number of Julia processes to start per Azure scale set instance.
* `julia_num_threads="\$(Threads.nthreads(),\$(Threads.nthreads(:interactive))"` set the number of julia threads for the detached process.[2]
* `omp_num_threads=get(ENV, "OMP_NUM_THREADS", 1)` set the number of OpenMP threads to run on each worker
* `exeflags=""` set additional command line start-up flags for Julia workers.  For example, `--heap-size-hint=1G`.
* `env=Dict()` each dictionary entry is an environment variable set on the worker before Julia starts. e.g. `env=Dict("OMP_PROC_BIND"=>"close")`
* `nretry=20` Number of retries for HTTP REST calls to Azure services.
* `verbose=0` verbose flag used in HTTP requests.
* `save_cloud_init_failures=false` set to true to copy cloud init logs (/var/log/clout-init-output.log) from workers that fail to join the cluster.
* `show_quota=false` after various operation, show the "x-ms-rate-remaining-resource" response header.  Useful for debugging/understanding Azure quota's.
* `user=AzManagers._manifest["ssh_user"]` ssh user.
* `spot=false` use Azure SPOT VMs for the scale-set
* `maxprice=-1` set maximum price per hour for a VM in the scale-set.  `-1` uses the market price.
* `spot_base_regular_priority_count=0` If spot is true, only start adding spot machines once there are this many non-spot machines added.
* `spot_regular_percentage_above_base` If spot is true, then when ading new machines (above `spot_base_reqular_priority_count`) use regular (non-spot) priority for this percent of new machines.
* `waitfor=false` wait for the cluster to be provisioned before returning, or return control to the caller immediately[3]
* `mpi_ranks_per_worker=0` set the number of MPI ranks per Julia worker[4]
* `mpi_flags="-bind-to core:\$(ENV["OMP_NUM_THREADS"]) -map-by numa"` extra flags to pass to mpirun (has effect when `mpi_ranks_per_worker>0`)
* `nvidia_enable_ecc=true` on NVIDIA machines, ensure that ECC is set to `true` or `false` for all GPUs[5]
* `nvidia_enable_mig=false` on NVIDIA machines, ensure that MIG is set to `true` or `false` for all GPUs[5]
* `hyperthreading=nothing` Turn on/off hyperthreading on supported machine sizes.  The default uses the setting in the template.  To override the template setting, use `true` (on) or `false` (off).

# Notes
[1] If `addprocs` is called from an Azure VM, then the default `imagename`,`imageversion` are the
image/version the VM was built with; otherwise, it is the latest version of the image specified in the scale-set template.
[2] Interactive threads are supported beginning in version 1.9 of Julia.  For earlier versions, the default for `julia_num_threads` is `Threads.nthreads()`.
[3] `waitfor=false` reflects the fact that the cluster manager is dynamic.  After the call to `addprocs` returns, use `workers()`
to monitor the size of the cluster.
[4] This is inteneded for use with Devito.  In particular, it allows Devito to gain performance by using
MPI to do domain decomposition using MPI within a single VM.  If `mpi_ranks_per_worker=0`, then MPI is not
used on the Julia workers.
[5] This may result in a re-boot of the VMs
"""
function Distributed.addprocs(template::Dict, n::Int;
        subscriptionid = "",
        resourcegroup = "",
        sigimagename = "",
        sigimageversion = "",
        imagename = "",
        osdisksize = 60,
        customenv = false,
        session = AzSession(;lazy=true),
        group = "cbox",
        overprovision = true,
        ppi = 1,
        julia_num_threads = VERSION >= v"1.9" ? "$(Threads.nthreads()),$(Threads.nthreads(:interactive))" : string(Threads.nthreads()),
        omp_num_threads = parse(Int, get(ENV, "OMP_NUM_THREADS", "1")),
        exeflags = "",
        env = Dict(),
        nretry = 20,
        verbose = 0,
        save_cloud_init_failures = false,
        show_quota = false,
        user = "",
        spot = false,
        maxprice = -1,
        spot_base_regular_priority_count = 0,
        spot_regular_percentage_above_base = 0,
        waitfor = false,
        mpi_ranks_per_worker = 0,
        mpi_flags = "-bind-to core:$(get(ENV, "OMP_NUM_THREADS", 1)) --map-by numa",
        nvidia_enable_ecc = true,
        nvidia_enable_mig = false,
        hyperthreading = nothing)
    n_current_workers = nprocs() == 1 ? 0 : nworkers()

    (subscriptionid == "" || resourcegroup == "" || user == "") && load_manifest()
    subscriptionid == "" && (subscriptionid = get(template, "subscriptionid", _manifest["subscriptionid"]))
    resourcegroup == "" && (resourcegroup = get(template, "resourcegroup", _manifest["resourcegroup"]))
    user == "" && (user = _manifest["ssh_user"])

    manager = azmanager!(session, user, nretry, verbose, save_cloud_init_failures, show_quota)
    sigimagename,sigimageversion,imagename = scaleset_image(manager, sigimagename, sigimageversion, imagename)
    scaleset_image!(manager, template["value"], sigimagename, sigimageversion, imagename)
    software_sanity_check(manager, imagename == "" ? sigimagename : imagename, customenv)

    @async delete_pending_down_vms()

    _scalesets = scalesets(manager)
    scaleset = ScaleSet(subscriptionid, resourcegroup, group)

    osdisksize = max(osdisksize, image_osdisksize(manager, template["value"], sigimagename, sigimageversion, imagename))

    julia_num_threads = nthreads_filter(julia_num_threads)

    @info "Provisioning $n virtual machines in scale-set $group..."
    _scalesets[scaleset] = scaleset_create_or_update(manager, user, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, sigimagename,
        sigimageversion, imagename, osdisksize, nretry, template, n, ppi, mpi_ranks_per_worker, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig,
        hyperthreading, julia_num_threads, omp_num_threads, exeflags, env, spot, maxprice, spot_base_regular_priority_count, spot_regular_percentage_above_base,
        verbose, customenv, overprovision)

    if waitfor
        @info "Initiating cluster..."
        spinner_tsk = @async spinner(n_current_workers + n)
        wait(spinner_tsk)
    end

    nothing
end

function Distributed.addprocs(template::AbstractString, n::Int; kwargs...)
    isfile(templates_filename_scaleset()) || error("scale-set template file does not exist.  See `AzManagers.save_template_scaleset`")

    templates_scaleset = JSON.parse(read(templates_filename_scaleset(), String))
    haskey(templates_scaleset, template) || error("scale-set template file does not contain a template with name: $template. See `AzManagers.save_template_scaleset`")

    addprocs(templates_scaleset[template], n; kwargs...)
end

function Distributed.launch(manager::AzManager, params::Dict, launched::Array, c::Condition)
    sockets = params[:sockets]

    @sync for socket in sockets
        @async try
            Distributed.launch_on_machine(manager, launched, c, socket)
        catch e
            @error "failed to launch on machine for socket=$socket"
            logerror(e, Logging.Error)
        end
    end
end

function Distributed.launch_on_machine(manager::AzManager, launched, c, socket)
    local _cookie
    try
        _cookie = read(socket, Distributed.HDR_COOKIE_LEN)
    catch e
        if manager.verbose > 0
            @error "unable to read cookie from socket"
            logerror(e, Logging.Error)
        end
        return
    end

    cookie = String(_cookie)
    cookie == Distributed.cluster_cookie() || error("Invalid cookie sent by remote worker.")

    local _connection_string
    try
        _connection_string = readline(socket)
    catch
        if manager.verbose > 0
            @error "unable to read connection string from socket"
            logerror(e, Logging.Error)
        end
        return
    end

    connection_string = String(base64decode(_connection_string))

    local vm
    try
        vm = JSON.parse(connection_string)
    catch
        if manager.verbose > 0
            @error "unable to parse connection string, string=$connection_string, cookie=$cookie"
            logerror(e, Logging.Error)
        end
        return
    end

    wconfig = WorkerConfig()
    wconfig.io = socket
    wconfig.bind_addr = vm["bind_addr"]
    wconfig.count = vm["ppi"]
    wconfig.exename = "julia"
    wconfig.exeflags = `$(vm["exeflags"])`
    wconfig.userdata = vm["userdata"]

    push!(launched, wconfig)
    notify(c)
end

function add_instance_to_pending_down_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.pending_down, scaleset)
        @debug "pushing worker with id=$instanceid onto pending_down"
        push!(manager.pending_down[scaleset], string(instanceid))
    else
        @debug "creating pending_down vector for id=$instanceid"
        manager.pending_down[scaleset] = Set{String}([string(instanceid)])
    end
    nothing
end

function add_instance_to_pruned_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.pruned, scaleset)
        @debug "pushing worker with id=$instanceid onto pruned"
        push!(manager.pruned[scaleset], string(instanceid))
    else
        @debug "creating pruned vector for id=$instanceid"
        manager.pruned[scaleset] = Set{String}([string(instanceid)])
    end
    nothing
end

function add_instance_to_deleted_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.deleted, scaleset)
        @debug "pushing worker with id=$instanceid onto deleted"
        manager.deleted[scaleset][instanceid] = now(Dates.UTC)
    else
        @debug "creating deleted dictionary for id=$instanceid"
        manager.deleted[scaleset] = Dict(instanceid=>now(Dates.UTC))
    end
    nothing
end

function Distributed.kill(manager::AzManager, id::Int, config::WorkerConfig)
    @debug "kill for id=$id"
    try
        remote_do(exit, id)
    catch
    end
    @debug "kill, done remote_do"

    u = config.userdata
    get(u, "localid", 1) > 1 && (return nothing) # an "additional" worker on an instance will have localid>1

    scaleset = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])

    add_instance_to_pending_down_list(manager, scaleset, u["instanceid"])
    add_instance_to_deleted_list(manager, scaleset, u["instanceid"])

    @debug "...kill, pushed."
    nothing
end

function remaining_resource(r)
    _remaining_resource = ""
    for header in r.headers
        if header[1] == "x-ms-ratelimit-remaining-resource"
            _remaining_resource = header[2]
        end
    end
    _remaining_resource
end

"""
    nworkers_provisioned([service=false])

Count of the number of scale-set machines that are provisioned
regardless if their status within the Julia cluster.  If `service=true`,
then we use the Azure scale-set service to make the count, otherwise
we use client side book-keeeping.  The later is useful to avoid making
too many requests to the Azure scale-set service, causing it to throttle
future responses.
"""
function nworkers_provisioned(service=false)
    manager = azmanager()
    _scalesets = scalesets(manager)

    n = 0
    for (scaleset, N) in _scalesets
        if service
            n += scaleset_capacity(manager, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, manager.nretry, manager.verbose)
        else
            n += N
        end
    end
    n
end

"""
    rmgroup(groupname[; kwargs...])

Remove an azure scale-set and all of its virtual machines.

# Optional keyword arguments
* `subscriptionid=AzManagers._manifest["subscriptionid"]`
* `resourcegroup=AzManagers._manifest["resourcegroup"]`
* `user=AzManagers._manifest["ssh_user"]` ssh user.
* `session=AzSession(;lazy=true)` The Azure session used for authentication.
* `nretry=20` Number of retries for HTTP REST calls to Azure services.
* `verbose=0` verbose flag used in HTTP requests.
* `show_quota=false` after various operation, show the "x-ms-rate-remaining-resource" response header.  Useful for debugging/understanding Azure quota's.
"""
function rmgroup(groupname;
        subscriptionid = "",
        resourcegroup = "",
        user = "",
        session = AzSession(;lazy=true),
        nretry = 20,
        verbose = 0,
        show_quota = false)
    load_manifest()
    subscriptionid == "" && (subscriptionid = AzManagers._manifest["subscriptionid"])
    resourcegroup == "" && (resourcegroup = AzManagers._manifest["resourcegroup"])
    user == "" && (user = AzManagers._manifest["ssh_user"])

    manager = azmanager!(session, user, nretry, verbose, false, show_quota)
    rmgroup(manager, subscriptionid, resourcegroup, groupname, nretry, verbose, show_quota)
end

function rmgroup(manager::AzManager, subscriptionid, resourcegroup, groupname, nretry=20, verbose=0, show_quota=false)
    groupnames = list_scalesets(manager, subscriptionid, resourcegroup, nretry, verbose)
    if groupname ∈ groupnames
        @debug "deleting scaleset $groupname"
        r = @retry nretry azrequest(
            "DELETE",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$groupname?forceDeletion=True&api-version=2023-07-01",
            ["Authorization" => "Bearer $(token(manager.session))"])

        if show_quota
            @info "Quotas after deleting scale-set" remaining_resource(r)
        end
    end
    nothing
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

function get_instanceid()
    local r
    try
        _r = HTTP.request("GET", "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01", ["Metadata"=>"true"])
        r = JSON.parse(String(_r.body))
    catch
        r = Dict()
    end
    get(r, "name", "")
end

"""
    preempted([id=myid()|id="instanceid"])

Check to see if the machine `id::Int` has received an Azure spot preempt message.  Returns
true if a preempt message is received and false otherwise.
"""
function preempted(instanceid::AbstractString="", clusterid::Int=0)
    isempty(instanceid) && (instanceid = get_instanceid())
    clusterid == 0 && (clusterid = myid())
    local _r
    try
        tic = time()
        _r = HTTP.request("GET", "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01", ["Metadata"=>"true"])
        if time() - tic > 55 # 55 seconds, simply because it is less that 60, and 60 seconds is the eviction notice.
            @warn "$(now()), took longer than 55 seconds to query the meta-data server for scheduled events (elapsed time=$(time() - tic))."
        end
    catch
        @warn "unable to get scheduledevents."
        return false
    end
    r = JSON.parse(String(_r.body))
    for event in get(r, "Events", [])
        if get(event, "EventType", "") == "Preempt" && instanceid ∈ get(event, "Resources", [])
            @warn "Machine with id $clusterid ($instanceid) is being pre-empted" now(Dates.UTC) event["NotBefore"] event["EventType"] event["EventSource"]
            return true
        end
    end
    return false
end
preempted(id::Int) = remotecall_fetch(preempted, id)

function _machine_preempt_loop(pid, clusterid)
    instanceid = ""
    while true
        instanceid = get_instanceid()
        instanceid == "" || break
        sleep(1)
    end
    while true
        if AzManagers.preempted(instanceid, clusterid)
            # self-destruct button, Distributed should see that the process is exited and update the cluster book-keeping.
            @info "self-destruct, killing pid=$pid"
            run(`kill -9 $pid`)
            exit()
            break
        end
        sleep(1)
    end
end

function machine_prempt_loop()
    project = dirname(Pkg.project().path)
    pid = getpid()
    id = myid()
    open(`julia --project=$project -e "using AzManagers; AzManagers._machine_preempt_loop($pid, $id)"`)
end

function azure_physical_name(keyval="PhysicalHostName")
    local physical_hostname
    try
        s = split(read("/var/lib/hyperv/.kvp_pool_3", String), '\0'; keepempty=false)
        i = findfirst(_s->_s==keyval, s)
        physical_hostname = s[i+1]
    catch
        physical_hostname = "unknown"
    end
    physical_hostname
end

function azure_worker_init(cookie, master_address, master_port, ppi, exeflags, mpi_size)
    c = connect(IPv4(master_address), master_port)

    nbytes_written = write(c, rpad(cookie, Distributed.HDR_COOKIE_LEN)[1:Distributed.HDR_COOKIE_LEN])
    nbytes_written == Distributed.HDR_COOKIE_LEN || error("unable to write bytes")
    flush(c)

    _r = HTTP.request("GET", "http://169.254.169.254/metadata/instance?api-version=2021-02-01", ["Metadata"=>"true"]; redirect=false)
    r = JSON.parse(String(_r.body))
    vm = Dict(
        "exeflags" => exeflags,
        "bind_addr" => string(getipaddr(IPv4)),
        "ppi" => ppi,
        "userdata" => Dict(
            "subscriptionid" => lowercase(r["compute"]["subscriptionId"]),
            "resourcegroup" => lowercase(r["compute"]["resourceGroupName"]),
            "scalesetname" => lowercase(r["compute"]["vmScaleSetName"]),
            "instanceid" => split(r["compute"]["resourceId"], '/')[end],
            "priority" => get(r["compute"], "priority", ""),
            "localid" => 1,
            "name" => r["compute"]["name"],
            "mpi" => mpi_size > 0,
            "mpi_size" => mpi_size,
            "physical_hostname" => azure_physical_name()))

    _vm = base64encode(json(vm))

    nbytes_written = write(c, _vm*"\n")
    nbytes_written == length(_vm)+1 || error("wrote wrong number of bytes")
    flush(c)

    c
end

function logging()
    manager = azmanager()

    # if the workers are MPI enabled, then manager is only fully defined on MPI rank 0
    if isdefined(manager, :worker_socket)
        out = manager.worker_socket

        redirect_stdout(out)
        redirect_stderr(out)

        # work-a-round https://github.com/JuliaLang/julia/issues/38482
        global_logger(ConsoleLogger(out, Logging.Info))
    end
    nothing
end

if VERSION < v"1.7"
    errormonitor = identity
end

function azure_worker_start(out::IO, cookie::AbstractString=readline(stdin); close_stdin::Bool=true, stderr_to_stdout::Bool=true)
    Distributed.init_multi()

    if close_stdin # workers will not use it
        redirect_stdin(devnull)
        close(stdin)
    end
    stderr_to_stdout && redirect_stderr(stdout)

    Distributed.init_worker(cookie)
    interface = IPv4(Distributed.LPROC.bind_addr)
    if Distributed.LPROC.bind_port == 0
        port_hint = 9000 + (getpid() % 1000)
        (port, sock) = listenany(interface, UInt16(port_hint))
        Distributed.LPROC.bind_port = port
    else
        sock = listen(interface, Distributed.LPROC.bind_port)
    end

    t = errormonitor(@async while isopen(sock)
        client = accept(sock)

        #=
        We observe that a valid machine often receive UInt(0)'s instead
        of the cookie.  We do not know he cuase of this, but here we throw
        an error which will be handled and rethrown, below, in the 'while true'
        loop.  This results in this function to throw, causing the 'azure_worker'
        method to re-try joining the cluster.

        The error handling is a little complicated here due to how the error
        handling in 'Distributed.process_messages' works.  In particular, we
        read the cookie ourselves and, subsequently, pass 'false' as the
        third argument to 'Distributed.process_messages'.  This, in turn,
        lets 'process_messages' skip its cookie read/check.
        =#

        cookie_from_master = read(client, Distributed.HDR_COOKIE_LEN)
        if cookie_from_master[1] == 0x00
            error("received cookie with at least one null character")
        end

        if String(cookie_from_master) != cookie
            error("received invalid cookie.")
        end

        Distributed.process_messages(client, client, false)
    end)
    print(out, "julia_worker:")  # print header
    print(out, "$(string(Distributed.LPROC.bind_port))#") # print port
    print(out, Distributed.LPROC.bind_addr)
    print(out, '\n')
    flush(out)

    Sockets.nagle(sock, false)
    Sockets.quickack(sock, true)

    if ccall(:jl_running_on_valgrind,Cint,()) != 0
        println(out, "PID = $(getpid())")
    end

    manager = azmanager()
    manager.worker_socket = out

    try
        while true
            Distributed.check_master_connect()
            @info "message loop..."
            wait(t)
            istaskfailed(t) && fetch(t)
            sleep(10)
        end
    catch e
        throw(e)
    finally
        close(sock)
    end
end

function azure_worker(cookie, master_address, master_port, ppi, exeflags)
    itry = 0

    #=
    The following `azure_worker_start` call, on occasion, fails within the
    `Distributed.process_messages` method.  The following retry logic is a
    work-a-round until the root cause can be investigated.
    =#
    while true
        itry += 1
        local c
        try
            c = azure_worker_init(cookie, master_address, master_port, ppi, exeflags, 0)
            azure_worker_start(c, cookie)
        catch e
            @error "error starting worker, attempt $itry, cookie=$cookie, master_address=$master_address, master_port=$master_port, ppi=$ppi"
            logerror(e, Logging.Error)
            if itry > 10
                throw(e)
            end
            if @isdefined c
                try
                    close(c)
                catch
                end
            end
        end
        sleep(60)
    end
end

# We create our own method here so that we can add `localid` and `cnt` to `wconfig`.  This can
# be useful when we need to understand the layout of processes that are sharing the same hardware.
function Distributed.launch_n_additional_processes(manager::AzManager, frompid, fromconfig, cnt, launched_q)
    @sync begin
        exename = Distributed.notnothing(fromconfig.exename)
        exeflags = something(fromconfig.exeflags, ``)
        cmd = `$exename $exeflags --worker`

        new_addresses = remotecall_fetch(Distributed.launch_additional, frompid, cnt, cmd)
        for (localid,address) in enumerate(new_addresses)
            (bind_addr, port) = address

            wconfig = Distributed.WorkerConfig()
            for x in [:host, :tunnel, :multiplex, :sshflags, :exeflags, :exename, :enable_threaded_blas]
                Base.setproperty!(wconfig, x, Base.getproperty(fromconfig, x))
            end
            wconfig.bind_addr = bind_addr
            wconfig.port = port
            wconfig.count = fromconfig.count
            wconfig.userdata = Dict(
                "localid" => localid+1,
                "name" => fromconfig.userdata["name"],
                "subscriptionid" => fromconfig.userdata["subscriptionid"],
                "resourcegroup" => fromconfig.userdata["resourcegroup"],
                "scalesetname" => fromconfig.userdata["scalesetname"])

            let wconfig=wconfig
                @async begin
                    pid = Distributed.create_worker(manager, wconfig)
                    remote_do(Distributed.redirect_output_from_additional_worker, frompid, pid, port)
                    push!(launched_q, pid)
                end
            end
        end
    end
end

#
# MPI specific methods --
# These methods are slightly modified versions of what is in the Julia distributed standard library
#
function azure_worker_mpi(cookie, master_address, master_port, ppi, exeflags)
    itry = 0
    while true
        itry += 1
        local c
        try
            MPI.Initialized() || MPI.Init()

            comm = MPI.COMM_WORLD
            mpi_size = MPI.Comm_size(comm)
            mpi_rank = MPI.Comm_rank(comm)

            local t
            if mpi_rank == 0
                c = azure_worker_init(cookie, master_address, master_port, ppi, exeflags, mpi_size)
                t = @async start_worker_mpi_rank0(c, cookie)
            else
                t = @async message_handler_loop_mpi_rankN()
            end

            MPI.Barrier(comm)
            fetch(t)
            MPI.Barrier(comm)
        catch e
            @error "error starting worker, attempt $itry, cookie=$cookie, master_address=$master_address, master_port=$master_port, ppi=$ppi"
            logerror(e, Logging.Error)
            if itry > 10
                throw(e)
            end
            if @isdefined c
                try
                    close(c)
                catch
                end
            end
        end
        sleep(60)
    end
end

function process_messages_mpi_rank0(r_stream::TCPSocket, w_stream::TCPSocket, incoming::Bool=true)
    @async process_tcp_streams_mpi_rank0(r_stream, w_stream, incoming)
end

function process_tcp_streams_mpi_rank0(r_stream::TCPSocket, w_stream::TCPSocket, incoming::Bool)
    Sockets.nagle(r_stream, false)
    Sockets.quickack(r_stream, true)
    Distributed.wait_connected(r_stream)
    if r_stream != w_stream
        Sockets.nagle(w_stream, false)
        Sockets.quickack(w_stream, true)
        Distributed.wait_connected(w_stream)
    end
    message_handler_loop_mpi_rank0(r_stream, w_stream, incoming)
end

function message_handler_loop_mpi_rank0(r_stream::IO, w_stream::IO, incoming::Bool)
    wpid=0          # the worker r_stream is connected to.
    boundary = similar(Distributed.MSG_BOUNDARY)

    comm = MPI.Initialized() ? MPI.COMM_WORLD : nothing

    try
        version = Distributed.process_hdr(r_stream, incoming)
        serializer = Distributed.ClusterSerializer(r_stream)

        # The first message will associate wpid with r_stream
        header = Distributed.deserialize_hdr_raw(r_stream)
        msg = Distributed.deserialize_msg(serializer)
        Distributed.handle_msg(msg, header, r_stream, w_stream, version)
        wpid = worker_id_from_socket(r_stream)
        @assert wpid > 0

        readbytes!(r_stream, boundary, length(Distributed.MSG_BOUNDARY))

        while true
            Distributed.reset_state(serializer)
            header = Distributed.deserialize_hdr_raw(r_stream)
            # println("header: ", header)

            try
                msg = Distributed.invokelatest(Distributed.deserialize_msg, serializer)
            catch e
                # Deserialization error; discard bytes in stream until boundary found
                boundary_idx = 1
                while true
                    # This may throw an EOF error if the terminal boundary was not written
                    # correctly, triggering the higher-scoped catch block below
                    byte = read(r_stream, UInt8)
                    if byte == Distributed.MSG_BOUNDARY[boundary_idx]
                        boundary_idx += 1
                        if boundary_idx > length(Distributed.MSG_BOUNDARY)
                            break
                        end
                    else
                        boundary_idx = 1
                    end
                end

                # remotecalls only rethrow RemoteExceptions. Any other exception is treated as
                # data to be returned. Wrap this exception in a RemoteException.
                remote_err = RemoteException(myid(), CapturedException(e, catch_backtrace()))
                # println("Deserialization error. ", remote_err)
                if !Distributed.null_id(header.response_oid)
                    ref = Distributed.lookup_ref(header.response_oid)
                    put!(ref, remote_err)
                end
                if !Distributed.null_id(header.notify_oid)
                    Distributed.deliver_result(w_stream, :call_fetch, header.notify_oid, remote_err)
                end
                continue
            end
            readbytes!(r_stream, boundary, length(Distributed.MSG_BOUNDARY))

            if comm !== nothing
                header = MPI.bcast(header, 0, comm)
                msg = MPI.bcast(msg, 0, comm)
                version = MPI.bcast(version, 0, comm)
            end

            tsk = Distributed.handle_msg(msg, header, r_stream, w_stream, version)

            if comm !== nothing
                wait(tsk) # TODO - this seems needed to not cause a race in the MPI logic, but I'm not sure what the side-effects are.
                MPI.Barrier(comm)
            end
        end
    catch e
        # Check again as it may have been set in a message handler but not propagated to the calling block above
        if wpid < 1
            wpid = worker_id_from_socket(r_stream)
        end

        if wpid < 1
            println(stderr, e, CapturedException(e, catch_backtrace()))
            println(stderr, "Process($(myid())) - Unknown remote, closing connection.")
        elseif !(wpid in Distributed.map_del_wrkr)
            werr = Distributed.worker_from_id(wpid)
            oldstate = werr.state
            Distributed.set_worker_state(werr, Distributed.W_TERMINATED)

            # If unhandleable error occurred talking to pid 1, exit
            if wpid == 1
                if isopen(w_stream)
                    @error "Fatal error on process $(myid())" exception=e,catch_backtrace()
                end
                exit(1)
            end

            # Will treat any exception as death of node and cleanup
            # since currently we do not have a mechanism for workers to reconnect
            # to each other on unhandled errors
            Distributed.deregister_worker(wpid)
        end

        isopen(r_stream) && close(r_stream)
        isopen(w_stream) && close(w_stream)

        if (myid() == 1) && (wpid > 1)
            if oldstate != Distributed.W_TERMINATING
                println(stderr, "Worker $wpid terminated.")
                rethrow()
            end
        end

        return nothing
    end
end

function message_handler_loop_mpi_rankN()
    comm = MPI.COMM_WORLD
    header,msg,version = nothing,nothing,nothing
    while true
        try
            header = MPI.bcast(header, 0, comm)
            msg = MPI.bcast(msg, 0, comm)
            version = MPI.bcast(version, 0, comm)

            # ignore the message unless it is of type CallMsg{:call}, CallMsg{:call_fetch}, CallWaitMsg, RemoteDoMsg
            if typeof(msg) ∈ (Distributed.CallMsg{:call}, Distributed.CallMsg{:call_fetch}, Distributed.CallWaitMsg, Distributed.RemoteDoMsg)
                # Cast the call_fetch message to a call method since we only want the fetch from MPI rank 0.
                if typeof(msg) ∈ (Distributed.CallMsg{:call_fetch}, Distributed.CallWaitMsg)
                    msg = Distributed.CallMsg{:call}(msg.f, msg.args, msg.kwargs)
                end

                tsk = Distributed.handle_msg(msg, header, devnull, devnull, version)
                wait(tsk)
            end

            MPI.Barrier(comm)
        catch e
            @warn "MPI - message_handler_loop_mpi"
            logerror(e, Logging.Warn)
        end
    end
end

start_worker_mpi_rank0(cookie::AbstractString=readline(stdin); kwargs...) = start_worker_mpi_rank0(stdout, cookie; kwargs...)
function start_worker_mpi_rank0(out::IO, cookie::AbstractString=readline(stdin); close_stdin::Bool=true, stderr_to_stdout::Bool=true)
    Distributed.init_multi()

    if close_stdin # workers will not use it
        redirect_stdin(devnull)
        close(stdin)
    end
    stderr_to_stdout && redirect_stderr(stdout)

    Distributed.init_worker(cookie)
    interface = IPv4(Distributed.LPROC.bind_addr)
    if Distributed.LPROC.bind_port == 0
        port_hint = 9000 + (getpid() % 1000)
        (port, sock) = listenany(interface, UInt16(port_hint))
        Distributed.LPROC.bind_port = port
    else
        sock = listen(interface, Distributed.LPROC.bind_port)
    end

    t = errormonitor(@async while isopen(sock)
        client = accept(sock)

        cookie_from_master = read(client, Distributed.HDR_COOKIE_LEN)
        if cookie_from_master[1] == 0x00
            error("received cookie with at least one null character")
        end

        if String(cookie_from_master) != cookie
            error("received invalid cookie.")
        end

        process_messages_mpi_rank0(client, client, false)
    end)
    print(out, "julia_worker:")  # print header
    print(out, "$(string(Distributed.LPROC.bind_port))#") # print port
    print(out, Distributed.LPROC.bind_addr)
    print(out, '\n')
    flush(out)

    Sockets.nagle(sock, false)
    Sockets.quickack(sock, true)

    if ccall(:jl_running_on_valgrind,Cint,()) != 0
        println(out, "PID = $(getpid())")
    end

    manager = azmanager()
    manager.worker_socket = out

    try
        while true
            Distributed.check_master_connect()
            @info "message loop..."
            wait(t)
            istaskfailed(t) && fetch(t)
            sleep(10)
        end
    catch e
        throw(e)
    finally
        close(sock)
    end
end

#
# Azure scale-set methods
#
function scaleset_image(manager::AzManager, sigimagename, sigimageversion, imagename)
    # early exit
    if imagename != "" || (sigimagename != "" && sigimageversion != "")
        return sigimagename, sigimageversion, imagename
    end

    # get machines' metadata
    t = @async begin
        r = HTTP.request("GET", "http://169.254.169.254/metadata/instance/compute/storageProfile/imageReference?api-version=2021-02-01", ["Metadata"=>"true"]; retry=false, redirect=false)
    end
    tic = time()
    while !istaskdone(t)
        (time() - tic) > 10 && break
        sleep(1)
    end

    istaskdone(t) || @async Base.throwto(t, InterruptException)
    r = fetch(t)

    local _image
    if !isa(r, HTTP.Messages.Response)
        return sigimagename, sigimageversion, imagename
    else
        r = fetch(t)
        image = JSON.parse(String(r.body))["id"]
        _image = split(image,"/")
    end

    k_galleries = findfirst(x->x=="galleries", _image)
    gallery = k_galleries == nothing ? "" : _image[k_galleries+1]
    different_image = true
    
    if sigimagename == "" && imagename == ""
        different_image = false
        k_images = findfirst(x->x=="images", _image)
        if k_galleries != nothing
            sigimagename = _image[k_images+1]
        else
            imagename = _image[k_images+1]
        end
    end

    (sigimagename != "" && gallery == "") && error("sigimagename provided, but gallery name not found in template")
    (sigimagename == "" && imagename == "") && error("Unable to determine 'image gallery name' or 'image name'")
    
    if imagename == "" && sigimageversion == ""
        k = findfirst(x->x=="versions", _image)
        if k != nothing && !different_image
            sigimageversion = _image[k+1]
        else
            k_subscriptions = findfirst(x->x=="subscriptions", _image)
            k_resourcegroups = findfirst(x->x=="resourceGroups", _image)
            if k_subscriptions != nothing && k_resourcegroups != nothing
                subscription = _image[k_subscriptions+1]
                resourcegroup = _image[k_resourcegroups+1]
                _r = @retry manager.nretry azrequest(
                    "GET",
                    manager.verbose,
                    "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourcegroup/providers/Microsoft.Compute/galleries/$gallery/images/$sigimagename/versions?api-version=2022-03-03",
                    ["Authorization"=>"Bearer $(token(manager.session))"])
                r = JSON.parse(String(_r.body))
                _versions,_r = getnextlinks!(manager, _r, get(r, "value", String[]), get(r, "nextLink", ""), manager.nretry, manager.verbose)
                versions = VersionNumber.(get.(_versions, "name", ""))
                if length(versions) > 0
                    sigimageversion = string(maximum(versions))
                end
            end
        end
    end

    @debug "after inspecting the VM metaddata, imagename=$imagename, sigimagename=$sigimagename, sigimageversion=$sigimageversion"

    sigimagename, sigimageversion, imagename
end

function image_osdisksize(manager::AzManager, template, sigimagename, sigimageversion, imagename)
    @debug "determining os disk size..."
    local imagerefs
    if haskey(template["properties"], "virtualMachineProfile") # scale-set template
        imagerefs = split(template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"], '/')
    else # vm template
        imagerefs = split(template["properties"]["storageProfile"]["imageReference"]["id"], '/')
    end

    k = findfirst(imageref->imageref=="subscriptions", imagerefs)
    subscription = k === nothing ? "" : imagerefs[k+1]

    k = findfirst(imageref->imageref=="resourceGroups", imagerefs)
    resourcegroup = k === nothing ? "" : imagerefs[k+1]

    k = findfirst(imageref->imageref=="galleries", imagerefs)
    gallery = k === nothing ? "" : imagerefs[k+1]

    osdisksize = 0
    if imagename != "" && sigimagename == "" && sigimageversion == ""
        r = @retry manager.nretry azrequest(
            "GET",
            manager.verbose,
            "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourcegroup/providers/Microsoft.Compute/images/$imagename?api-version=2023-03-01",
            ["Authorization"=>"Bearer $(token(manager.session))"]
        )
        b = JSON.parse(String(r.body))
        osdisksize = b["properties"]["storageProfile"]["osDisk"]["diskSizeGB"]
    elseif imagename == "" && sigimagename != "" && sigimageversion != ""
        r = @retry manager.nretry azrequest(
            "GET",
            manager.verbose,
            "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourcegroup/providers/Microsoft.Compute/galleries/$gallery/images/$sigimagename/versions/$sigimageversion?api-version=2022-03-03",
            ["Authorization"=>"Bearer $(token(manager.session))"]
        )
        b = JSON.parse(String(r.body))
        osdisksize = b["properties"]["storageProfile"]["osDiskImage"]["sizeInGB"]
    else
        error("unable to determine os disk size")
    end

    @debug "found os disk size: $osdisksize GB"

    osdisksize
end

function scaleset_image!(manager::AzManager, template, sigimagename, sigimageversion, imagename)
    if imagename != ""
        if haskey(template["properties"], "virtualMachineProfile") # scale-set
            id = template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"]
            template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"] = join(split(id, '/')[1:end-4], '/')*"/images/"*imagename
        else # vm
            id = template["properties"]["storageProfile"]["imageReference"]["id"]
            template["properties"]["storageProfile"]["imageReference"]["id"] = join(split(id, '/')[1:end-4], '/')*"/images/"*imagename
        end
    else
        if sigimagename != ""
            if haskey(template["properties"], "virtualMachineProfile") # scale-set
                id = split(template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"], '/')
                j = findfirst(_id->_id=="images", id)
                template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"] = join(id[1:j], '/')*"/"*sigimagename
            else # vm
                id = split(template["properties"]["storageProfile"]["imageReference"]["id"], '/')
                j = findfirst(_id->_id=="images", id)
                template["properties"]["storageProfile"]["imageReference"]["id"] = join(id[1:j], '/')*"/"*sigimagename
            end
        end

        if sigimageversion != ""
            if haskey(template["properties"], "virtualMachineProfile") # scale-set
                id = split(template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"], '/')
                j = findfirst(_id->_id=="versions", id)
                if j == nothing
                    template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"] *= "/versions/$sigimageversion"
                else
                    template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"] = join(id[1:j], '/')*sigimageversion
                end
            else # vm
                id = split(template["properties"]["storageProfile"]["imageReference"]["id"], '/')
                j = findfirst(_id->_id=="versions", id)
                if j == nothing
                    template["properties"]["storageProfile"]["imageReference"]["id"] *= "/versions/$sigimageversion"
                else
                    template["properties"]["storageProfile"]["imageReference"]["id"] = join(id[1:j], '/')*sigimageversion
                end
            end
        end
    end

    if haskey(template["properties"], "virtualMachineProfile") # scale-set
        @debug "using image=$(template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"])"
    else # vm
        @debug "using image=$(template["properties"]["storageProfile"]["imageReference"]["id"])"
    end
end

function software_sanity_check(manager, imagename, custom_environment)
    projectinfo = Pkg.project()
    envpath = normpath(joinpath(projectinfo.path, ".."))
    _packages = TOML.parse(read(joinpath(envpath, "Manifest.toml"), String))

    packages = VERSION < v"1.7" ? _packages : _packages["deps"]

    if custom_environment
        for (packagename, packageinfo) in packages
            if haskey(packageinfo[1], "path")
                error("Project/environment has dev'd packages that will not be accessible from workers.")
            end
        end
    end
end

function compress_environment(julia_environment_folder)
    project_text = read(joinpath(julia_environment_folder, "Project.toml"), String)
    manifest_text = read(joinpath(julia_environment_folder, "Manifest.toml"), String)
    localpreferences_text = isfile(joinpath(julia_environment_folder, "LocalPreferences.toml")) ? read(joinpath(julia_environment_folder, "LocalPreferences.toml"), String) : ""
    local project_compressed,manifest_compressed,localpreferences_compressed
    with_logger(ConsoleLogger(stdout, Logging.Info)) do
        project_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(project_text)))
        manifest_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(manifest_text)))
        localpreferences_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(localpreferences_text)))
    end

    project_compressed, manifest_compressed, localpreferences_compressed
end

function decompress_environment(project_compressed, manifest_compressed, localpreferences_compressed, remote_julia_environment_name)
    mkpath(joinpath(Pkg.envdir(), remote_julia_environment_name))

    text = String(CodecZlib.transcode(ZlibDecompressor, Vector{UInt8}(base64decode(project_compressed))))
    write(joinpath(Pkg.envdir(), remote_julia_environment_name, "Project.toml"), text)
    text = String(CodecZlib.transcode(ZlibDecompressor, Vector{UInt8}(base64decode(manifest_compressed))))
    write(joinpath(Pkg.envdir(), remote_julia_environment_name, "Manifest.toml"), text)
    text = String(CodecZlib.transcode(ZlibDecompressor, Vector{UInt8}(base64decode(localpreferences_compressed))))
    if text != ""
        write(joinpath(Pkg.envdir(), remote_julia_environment_name, "LocalPreferences.toml"), text)
    end
end

function nvidia_has_nvidia_smi()
    if Sys.which("nvidia-smi") === nothing
        return false
    end
    p = open(`nvidia-smi`)
    wait(p)
    success(p)
end

function nvidia_gpumode(feature)
    p = open(`nvidia-smi --query-gpu=$feature.mode.current --format=csv`)
    wait(p)
    isenabled = Bool[]
    if success(p)
        for line in readlines(p)
            _line = lowercase(line)
            _line == "$feature.mode.current" || push!(isenabled, lowercase(line) == "enabled")
        end
    else
        @warn "unable to retrieve status for feature='$feature'"
    end
    @info "NVIDIA $feature is $isenabled"
    isenabled
end

function nvidia_gpumode!(feature, switch)
    _switch = switch ? 1 : 0
    p = open(`sudo nvidia-smi $feature $_switch`)
    wait(p)
    success(p) || @error "unable to toggle NVIDIA GPU feature='$feature' to '$_switch'."
    @info "NVIDIA $feature is toggled to $_switch"
end

function nvidia_gpucheck(enable_ecc=true, enable_mig=false)
    if !nvidia_has_nvidia_smi()
        @info "no NVIDIA devices detected."
        return
    end

    # turn on/off ECC?
    ecc_isenabled = nvidia_gpumode("ecc")
    switch_ecc = (!all(ecc_isenabled) && enable_ecc) || (any(ecc_isenabled) && !enable_ecc)
    switch_ecc && nvidia_gpumode!("-e", enable_ecc)

    # turn on/off MIG
    mig_isenabled = nvidia_gpumode("mig")
    switch_mig = (!all(mig_isenabled) && enable_mig) || (any(mig_isenabled) && !enable_mig)
    switch_mig && nvidia_gpumode!("-mig", enable_mig)

    if switch_mig || switch_ecc
        @info "rebooting so that change to nvidia settings take effect."
        run(`sudo reboot`)
    end
end

function buildstartupscript(manager::AzManager, user::String, disk::AbstractString, custom_environment::Bool)
    cmd = """
    #!/bin/sh
    $disk
    sed -i 's/ scripts-user/ [scripts-user, always]/g' /etc/cloud/cloud.cfg
    """
    
    if isfile(joinpath(homedir(), ".gitconfig"))
        gitconfig = read(joinpath(homedir(), ".gitconfig"), String)
        cmd *= """
        
        sudo su - $user << EOF
        echo '$gitconfig' > ~/.gitconfig
        EOF
        """
    end
    if isfile(joinpath(homedir(), ".git-credentials"))
        gitcredentials = rstrip(read(joinpath(homedir(), ".git-credentials"), String), [' ','\n'])
        cmd *= """
        
        sudo su - $user << EOF
        echo "$gitcredentials" > ~/.git-credentials
        chmod 600 ~/.git-credentials
        EOF
        """
    end

    remote_julia_environment_name = ""
    if custom_environment
        try
            projectinfo = Pkg.project()
            julia_environment_folder = normpath(joinpath(projectinfo.path, ".."))

            #=
            There is no guarantee that `julia_environment_folder` will exist on the worker.
            Therefore, we will put the environment into a sub-folder of Pkg.envdir().
            =#
            remote_julia_environment_name = splitpath(julia_environment_folder)[end]

            project_compressed, manifest_compressed, localpreferences_compressed = compress_environment(julia_environment_folder)

            cmd *= """
            
            sudo su - $user <<'EOF'
            julia -e 'using AzManagers; AzManagers.decompress_environment("$project_compressed", "$manifest_compressed", "$localpreferences_compressed", "$remote_julia_environment_name")'
            julia -e 'using Pkg; path=joinpath(Pkg.envdir(), "$remote_julia_environment_name"); Pkg.Registry.update(); Pkg.activate(path); (retry(Pkg.instantiate))(); Pkg.precompile()'
            EOF
            """
        catch e
            @warn "Unable to use a custom environment."
            logerror(e, Logging.Warn)
        end
    end

    cmd, remote_julia_environment_name
end

function build_envstring(env::Dict)
    envstring = ""
    for (key,value) in env
        envstring *= "export $key=$value\n"
    end
    envstring
end

function buildstartupscript_cluster(manager::AzManager, spot::Bool, ppi::Int, mpi_ranks_per_worker::Int, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig, julia_num_threads::String, omp_num_threads::Int, exeflags::String, env::Dict, user::String,
        disk::AbstractString, custom_environment::Bool)
    cmd, remote_julia_environment_name = buildstartupscript(manager, user, disk, custom_environment)

    cookie = Distributed.cluster_cookie()
    master_address = string(getipaddr())
    master_port = manager.port

    envstring = build_envstring(env)

    juliaenvstring = remote_julia_environment_name == "" ? "" : """using Pkg; Pkg.activate(joinpath(Pkg.envdir(), "$remote_julia_environment_name")); """

    _exeflags = isempty(exeflags) ? "-t $julia_num_threads" : "$exeflags -t $julia_num_threads"

    if mpi_ranks_per_worker == 0
        cmd *= """

        sudo su - $user <<EOF
        export JULIA_WORKER_TIMEOUT=$(get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
        export OMP_NUM_THREADS=$omp_num_threads
        $envstring
        julia $_exeflags -e '$(juliaenvstring)using AzManagers; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks(); AzManagers.azure_worker("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'
        EOF
        """
    else
        cmd *= """

        sudo su - $user <<EOF
        export JULIA_WORKER_TIMEOUT=$(get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
        export OMP_NUM_THREADS=$omp_num_threads
        $envstring
        julia -e '$(juliaenvstring)using AzManagers; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks()'
        mpirun -n $mpi_ranks_per_worker $mpi_flags julia $_exeflags -e '$(juliaenvstring)using AzManagers, MPI; AzManagers.azure_worker_mpi("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'
        EOF
        """
    end

    cmd
end

function buildstartupscript_detached(manager::AzManager, julia_num_threads::String, omp_num_threads::Int, env::Dict, user::String,
        disk::AbstractString, custom_environment::Bool, subscriptionid, resourcegroup, vmname)
    cmd, remote_julia_environment_name = buildstartupscript(manager, user, disk, custom_environment)

    envstring = build_envstring(env)

    juliaenvstring = remote_julia_environment_name == "" ? "" : """using Pkg; Pkg.activate(joinpath(Pkg.envdir(), "$remote_julia_environment_name")); """

    cmd *= """

    sudo su - $user <<EOF
    $envstring
    export JULIA_WORKER_TIMEOUT=$(get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
    export OMP_NUM_THREADS=$omp_num_threads
    ssh-keygen -f /home/$user/.ssh/azmanagers_rsa -N '' <<<y
    cd /home/$user
    julia -t $julia_num_threads -e '$(juliaenvstring)using AzManagers; AzManagers.mount_datadisks(); AzManagers.detached_port!($(AzManagers.detached_port())); AzManagers.detachedservice(;subscriptionid="$subscriptionid", resourcegroup="$resourcegroup", vmname="$vmname")'
    EOF
    """

    cmd
end

function quotacheck(manager, subscriptionid, template, δn, nretry, verbose)
    location = template["location"]

    # get a mapping from vm-size to vm-family
    f = HTTP.escapeuri("location eq '$location'")

    # resources in southcentralus
    target = "https://management.azure.com/subscriptions/$subscriptionid/providers/Microsoft.Compute/skus?api-version=2021-07-01&\$filter=$f"
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        target,
        ["Authorization"=>"Bearer $(token(manager.session))"])

    if manager.show_quota
        @info "Quota after getting skus" remaining_resource(_r)
    end

    resources = JSON.parse(String(_r.body))["value"]

    # filter to get only virtualMachines, TODO - can this filter be done in the above REST call?
    vms = filter(resource->resource["resourceType"]=="virtualMachines", resources)

    # find the vm in the resources list
    local k
    if haskey(template, "sku")
        k = findfirst(vm->vm["name"]==template["sku"]["name"], vms) # for scale-set templates
    else
        k = findfirst(vm->vm["name"]==template["properties"]["hardwareProfile"]["vmSize"], vms) # for vm templates
    end

    if k == nothing
        if haskey(template, "sku")
            error("VM size $(template["sku"]["name"]) not found") # for scale-set templates
        else
            error("VM size $(template["properties"]["hardwareProfile"]["vmSize"]) not found") # for vm templates
        end
    end

    family = vms[k]["family"]
    capabilities = vms[k]["capabilities"]
    k = findfirst(capability->capability["name"]=="vCPUs", capabilities)

    if k == nothing
        error("unable to find vCPUs capability in resource")
    end

    ncores_per_machine = parse(Int, capabilities[k]["value"])

    # get usage in our location
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/providers/Microsoft.Compute/locations/$location)/usages?api-version=2019-07-01",
        ["Authorization"=>"Bearer $(token(manager.session))"])
    r = JSON.parse(String(_r.body))

    if manager.show_quota
        @info "Quota after getting quota usage" remaining_resource(_r)
    end

    usages = r["value"]

    k = findfirst(usage->usage["name"]["value"]==family, usages)

    if k == nothing
        error("unable to find SKU family in usages while chcking quota")
    end

    ncores_limit = r["value"][k]["limit"]
    ncores_current = r["value"][k]["currentValue"]
    ncores_available = ncores_limit - ncores_current

    k = findfirst(usage->usage["name"]["value"]=="lowPriorityCores", usages)

    if k == nothing
        error("unable to find low-priority CPU limit while checking quota")
    end
    ncores_spot_limit = r["value"][k]["limit"]
    ncores_spot_current = r["value"][k]["currentValue"]
    ncores_spot_available = ncores_spot_limit - ncores_spot_current

    ncores_available - (ncores_per_machine * δn), ncores_spot_available - (ncores_per_machine * δn)
end

function getnextlinks!(manager::AzManager, _r, value, nextlink, nretry, verbose)
    while nextlink != ""
        _r = @retry nretry azrequest(
            "GET",
            verbose,
            nextlink,
            ["Authorization"=>"Bearer $(token(manager.session))"])
        r = JSON.parse(String(_r.body))
        value = [value;get(r,"value",[])]
        nextlink = get(r, "nextLink", "")
    end
    value, _r
end

function resourcegraphrequest(manager, body)
    skiptoken = ""
    data = []
    local _r
    while true
        if skiptoken != ""
            body["\$skipToken"] = skiptoken
        end
        _r = @retry manager.nretry azrequest(
            "POST",
            manager.verbose,
            "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01",
            ["Authorization"=>"Bearer $(token(manager.session))", "Content-Type"=>"application/json"],
            json(body)
        )
        r = JSON.parse(String(_r.body))
        data = [data;get(r, "data", [])]
        skiptoken = get(r, "\$skipToken", "")

        if skiptoken == ""
            break
        end
    end

    if manager.show_quota
        @info "Quota after getting instances for scaleset pruning" remaining_resource(_r)
    end

    data
end

function list_scalesets(manager::AzManager, subscriptionid, resourcegroup, nretry, verbose)
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2023-03-01",
        ["Authorization"=>"Bearer $(token(manager.session))"])
    r = JSON.parse(String(_r.body))
    scalesets,_r = getnextlinks!(manager, _r, get(r, "value", []), get(r, "nextLink", ""), nretry, verbose)
    [get(scaleset, "name", "") for scaleset in scalesets]
end

function list_scaleset_vms_uniform(manager, scaleset)
    _r = @retry manager.nretry azrequest(
            "GET",
            manager.verbose,
            "https://management.azure.com/subscriptions/$(scaleset.subscriptionid)/resourceGroups/$(scaleset.resourcegroup)/providers/Microsoft.Compute/virtualMachineScaleSets/$(scaleset.scalesetname)/virtualMachines?api-version=2022-11-01",
            ["Authorization"=>"Bearer $(token(manager.session))"])
    r = JSON.parse(String(_r.body))
    vms,_r = getnextlinks!(manager, _r, get(r, "value", []), get(r, "nextLink", ""), manager.nretry, manager.verbose)

    if manager.show_quota
        @info "Quota after getting instances for scaleset pruning" remaining_resource(_r)
    end

    vms
end

function list_scaleset_vms_flexible(manager, scaleset)
    body = Dict(
            "subscriptions" => [
                scaleset.subscriptionid
            ],
            "query" => "Resources | where type =~ \"Microsoft.Compute/virtualMachines\" | where resourceGroup =~ \"$(scaleset.resourcegroup)\" | where properties.virtualMachineScaleSet.id contains \"$(scaleset.scalesetname)\" | project id,name,properties"
        )
    vms = resourcegraphrequest(manager, body)
    vms
end

function list_scaleset_vms(manager, scaleset)
    local vms, _r
    try
        _r = @retry manager.nretry azrequest(
            "GET",
            manager.verbose,
            "https://management.azure.com/subscriptions/$(scaleset.subscriptionid)/resourceGroups/$(scaleset.resourcegroup)/providers/Microsoft.Compute/virtualMachineScaleSets/$(scaleset.scalesetname)?api-version=2023-03-01",
            ["Authorization"=>"Bearer $(token(manager.session))"])
    catch e
        if status(e) == 404
            # the scale-set does not exist, so the set of vms is empty
            return []
        end
    end
    r = JSON.parse(String(_r.body))

    local vms
    if get(get(r, "properties", Dict()), "orchestrationMode", "Uniform") == "Flexible"
        vms = list_scaleset_vms_flexible(manager, scaleset)
    else
        vms = list_scaleset_vms_uniform(manager, scaleset)
    end
    vms
end

function scaleset_capacity(manager::AzManager, subscriptionid, resourcegroup, scalesetname, nretry, verbose)
    local r
    try
        _r = @retry nretry azrequest(
            "GET",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname?api-version=2023-03-01",
            ["Authorization"=>"Bearer $(token(manager.session))"])
        r = JSON.parse(String(_r.body))
    catch e
        if status(e) == 404
            return 0
        end
        throw(e)
    end

    if manager.show_quota
        @info "Quota after getting scale set capacity" remaining_resource(_r)
    end

    r["sku"]["capacity"]
end

function scaleset_capacity!(manager::AzManager, subscriptionid, resourcegroup, scalesetname, capacity, nretry, verbose)
    @retry nretry azrequest(
        "PATCH",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname?api-version=2023-03-01",
        ["Authorization"=>"Bearer $(token(manager.session))", "Content-Type"=>"application/json"],
        json(Dict("sku"=>Dict("capacity"=>capacity))))
end

function scaleset_create_or_update(manager::AzManager, user, subscriptionid, resourcegroup, scalesetname, sigimagename, sigimageversion,
        imagename, osdisksize, nretry, template, δn, ppi, mpi_ranks_per_worker, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig, hyperthreading, julia_num_threads,
        omp_num_threads, exeflags, env, spot, maxprice, spot_base_regular_priority_count, spot_regular_percentage_above_base, verbose, custom_environment, overprovision)
    load_manifest()
    ssh_key = _manifest["ssh_public_key_file"]

    @debug "scaleset_create_or_update"
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2023-03-01",
        ["Authorization"=>"Bearer $(token(manager.session))"])
    r = JSON.parse(String(_r.body))

    if manager.show_quota
        @info "Quota after getting a list of existing scale-sets" remaining_resource(_r)
    end

    _template = deepcopy(template["value"])

    _template["properties"]["virtualMachineProfile"]["osProfile"]["computerNamePrefix"] = string(scalesetname, "-")

    _template["properties"]["virtualMachineProfile"]["storageProfile"]["osDisk"]["diskSizeGB"] = osdisksize

    _t = token(manager.session)
    _e = JSONWebTokens.None()
    _decoded = JSONWebTokens.decode(_e, _t)
    if haskey(_decoded, "unique_name")
        _user = _decoded["unique_name"]

        if !haskey(_template, "tags")
            _template["tags"] = Dict{Any,Any}()
        end
        _template["tags"]["UserUniqueName"] = _user
    end

    key = Dict("path" => "/home/$user/.ssh/authorized_keys", "keyData" => read(ssh_key, String))
    push!(_template["properties"]["virtualMachineProfile"]["osProfile"]["linuxConfiguration"]["ssh"]["publicKeys"], key)
    
    cmd = buildstartupscript_cluster(manager, spot, ppi, mpi_ranks_per_worker, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig, julia_num_threads, omp_num_threads, exeflags, env, user, template["tempdisk"], custom_environment)
    _cmd = base64encode(cmd)

    if length(_cmd) > 64_000
        error("cloud init custom data is too large.")
    end

    if overprovision
        _template["properties"]["overprovision"] = true
        _template["properties"]["doNotRunExtensionsOnOverprovisionedVMs"] = true
    else
        _template["properties"]["overprovision"] = false
    end
    _template["properties"]["virtualMachineProfile"]["osProfile"]["customData"] = _cmd

    if spot
        _template["properties"]["virtualMachineProfile"]["priority"] = "Spot"
        _template["properties"]["virtualMachineProfile"]["evictionPolicy"] = "Delete"
        _template["properties"]["virtualMachineProfile"]["billingProfile"] = Dict("maxPrice"=>maxprice)

        if spot_base_regular_priority_count > 0 || spot_regular_percentage_above_base > 0
            _template["properties"]["orchestrationMode"] = "Flexible"
            _template["properties"]["virtualMachineProfile"]["networkProfile"]["networkApiVersion"] = "2020-11-01"
            _template["properties"]["priorityMixPolicy"] = Dict("baseRegularPriorityCount" => spot_base_regular_priority_count, "regularPriorityPercentageAboveBase" => spot_regular_percentage_above_base)

            # the following seems to be required for "flexible" orchestration mode
            _template["properties"]["platformFaultDomainCount"] = 1
            haskey(_template["properties"], "overprovision") && (delete!(_template["properties"], "overprovision"))
            haskey(_template["properties"], "doNotRunExtensionsOnOverprovisionedVMs") && (delete!(_template["properties"], "doNotRunExtensionsOnOverprovisionedVMs"))
            haskey(_template["properties"], "upgradePolicy") && (delete!(_template["properties"], "upgradePolicy"))
            #
        end
    end

    if hyperthreading !== nothing
        if !haskey(_template, "tags")
            _template["tags"] = Dict{Any,Any}()
        end
        _template["tags"]["platformsettings.host_environment.disablehyperthreading"] = hyperthreading ? "False" : "True"
    end

    n = 0
    scalesets = get(r, "value", [])
    scaleset_exists = false
    for scaleset in scalesets
        if scaleset["name"] == scalesetname
            n = scaleset_capacity(manager, subscriptionid, resourcegroup, scalesetname, nretry, verbose)
            scaleset_exists = true
            break
        end
    end
    n += δn

    @debug "about to check quota"

    # check usage/quotas
    while true
        navailable_cores, navailable_cores_spot = quotacheck(manager, subscriptionid, _template, δn, nretry, verbose)
        if spot
            navailable_cores_spot >= 0 && break
            @warn "Insufficient spot quota, $(-navailable_cores_spot) too few cores left in quota.  Sleeping for 60 seconds before trying again.  Ctrl-C to cancel."
        else
            navailable_cores >= 0 && break
            @warn "Insufficient quota, $(-navailable_cores) too few cores left in quota. Sleeping for 60 seconds before trying again. Ctrl-C to cancel."
        end

        try
            sleep(60)
        catch e
            isa(e, InterruptException) || rethrow(e)
            return -1
        end
    end

    @debug "done checking quota, δn=$(δn), n=$n"

    _template["sku"]["capacity"] = n
    _r = @retry nretry azrequest(
        "PUT",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname?api-version=2023-03-01",
        ["Content-type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"],
        String(json(_template)))

    if manager.show_quota
        @info "Quota after requesting that the scale-set is created or grows" remaining_resource(_r)
    end

    n
end

function delete_vms(manager::AzManager, subscriptionid, resourcegroup, scalesetname, ids, nretry, verbose)
    body = Dict("instanceIds"=>ids)
    _r = @retry nretry azrequest(
        "POST",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/delete?forceDeletion=True&api-version=2023-07-01",
        ["Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"],
        json(body))

    if manager.show_quota
        @info "Quota after requesting deletion of $(length(ids)) virtual machines" remaining_resource(_r)
    end
end

# see https://docs.microsoft.com/en-us/azure/virtual-machines/linux/add-disk
function mount_datadisks()
    try
        @info "mounting data disks"
        _r = HTTP.request("GET", "http://169.254.169.254/metadata/instance?api-version=2021-02-01", ["Metadata"=>"true"]; redirect=false)
        r = JSON.parse(String(_r.body))
        luns = String[]
        for datadisks in r["compute"]["storageProfile"]["dataDisks"]
            push!(luns, datadisks["lun"])
        end

        blks = JSON.parse(String(read(open(`lsblk -J -o NAME,HCTL,MOUNTPOINTS,TYPE`))))
        for blk in blks["blockdevices"]
            hctl = blk["hctl"]
            mountpoints = blk["mountpoints"]
            type = blk["type"]
            if hctl != nothing && type == "disk" && !haskey(blk, "children") && !isempty(mountpoints) && mountpoints[1] === nothing
                lun = split(hctl,':')[end]
                if lun ∈ luns
                    try
                        name = blk["name"]
                        @info "mounting data disk with lun $lun ($name)..."
                        run(`sudo parted /dev/$name --script mklabel gpt mkpart xfspart xfs 0% 100%`)
                        sleep(1) # I'm not sure why this is needed, but the following command often fails without it
                        run(`sudo mkfs.xfs /dev/$(name)1`)
                        run(`sudo partprobe /dev/$(name)1`)
                        run(`sudo mkdir /scratch$lun`)
                        run(`sudo mount /dev/$(name)1 /scratch$lun`)
                        run(`sudo chmod 777 /scratch$lun`)
                        @info "done mounting data disk with lun $lun ($name)"
                    catch e
                        @error "caught error formatting mounting data disk lun=$lun ($name)"
                        logerror(e, Logging.Error)
                        run(`sudo rm -rf /scratch$lun`)
                    end
                end
            end
        end
    catch
        @error "caught error formatting/mounting data disks"
        logerror(e, Logging.Error)
    end
end

function simulate_spot_eviction(pid)
    if pid == 1
        return
    end
    instanceid = Distributed.map_pid_wrkr[pid].config.userdata["instanceid"]
    subscriptionid = Distributed.map_pid_wrkr[pid].config.userdata["subscriptionid"]
    resourcegroup = Distributed.map_pid_wrkr[pid].config.userdata["resourcegroup"]
    scalesetname = Distributed.map_pid_wrkr[pid].config.userdata["scalesetname"]

    manager = azmanager()
    session = manager.session

    HTTP.request(
        "POST",
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/virtualMachines/$instanceid/simulateEviction?api-version=2023-03-01",
        ["Authorization" => "Bearer $(token(session))"])
    nothing
end

function get_ipaddress_for_scaleset_vm(manager, vm)
    id = vm["properties"]["networkProfile"]["networkInterfaces"][1]["id"]

    _r = @retry manager.nretry azrequest(
        "GET",
        manager.verbose,
        "https://management.azure.com/$id?api-version=2023-09-01",
        ["Authorization"=>"Bearer $(token(manager.session))"])

    r = JSON.parse(String(_r.body))
    properties = r["properties"]["ipConfigurations"][1]["properties"]
    get(properties, "publicIPAddress", get(properties, "privateIPAddress", ""))
end

#
# detached service and REST API
#
const DETACHED_ROUTER = HTTP.Router()
const DETACHED_JOBS = Dict()
const DETACHED_VM = Ref(Dict())

let DETACHED_ID::Int = 1
    global detached_nextid
    detached_nextid() = (id = DETACHED_ID; DETACHED_ID += 1; id)
end

let DETACHED_PORT::Int = 8081
    global detached_port
    detached_port() = DETACHED_PORT
    global detached_port!
    detached_port!(port) = DETACHED_PORT = port
end

function timestamp_metaformatter(level::Logging.LogLevel, _module, group, id, file, line)
    @nospecialize
    timestamp = Dates.format(now(Dates.UTC), "yyyy-mm-ddTHH:MM:SS")
    color = Logging.default_logcolor(level)
    prefix = timestamp*" - "*(level == Logging.Warn ? "Warning" : string(level))*':'
    suffix = ""
    color, prefix, suffix
end

function detachedservice(address=ip"0.0.0.0"; server=nothing, subscriptionid="", resourcegroup="", vmname="")
    HTTP.register!(DETACHED_ROUTER, "POST", "/cofii/detached/run", detachedrun)
    HTTP.register!(DETACHED_ROUTER, "POST", "/cofii/detached/job/*/kill", detachedkill)
    HTTP.register!(DETACHED_ROUTER, "POST", "/cofii/detached/job/*/wait", detachedwait)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/status", detachedstatus)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/stdout", detachedstdout)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/stderr", detachedstderr)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/ping", detachedping)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/vm", detachedvminfo)

    port = detached_port()

    AzManagers.DETACHED_VM[] = Dict("subscriptionid"=>string(subscriptionid), "resourcegroup"=>string(resourcegroup),
        "name"=>string(vmname), "ip"=>string(getipaddr()), "port"=>string(port))

    global_logger(ConsoleLogger(stdout, Logging.Info; meta_formatter=timestamp_metaformatter))

    HTTP.serve(DETACHED_ROUTER, address, port; server=server)
end

function detachedrun(request::HTTP.Request)
    @info "inside detachedrun"
    local process, id, pid, r

    try
        r = JSON.parse(String(HTTP.payload(request)))

        if !haskey(r, "code")
            return HTTP.Response(400, ["Content-Type"=>"application/json"], json(Dict("error"=>"Malformed body: JSON body must contain the key: code")); request)
        end

        _tempname_logging = tempname(;cleanup=false)
        write(_tempname_logging, """using Logging; global_logger(ConsoleLogger(stdout, Logging.Info))""")

        _tempname_varbundle = tempname(;cleanup=false)
        if haskey(r, "variablebundle")
            write(_tempname_varbundle, """using AzManagers, Base64, Serialization; variablebundle!(deserialize(IOBuffer(base64decode("$(r["variablebundle"])"))))\n""")
        else
            write(_tempname_varbundle, "\n")
        end

        code = r["code"]
        codelines = split(code, "\n")

        if strip(codelines[1]) == "begin"
            popfirst!(codelines)
            while length(codelines) > 0
                occursin("end", pop!(codelines)) && break
            end
        end
        if length(codelines) == 0
            return HTTP.Response(400, ["Content-Type"=>"application/json"], json(Dict("error"=>"No code to execute, missing end?", "code"=>code)); request)
        end

        code = join(codelines, "\n")

        _tempname = tempname(;cleanup=false)
        write(_tempname, code)

        id = detached_nextid()
        outfile = "job-$id.out"
        errfile = "job-$id.err"
        wrapper_code = """
        open("$outfile", "w") do out
            open("$errfile", "w") do err
                redirect_stdout(out) do
                    redirect_stderr(err) do
                        include("$_tempname_logging")
                        include("$_tempname_varbundle")
                        try
                            include("$_tempname")
                        catch e
                            for (exc, bt) in Base.catch_stack()
                                showerror(stderr, exc, bt)
                                println(stderr)
                            end
                            write(stderr, "\\n\\n")
                            title = "Code listing ($_tempname)"
                            write(stderr, title*"\\n")
                            nlines = countlines("$_tempname")
                            pad = nlines > 0 ? floor(Int,log10(nlines)) : 0
                            for (iline,line) in enumerate(readlines("$_tempname"))
                                write(stderr, "\$(lpad(iline,pad)): \$line\\n")
                            end
                            write(stderr, "\\n")
                            flush(stderr)
                            throw(e)
                        end
                    end
                end
            end
        end
        """

        _tempname_wrapper = tempname(;cleanup=false)
        write(_tempname_wrapper, wrapper_code)

        nthreads = Threads.nthreads()
        projectdir = dirname(Pkg.project().path)
        process = open(`julia -t $nthreads --project=$projectdir $_tempname_wrapper`)
        pid = getpid(process)
        @info "executing $_tempname_wrapper with $nthreads threads, and pid $pid"

        DETACHED_JOBS[string(id)] = Dict("process"=>process, "request"=>request, "stdout"=>outfile, "stderr"=>errfile, "codefile"=>_tempname, "code"=>code)
    catch e
        io = IOBuffer()
        logerror(e, Logging.Warn)
        return HTTP.Response(500, ["Content-Type"=>"application/json"], json(Dict("error"=>String(take!(io)))); request)
    end

    Threads.@spawn begin
        try
            wait(process)
        catch
        end
        if !r["persist"]
            vm = AzManagers.DETACHED_VM[]
            rmproc(vm; session=sessionbundle(:management))
        end
    end
    HTTP.Response(200, ["Content-Type"=>"application/json"], json(Dict("id"=>id, "pid"=>pid)); request)
end

function detachedkill(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    local _process
    try
        _process = DETACHED_JOBS[string(id)]["process"]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "unable to find process id in job $id"; request)
    end

    local response
    try
        kill(_process)
        response = HTTP.Response(200, ["Content-Type"=>"application/text"], "process for job $id killed"; request)
    catch
        response = HTTP.Response(500, ["Content-Type"=>"application/text"], "error deleting process id for job $id"; request)
    end
    response
end

function detachedstatus(request::HTTP.Request)
    @info "inside detachedstatus"
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Job with id=$id does not exist."; request)
    end

    local status
    try
        process = DETACHED_JOBS[id]["process"]

        if process_exited(process)
            status = success(process) ? "done" : "failed"
        elseif process_running(process)
            status = "running"
        else
            status = "starting"
        end
    catch e
        return HTTP.Response(500, ["Content-Type"=>"application/json"], json(Dict("error"=>show(e), "trace"=>show(stacktrace()))); request)
    end
    HTTP.Response(200, ["Content-Type"=>"application/json"], json(Dict("id"=>id, "status"=>status)); request)
end

function detachedstdout(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Job with id=$id does not exist."; request)
    end

    local stdout
    if isfile(DETACHED_JOBS[id]["stdout"])
        stdout = read(DETACHED_JOBS[id]["stdout"])
    else
        stdout = ""
    end
    HTTP.Response(200, ["Content-Type"=>"application/text"], stdout; request)
end

function detachedstderr(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Job with id=$id does not exist."; request)
    end

    local stderr
    if isfile(DETACHED_JOBS[id]["stderr"])
        stderr = read(DETACHED_JOBS[id]["stderr"])
    else
        stderr = ""
    end

    HTTP.Response(200, ["Content-Type"=>"application/text"], stderr; request)
end

function detachedwait(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(400, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(400, ["Content-Type"=>"application/string"], "ERROR: Job with id=$id does not exist."; request)
    end

    try
        process = DETACHED_JOBS[id]["process"]
        wait(process)
    catch e
        logerror(e, Logging.Error)

        write(io, "\n\n")

        title = "Code listing ($(DETACHED_JOBS[id]["codefile"]))"
        write(io, title*"\n")
        write(io, "-"^length(title)*"\n")
        lines = split(DETACHED_JOBS[id]["code"])
        nlines = length(lines)
        pad = nlines > 0 ? floor(Int,log10(nlines)) : 0
        for (iline,line) in enumerate(lines)
            write(io, "$(lpad(iline,pad)): $line\n")
        end
        write(io, "\n")

        return HTTP.Response(400, ["Content-Type"=>"application/json"], json(Dict("error"=>String(take!(io)))); request)
    end
    HTTP.Response(200, ["Content-Type"=>"application/text"], "OK, job $id is finished"; request)
end

function detachedping(request::HTTP.Request)
    HTTP.Response(200, ["Content-Type"=>"applicaton/text"], "OK"; request)
end

function detachedvminfo(request::HTTP.Request)
    HTTP.Response(200, ["Content-Type"=>"application/json"], json(AzManagers.DETACHED_VM[]); request)
end

#
# detached service client API
#
"""
    addproc(template[; name="", basename="cbox", subscriptionid="myid", resourcegroup="mygroup", nretry=10, verbose=0, session=AzSession(;lazy=true), sigimagename="", sigimageversion="", imagename="", detachedservice=true])

Create a VM, and returns a named tuple `(name,ip,resourcegrup,subscriptionid)` where `name` is the name of the VM, and `ip` is the ip address of the VM.
`resourcegroup` and `subscriptionid` denote where the VM resides on Azure.

# Parameters
* `name=""` name for the VM.  If it is not an empty string, then the next paramter (`basename`) is ignored
* `basename="cbox"` base name for the VM, we append a random suffix to ensure uniqueness
* `subscriptionid=template["subscriptionid"]` if exists, or `AzManagers._manifest["subscriptionid"]` otherwise.
* `resourcegroup=template["resourcegroup"]` if exists, or `AzManagers._manifest["resourcegroup"]` otherwise.
* `session=AzSession(;lazy=true)` Session used for OAuth2 authentication
* `sigimagename=""` Azure shared image gallery image to use for the VM (defaults to the template's image)
* `sigimageversion=""` Azure shared image gallery image version to use for the VM (defaults to latest)
* `imagename=""` Azure image name used as an alternative to `sigimagename` and `sigimageversion` (used for development work)
* `osdisksize=60` Disk size of the OS disk in GB
* `customenv=false` If true, then send the current project environment to the workers where it will be instantiated.
* `nretry=10` Max retries for re-tryable REST call failures
* `verbose=0` Verbosity flag passes to HTTP.jl methods
* `show_quota=false` after various operation, show the "x-ms-rate-remaining-resource" response header.  Useful for debugging/understanding Azure quota's.
* `julia_num_threads="\$(Threads.nthreads(),\$(Threads.nthreads(:interactive))"` set the number of julia threads for the workers.[1]
* `omp_num_threads = get(ENV, "OMP_NUM_THREADS", 1)` set `OMP_NUM_THREADS` environment variable before starting the detached process
* `env=Dict()` Dictionary of environemnt variables that will be exported before starting the detached process
* `detachedservice=true` start the detached service allowing for RESTful remote code execution

# Notes
[1] Interactive threads are supported beginning in version 1.9 of Julia.  For earlier versions, the default for `julia_num_threads` is `Threads.nthreads()`.
"""
function addproc(vm_template::Dict, nic_template=nothing;
        name = "",
        basename = "cbox",
        user = "",
        subscriptionid = "",
        resourcegroup = "",
        session = AzSession(;lazy=true),
        customenv = false,
        sigimagename = "",
        sigimageversion = "",
        imagename = "",
        osdisksize = 60,
        nretry = 10,
        verbose = 0,
        show_quota = false,
        julia_num_threads = VERSION >= v"1.9" ? "$(Threads.nthreads()),$(Threads.nthreads(:interactive))" : string(Threads.nthreads()),
        omp_num_threads = parse(Int, get(ENV, "OMP_NUM_THREADS", "1")),
        env = Dict(),
        detachedservice = true)
    load_manifest()
    subscriptionid == "" && (subscriptionid = get(vm_template, "subscriptionid", _manifest["subscriptionid"]))
    resourcegroup == "" && (resourcegroup = get(vm_template, "resourcegroup", _manifest["resourcegroup"]))
    user == "" && (user = _manifest["ssh_user"])
    ssh_key =  AzManagers._manifest["ssh_public_key_file"]
    user == "" && (user = AzManagers._manifest["ssh_user"])

    timeout = Distributed.worker_timeout()

    vmname = name == "" ? basename*"-"*randstring('a':'z', 6) : name
    nicname = vmname*"-nic"

    if nic_template == nothing
        isfile(templates_filename_nic()) || error("if nic_template==nothing, then the file $(templates_filename_nic()) must exist.  See AzManagers.save_template_nic.")
        nic_templates = JSON.parse(read(templates_filename_nic(), String))
        _keys = keys(nic_templates)
        length(_keys) == 0 && error("if nic_template==nothing, then the file $(templates_filename_nic()) must contain at-least one template.  See AzManagers.save_template_nic.")
        nic_template = nic_templates[first(_keys)]
    elseif isa(nic_template, AbstractString)
        isfile(templates_filename_nic()) || error("if nic_template is a string, then the file $(templates_filename_nic()) must exist.  See AzManagers.save_template_nic.")
        nic_templates = JSON.parse(read(templates_filename_nic(), String))
        haskey(nic_templates, nic_template) || error("if nic_template is a string, then the file $(templates_filename_nic()) must contain the key: $nic_template.  See AzManagers.save_template_nic.")
        nic_template = nic_templates[nic_template]
    end

    manager = azmanager!(session, user, nretry, verbose, false, show_quota)

    vm_template["value"]["properties"]["osProfile"]["computerName"] = vmname

    subnetid = vm_template["value"]["properties"]["networkProfile"]["networkInterfaces"][1]["id"]

    @debug "getting image info"
    sigimagename, sigimageversion, imagename = scaleset_image(manager, sigimagename, sigimageversion, imagename)
    scaleset_image!(manager, vm_template["value"], sigimagename, sigimageversion, imagename)

    vm_template["value"]["properties"]["storageProfile"]["osDisk"]["diskSizeGB"] = max(osdisksize, image_osdisksize(manager, vm_template["value"], sigimagename, sigimageversion, imagename))

    @debug "software sanity check"
    software_sanity_check(manager, imagename == "" ? sigimagename : imagename, customenv)

    @debug "making nic"
    r = @retry nretry azrequest(
        "PUT",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2022-09-01",
        ["Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(session))"],
        String(json(nic_template)))

    nic_id = JSON.parse(String(r.body))["id"]

    @debug "unique names for the attached disks"
    for attached_disk in get(vm_template["value"]["properties"]["storageProfile"], "dataDisks", [])
        attached_disk["name"] = vmname*"-"*attached_disk["name"]*"-"*randstring('a':'z', 6)
    end

    vm_template["value"]["properties"]["networkProfile"]["networkInterfaces"][1]["id"] = nic_id
    key = Dict("path" => "/home/$user/.ssh/authorized_keys", "keyData" => read(ssh_key, String))
    push!(vm_template["value"]["properties"]["osProfile"]["linuxConfiguration"]["ssh"]["publicKeys"], key)

    disk = vm_template["tempdisk"]

    local cmd
    if detachedservice
        cmd = buildstartupscript_detached(manager, nthreads_filter(julia_num_threads), omp_num_threads, env, user,
            disk, customenv, subscriptionid, resourcegroup, vmname)
    else
        cmd,_ = buildstartupscript(manager, user, disk, customenv)
    end
    
    _cmd = base64encode(cmd)

    if length(_cmd) > 64_000
        error("custom data is too large.")
    end

    vm_template["value"]["properties"]["osProfile"]["customData"] = _cmd

    # vm quota check
    @debug "quota check"
    while true
        navailable_cores, navailable_cores_spot = quotacheck(manager, subscriptionid, vm_template["value"], 1, nretry, verbose)
        navailable_cores >= 0 && break
        @warn "Insufficient quota for VM.  VM will start when usage allows; sleeping for 60 seconds, and trying again."
        try
            sleep(60)
        catch e
            isa(e, InterruptException) || rethrow(e)
            @warn "Recieved interupt, canceling AzManagers operation."
            return
        end
    end

    @debug "making vm"
    r = @retry nretry azrequest(
        "PUT",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?api-version=2019-07-01",
        ["Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(session))"],
        String(json(vm_template["value"])))

    spincount = 1
    starttime = tic = time()
    elapsed_time = 0.0
    while true
        if time() - tic > 10
            _r = @retry nretry azrequest(
                "GET",
                verbose,
                "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?api-version=2022-11-01",
                ["Authorization"=>"Bearer $(token(session))"])
            r = JSON.parse(String(_r.body))

            r["properties"]["provisioningState"] == "Succeeded" && break

            if r["properties"]["provisioningState"] == "Failed"
                error("Failed to create VM.  Check the Azure portal to diagnose the problem.")
            end
            tic = time()
        end

        elapsed_time = time() - starttime
        if elapsed_time > timeout
            error("reached timeout ($timeout seconds) while creating head VM.")
        end

        write(stdout, spin(spincount, elapsed_time)*", waiting for VM, $vmname, to start.\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1

        sleep(0.5)
    end
    write(stdout, spin(5, elapsed_time)*", waiting for VM, $vmname, to start.\r")
    write(stdout, "\n")

    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2022-09-01",
        ["Authorization"=>"Bearer $(token(session))"])

    r = JSON.parse(String(_r.body))

    vm = Dict("name"=>vmname, "ip"=>string(r["properties"]["ipConfigurations"][1]["properties"]["privateIPAddress"]),
        "subscriptionid"=>string(subscriptionid), "resourcegroup"=>string(resourcegroup), "port"=>string(detached_port()),
        "julia_num_threads"=>string(julia_num_threads), "omp_num_threads"=>string(omp_num_threads))

    if detachedservice
        detached_service_wait(vm, customenv)
    elseif customenv
        @info "There will be a delay before the custom environment is instantiated, but this work is happening asynchronously"
    end

    vm
end

function addproc(vm_template::AbstractString, nic_template=nothing; kwargs...)
    isfile(templates_filename_vm()) || error("if vm_template is a string, then the file $(templates_filename_vm()) must exist.  See AzManagers.save_template_vm.")
    vm_templates = JSON.parse(read(templates_filename_vm(), String))
    vm_template = vm_templates[vm_template]

    addproc(vm_template, nic_template; kwargs...)
end

"""
    rmproc(vm[; session=AzSession(;lazy=true), verbose=0, nretry=10])

Delete the VM that was created using the `addproc` method.

# Parameters
* `session=AzSession(;lazy=true)` Azure session for OAuth2 authentication
* `verbose=0` verbosity flag passed to HTTP.jl methods
* `nretry=10` max number of retries for retryable REST calls
* `show_quota=false` after various operation, show the "x-ms-rate-remaining-resource" response header.  Useful for debugging/understanding Azure quota's.
"""
function rmproc(vm;
        session = AzSession(;lazy=true),
        nretry = 10,
        verbose = 0,
        show_quota = false)
    timeout = Distributed.worker_timeout()

    resourcegroup = vm["resourcegroup"]
    subscriptionid = vm["subscriptionid"]
    vmname = vm["name"]

    manager = azmanager!(session, nretry, verbose, false, show_quota)

    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?\$expand=instanceView&api-version=2022-11-01",
        ["Authorization"=>"Bearer $(token(session))"])

    r = JSON.parse(String(_r.body))

    osdisk = r["properties"]["storageProfile"]["osDisk"]["name"]
    datadisks = [datadisk["name"] for datadisk in r["properties"]["storageProfile"]["dataDisks"]]

    r = @retry nretry azrequest(
        "DELETE",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?api-version=2022-11-01",
        ["Authorization"=>"Bearer $(token(session))"])

    if r.status >= 300
        @warn "Problem removing VM, $vmname, status=$(r.status)"
    end

    @debug "Waiting for VM deletion"
    starttime = time()
    elapsed_time = 0.0
    tic = time() - 20
    spincount = 1
    while true
        if time() - tic > 10
            _r = @retry nretry azrequest(
                "GET",
                verbose,
                "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines?api-version=2022-11-01",
                ["Authorization" => "Bearer $(token(session))"])

            r = JSON.parse(String(_r.body))
            vms,_r = getnextlinks!(manager, _r, get(r, "value", []), get(r, "nextLink", ""), nretry, verbose)

            haveit = false
            for vm in vms
                if vm["name"] == vmname
                    haveit = true
                    break
                end
            end
            haveit || break
            tic = time()
        end

        elapsed_time = time() - starttime
        elapsed_time > timeout && @warn "Unable to delete virtual machine in $timeout seconds"

        write(stdout, spin(spincount, elapsed_time)*", waiting for VM, $vmname, to delete.\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1

        sleep(0.5)
    end
    write(stdout, spin(5, elapsed_time)*", waiting for VM, $vmname, to delete.\r")
    write(stdout, "\n")

    @retry nretry azrequest(
        "DELETE",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/disks/$osdisk?api-version=2021-12-01",
        ["Authorization" => "Bearer $(token(session))"])

    for datadisk in datadisks
        @retry nretry azrequest(
            "DELETE",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/disks/$datadisk?api-version=2021-12-01",
            ["Authorization" => "Bearer $(token(session))"])
    end

    nicname = vmname*"-nic"

    @retry nretry azrequest(
        "DELETE",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2022-09-01",
        ["Authorization"=>"Bearer $(token(session))"])
    nothing
end

vm(;kwargs...) = nothing

macro detach(expr::Expr)
    Expr(:call, :detached_run, string(expr))
end

macro detach(parms::Expr, expr::Expr)
    Expr(:call, :detached_run, esc(parms.args[2]), string(expr))
end

"""
    @detachat myvm begin ... end

Run code on an Azure VM.

# Example
```
using AzManagers
myvm = addproc("myvm")
job = @detachat myvm begin
    @info "I'm running detached"
end
read(job)
wait(job)
rmproc(myvm)
```
"""
macro detachat(ip::String, expr::Expr)
    Expr(:call, :detached_run, string(expr), ip)
end

macro detachat(ip, expr::Expr)
    Expr(:call, :detached_run, string(expr), esc(ip))
end

struct DetachedJob
    vm::Dict{String,String}
    id::String
    pid::String
    logurl::String
end
DetachedJob(ip, id; port=detached_port()) = DetachedJob(Dict("ip"=>string(ip), "port"=>string(port)), string(id), "-1", "")
DetachedJob(ip, id, pid; port=detached_port()) = DetachedJob(Dict("ip"=>string(ip), "port"=>string(port)), string(id), string(pid), "")

function loguri(job::DetachedJob)
    job.logurl
end

function detached_service_wait(vm, custom_environment)
    timeout = Distributed.worker_timeout()
    starttime = time()
    elapsed_time = 0.0
    tic = starttime - 20
    spincount = 1
    waitfor = custom_environment ? "Julia package instantiation and COFII detached service" : "COFII detached service"
    while true
        if time() - tic > 5
            try
                r = HTTP.request("GET", "http://$(vm["ip"]):$(vm["port"])/cofii/detached/ping")
                break
            catch
                tic = time()
            end
        end

        elapsed_time = time() - starttime

        if elapsed_time > timeout
            error("reached timeout ($timeout seconds) while waiting for $waitfor to start.")
        end
        
        write(stdout, spin(spincount, elapsed_time)*", waiting for $waitfor on VM, $(vm["name"]):$(vm["port"]), to start.\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1
        
        sleep(0.5)
    end
    write(stdout, spin(5, elapsed_time)*", waiting for $waitfor on VM, $(vm["name"]):$(vm["port"]), to start.\r")
    write(stdout, "\n")
end

const VARIABLE_BUNDLE = Dict()
function variablebundle!(bundle::Dict)
    for (key,value) in bundle
        AzManagers.VARIABLE_BUNDLE[Symbol(key)] = value
    end
    AzManagers.VARIABLE_BUNDLE
end

"""
    variablebundle!(;kwargs...)

Define variables that will be passed to a detached job.

# Example
```julia
using AzManagers
variablebundle(;x=1)
myvm = addproc("myvm")
myjob = @detachat myvm begin
    write(stdout, "my variable is \$(variablebundle(:x))\n")
end
wait(myjob)
read(myjob)
```
"""
function variablebundle!(;kwargs...)
    for kwarg in kwargs
        AzManagers.VARIABLE_BUNDLE[kwarg[1]] = kwarg[2]
    end
    AzManagers.VARIABLE_BUNDLE
end
variablebundle() = AzManagers.VARIABLE_BUNDLE

"""
    variablebundle(:key)

Retrieve a variable from a variable bundle.  See `variablebundle!`
for more information.
"""
variablebundle(key) = AzManagers.VARIABLE_BUNDLE[Symbol(key)]

function detached_run(code, ip::String="", port=detached_port();
        persist=true,
        vm_template = "",
        customenv = false,
        nic_template = nothing,
        basename = "cbox",
        user = "",
        subscriptionid = "",
        resourcegroup = "",
        session = AzSession(;lazy=true),
        sigimagename = "",
        sigimageversion = "",
        imagename = "",
        nretry = 10,
        verbose = 0,
        detachedservice = true)
    local vm
    if ip == ""
        vm_template == "" && error("must specify a vm template.")
        vm = addproc(vm_template, nic_template;
            basename = basename,
            user = user,
            subscriptionid = subscriptionid,
            resourcegroup = resourcegroup,
            session = session,
            customenv = customenv,
            sigimagename = sigimagename,
            sigimageversion = sigimageversion,
            imagename = imagename,
            nretry = nretry,
            verbose = verbose)
    else
        r = HTTP.request(
            "GET",
            "http://$ip:$port/cofii/detached/vm")
        vm = JSON.parse(String(r.body))
    end

    io = IOBuffer()
    serialize(io, variablebundle())
    body = Dict(
        "persist" => persist,
        "variablebundle" => base64encode(take!(io)),
        "code" => """
        $code
        """)

    _r = HTTP.request(
        "POST",
        "http://$(vm["ip"]):$(vm["port"])/cofii/detached/run",
        ["Content-Type"=>"application/json"],
        json(body))
    r = JSON.parse(String(_r.body))

    @info "detached job id is $(r["id"]) at $(vm["name"]),$(vm["ip"]):$(vm["port"])"
    DetachedJob(vm, string(r["id"]), string(r["pid"]), "")
end

detached_run(code, vm::Dict; kwargs...) = detached_run(code, vm["ip"], vm["port"]; kwargs...)

"""
    read(job[;stdio=stdout])

returns the stdout from a detached job.
"""
function Base.read(job::DetachedJob; stdio=stdout)
    r = HTTP.request(
        "GET",
        "http://$(job.vm["ip"]):$(job.vm["port"])/cofii/detached/job/$(job.id)/$(stdio==stdout ? "stdout" : "stderr")", readtimeout=60)
    String(r.body)
end

"""
    status(job)

returns the status of a detached job.
"""
function status(job::DetachedJob)
    local _r
    try
        # the timeout is needed in the event that the vm is deleted
        _r = HTTP.request(
            "GET",
            "http://$(job.vm["ip"]):$(job.vm["port"])/cofii/detached/job/$(job.id)/status", readtimeout=60)
        r = JSON.parse(String(_r.body))
        _r = r["status"]
    catch e
        _r = "failed to fetch status from server"
        showerror(stderr, e)
    end
    _r
end

"""
    wait(job[;stdio=stdout])

blocks until the detached job, `job`, is complete.
"""
function Base.wait(job::DetachedJob)
    HTTP.request(
        "POST",
        "http://$(job.vm["ip"]):$(job.vm["port"])/cofii/detached/job/$(job.id)/wait")
end

"""
    kill(job)

kill the linux process associated with `job`
"""
function Base.kill(job::DetachedJob)
    HTTP.request(
        "POST",
        "http://$(job.vm["ip"]):$(job.vm["port"])/cofii/detached/job/$(job.id)/kill")
end

export AzManager, DetachedJob, addproc, nworkers_provisioned, preempted, rmproc, scalesets, status, variablebundle, variablebundle!, vm, @detach, @detachat

end
