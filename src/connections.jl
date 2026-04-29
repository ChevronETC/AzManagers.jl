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
    tsk = @async addprocs(manager; sockets)

    # Arm a watchdog that interrupts the task on timeout
    watchdog = Timer(timeout) do _
        if !istaskdone(tsk)
            @warn "AzManagers, interrupting addprocs due to timeout"
            @async Base.throwto(tsk, InterruptException())
        end
    end

    local pids
    try
        pids = fetch(tsk)
    catch e
        @warn "AzManagers, failed to process pending connections"
        logerror(e, Logging.Debug)
        pids = []
    finally
        close(watchdog)
    end
    pids
end

function process_pending_connections()
    manager = azmanager()
    max_sockets = manager.pending_up.sz_max
    flush_delay = parse(Float64, get(ENV, "JULIA_AZMANAGERS_BATCH_FLUSH_DELAY",
        get(ENV, "JULIA_AZMANAGERS_PENDING_CADENCE", "5.0")))

    while true
        # Block until at least one socket arrives
        local first_socket
        try
            first_socket = take!(manager.pending_up)
        catch e
            @error "AzManagers, error retrieving pending connection"
            logerror(e, Logging.Debug)
            continue
        end
        sockets = TCPSocket[first_socket]

        try
            # Drain additional sockets until deadline or batch full
            drain_with_deadline!(sockets, manager.pending_up, max_sockets, flush_delay)

            @debug "flushing batch" length(sockets)
            pids = addprocs_with_timeout(manager; sockets)
            @debug "batch done" pids

            if isdefined(manager, :workers_changed)
                lock(manager.workers_changed)
                try
                    notify(manager.workers_changed)
                finally
                    unlock(manager.workers_changed)
                end
            end

            start_preempt_monitors(manager, pids)
            @debug "loop iteration complete, waiting for next socket"
        catch e
            @error "AzManagers, error processing connection batch"
            logerror(e, Logging.Debug)
        end
    end
end

function drain_with_deadline!(sockets, pending_up, max_sockets, deadline_s)
    deadline = Timer(deadline_s)
    try
        while length(sockets) < max_sockets
            # Check if deadline has fired
            if !isopen(deadline)
                break
            end
            # Non-blocking check for available sockets
            if isready(pending_up)
                push!(sockets, take!(pending_up))
            else
                # Brief yield to avoid busy-wait, but much shorter than old 0.1s poll
                sleep(0.01)
            end
        end
    catch e
        @debug "drain_with_deadline! interrupted" e
    finally
        close(deadline)
    end
end

function start_preempt_monitors(manager, pids)
    @debug "starting preempt loops" pids
    for pid in pids
        @async monitor_worker_preempt(manager, pid)
    end
    @debug "done starting preempt loops"
end

function monitor_worker_preempt(manager, pid)
    haskey(Distributed.map_pid_wrkr, pid) || return
    wrkr = Distributed.map_pid_wrkr[pid]

    isdefined(wrkr, :config) || return
    isdefined(wrkr.config, :userdata) || return
    lowercase(get(wrkr.config.userdata, "priority", "")) == "spot" || return

    try
        manager.preempt_channel_futures[pid] = remotecall(Channel{Bool}, pid, 1)
        remotecall_fetch(machine_preempt_loop, pid, manager.preempt_channel_futures[pid])
    catch e
        handle_preempt_exception(manager, pid, wrkr, e)
    end
end

function handle_preempt_exception(manager, pid, wrkr, e)
    if isa(e, RemoteException) && isa(e.captured.ex, TaskFailedException) && isa(e.captured.ex.task.result.ex, SpotPreemptException)
        # Graceful preemption: IMDS scheduled event was detected before the VM was killed.
        # Honor the NotBefore window before deregistering.
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
    else
        # Worker connection dropped — could be an ungraceful eviction, crash, or
        # normal cleanup (rmprocs killed the process while preempt monitor was running).
        # Only warn if the worker is still registered (i.e. not already cleaned up by rmprocs).
        if haskey(Distributed.map_pid_wrkr, pid)
            @warn "worker $pid lost unexpectedly (not a graceful preemption), deregistering" exception=(e, catch_backtrace())
        end
    end

    # Always deregister the worker — whether graceful preemption or unexpected death.
    try
        lock(Distributed.worker_lock)
        if haskey(Distributed.map_pid_wrkr, pid)
            Distributed.set_worker_state(Distributed.map_pid_wrkr[pid], Distributed.W_TERMINATED)
            Distributed.deregister_worker(pid)
        end
    catch
    finally
        unlock(Distributed.worker_lock)
    end
end

function Distributed.setup_launched_worker(manager::AzManager, wconfig, launched_q)
    # Distributed.create_worker also uses Distributed.worker_timeout, so we add a grace period
    # to allow for the Distributed.create_worker to hit its timeout.
    timeout = Distributed.worker_timeout() + 10
    local pid
    try
        tsk_create_worker = @async Distributed.create_worker(manager, wconfig)

        watchdog = Timer(timeout) do _
            if !istaskdone(tsk_create_worker)
                @async Base.throwto(tsk_create_worker, InterruptException())
            end
        end

        try
            pid = fetch(tsk_create_worker)
        finally
            close(watchdog)
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


function spinner(n_target_workers)
    manager = azmanager()
    starttime = time()

    while (nprocs() == 1 ? 0 : nworkers()) != n_target_workers
        # Wait for workers_changed notification with a timeout for UI updates
        tsk = @async begin
            lock(manager.workers_changed)
            try
                wait(manager.workers_changed)
            finally
                unlock(manager.workers_changed)
            end
        end
        timedwait(() -> istaskdone(tsk), 2.0)

        elapsed = time() - starttime
        n = nprocs() == 1 ? 0 : nworkers()
        @printf(stdout, "\r  %d/%d workers up (%.1fs)     ", n, n_target_workers, elapsed)
        flush(stdout)
    end

    elapsed = time() - starttime
    n = nprocs() == 1 ? 0 : nworkers()
    @printf(stdout, "\r  %d/%d workers running (%.1fs)\n", n, n_target_workers, elapsed)
    flush(stdout)
    nothing
end

function nthreads_filter(nthreads)
    _nthreads = split(string(nthreads), ',')
    nthreads_default = length(_nthreads) > 0 ? parse(Int, _nthreads[1]) : 1
    nthreads_interactive = length(_nthreads) > 1 ? parse(Int, _nthreads[2]) : 0

    nthreads_interactive > 0 ? string("$nthreads_default,$nthreads_interactive") : string(nthreads_default)
end

"""
    addprocs(mgr::AzManager, template, ninstances[; kwargs...])

Add Azure scale set instances where `mgr` is an `AzManager` instance (e.g. `AzManager()`),
and template is either a dictionary produced via the `AzManagers.build_sstemplate`
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
* `exename="\$(Sys.BINDIR)/julia"` name of the julia executable.
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
* `use_lvm=false` For SKUs that have 1 or more nvme disks, combines all disks as a single mount point /scratch vs /scratch, /scratch1, /scratch2, etc..

# Notes
[1] If `addprocs` is called from an Azure VM, then the default `imagename`,`imageversion` are the
image/version the VM was built with; otherwise, it is the latest version of the image specified in the scale-set template.
[2] Interactive threads are supported beginning in version 1.9 of Julia.  For earlier versions, the default for `julia_num_threads` is `Threads.nthreads()`.
[3] `waitfor=false` reflects the fact that the cluster manager is dynamic.  After the call to `addprocs` returns, use `workers()`
to monitor the size of the cluster.
[4] This is inteneded for use with Devito.  In particular, it allows Devito to gain performance by using
MPI to do domain decomposition using MPI within a single VM.  If `mpi_ranks_per_worker=0`, then MPI is not
used on the Julia workers.  This feature makes use of package extensions, meaning that you need to ensure
that `using MPI` is somewhere in your calling script.
[5] This may result in a re-boot of the VMs
"""
function Distributed.addprocs(::AzManager, template::Dict, n::Int;
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
        exename = "$(Sys.BINDIR)/julia",
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
        hyperthreading = nothing,
        use_lvm = false)
    n_current_workers = nprocs() == 1 ? 0 : nworkers()

    (subscriptionid == "" || resourcegroup == "" || user == "") && load_manifest()
    subscriptionid == "" && (subscriptionid = get(template, "subscriptionid", _manifest["subscriptionid"]))
    resourcegroup == "" && (resourcegroup = get(template, "resourcegroup", _manifest["resourcegroup"]))
    user == "" && (user = _manifest["ssh_user"])

    manager = azmanager!(session, user, nretry, verbose, save_cloud_init_failures, show_quota)
    sigimagename,sigimageversion,imagename = scaleset_image(manager, sigimagename, sigimageversion, imagename, template["value"])
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
        hyperthreading, julia_num_threads, omp_num_threads, exename, exeflags, env, spot, maxprice, spot_base_regular_priority_count, spot_regular_percentage_above_base,
        verbose, customenv, overprovision, use_lvm)

    if waitfor
        @info "Initiating cluster..."
        spinner_tsk = @async spinner(n_current_workers + n)
        wait(spinner_tsk)
    end

    nothing
end

function Distributed.addprocs(mgr::AzManager, template::AbstractString, n::Int; kwargs...)
    isfile(templates_filename_scaleset()) || error("scale-set template file does not exist.  See `AzManagers.save_template_scaleset`")

    templates_scaleset = JSON.parse(read(templates_filename_scaleset(), String); dicttype=Dict)
    haskey(templates_scaleset, template) || error("scale-set template file does not contain a template with name: $template. See `AzManagers.save_template_scaleset`")

    addprocs(mgr, templates_scaleset[template], n; kwargs...)
end

function Distributed.launch(manager::AzManager, params::Dict, launched::Array, c::Condition)
    sockets = params[:sockets]

    @sync for socket in sockets
        @async try
            Distributed.launch_on_machine(manager, launched, c, socket)
        catch e
            @error "failed to launch on machine for socket=$socket"
            logerror(e, Logging.Debug)
        end
    end
    notify(c)
end

function Distributed.launch_on_machine(manager::AzManager, launched, c, socket)
    local _cookie
    try
        _cookie = read(socket, Distributed.HDR_COOKIE_LEN)
    catch e
        @error "unable to read cookie from socket"
        logerror(e, Logging.Debug)
        return
    end

    cookie = String(_cookie)
    cookie == Distributed.cluster_cookie() || error("Invalid cookie sent by remote worker.")

    local _connection_string
    try
        _connection_string = readline(socket)
    catch e
        @error "unable to read connection string from socket"
        throw(e)
    end

    connection_string = String(base64decode(_connection_string))

    local vm
    try
        vm = JSON.parse(connection_string)
    catch e
        @error "unable to parse connection string, string=$connection_string, cookie=$cookie"
        throw(e)
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

function add_instance_to_preempted_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.preempted, scaleset)
        @debug "pushing worker with id=$instanceid onto preempted"
        push!(manager.preempted[scaleset], string(instanceid))
    else
        @debug "creating preempted vector for id=$instanceid"
        manager.preempted[scaleset] = Set{String}([string(instanceid)])
    end
end

function ispreempted(manager::AzManager, config::WorkerConfig)
    u = config.userdata
    scaleset = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
    string(u["instanceid"])  ∈ get(manager.preempted, scaleset, Set{String}())
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

    if ispreempted(manager, config)
        @debug "kill on id=$id because it was preempted"
        return nothing
    end

    try
        remote_do(exit, id, 42)
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


function azure_worker_mpi end

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
