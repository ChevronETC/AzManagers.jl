module AzManagers

using AzSessions, Base64, CodecZlib, Dates, Distributed, HTTP, Hwloc, JSON, JWTs, LibCURL, LibGit2, Logging, Pkg, Printf, Random, Serialization, Sockets, ThreadPinning, TOML

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

mutable struct AzManagersManifest
    resourcegroup::String
    ssh_user::String
    ssh_private_key_file::String
    ssh_public_key_file::String
    subscriptionid::String
end

AzManagersManifest() = AzManagersManifest("", "", "", "", "")

const MANIFEST_FIELDS = ("resourcegroup", "ssh_user", "ssh_private_key_file",
                        "ssh_public_key_file", "subscriptionid")

function Base.getindex(manifest::AzManagersManifest, key::AbstractString)
    key in MANIFEST_FIELDS ||
        throw(KeyError(key))
    getfield(manifest, Symbol(key))
end

function Base.setindex!(manifest::AzManagersManifest, value, key::AbstractString)
    key in MANIFEST_FIELDS ||
        throw(KeyError(key))
    setfield!(manifest, Symbol(key), String(value))
end

Base.keys(::AzManagersManifest) = MANIFEST_FIELDS
Base.haskey(manifest::AzManagersManifest, key::AbstractString) = key in MANIFEST_FIELDS

const _manifest = AzManagersManifest()

manifestpath() = joinpath(homedir(), ".azmanagers")
manifestfile() = joinpath(manifestpath(), "manifest.json")

include("placement.jl")
include("cluster_manager.jl")
include("azure_api.jl")
include("cloud_init.jl")
include("detached.jl")

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
        write(manifestfile(), JSON.json(manifest, 1))
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
    _manifest
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
        logerror(e, Logging.Debug)
    end
    while nprocs() == 1 || nworkers() != n_target_workers
        try
            elapsed_time = time() - starttime
            # Refresh worker count and emit a real newline every 10 s. The
            # \n acts as a checkpoint so CI log collectors (which split on
            # \n, not \r) flush the spinner output instead of buffering it
            # for the lifetime of the addprocs call.
            checkpoint = time() - tic > 10
            if checkpoint
                _nworkers = nprocs() == 1 ? 0 : nworkers()
                tic = time()
            end
            sep = checkpoint ? "\n" : "\r"
            write(stdout, spin(spincount, elapsed_time)*", $_nworkers/$n_target_workers up. $ws$sep")
            flush(stdout)
            spincount = spincount == 4 ? 1 : spincount + 1
            yield()
            sleep(.25)
        catch e
            @warn "error during startup:"
            logerror(e, Logging.Debug)
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

    # On Julia 1.9+ keep the explicit "N,I" form even when I==0. Collapsing
    # to bare "N" is only safe for pre-1.9 (which has no interactive pool):
    # Julia 1.11+ silently auto-adds an interactive thread when -t is a
    # bare N, which would override an explicit request for zero interactive
    # threads.
    if VERSION >= v"1.9"
        return "$nthreads_default,$nthreads_interactive"
    end
    nthreads_interactive > 0 ? "$nthreads_default,$nthreads_interactive" : string(nthreads_default)
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
* `ppi=1` Procs-per-instance: the number of Julia workers to start per Azure scale set instance.
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
function Distributed.addprocs(::AzManager, template::AbstractDict, n::Int;
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
    validate_ppi_options(ppi, mpi_ranks_per_worker)

    (subscriptionid == "" || resourcegroup == "" || user == "") && load_manifest()
    subscriptionid == "" && (subscriptionid = get(template, "subscriptionid", _manifest["subscriptionid"]))
    resourcegroup == "" && (resourcegroup = get(template, "resourcegroup", _manifest["resourcegroup"]))
    user == "" && (user = _manifest["ssh_user"])

    manager = azmanager!(session, user, nretry, verbose, save_cloud_init_failures, show_quota)

    # If the caller didn't specify an image, derive it from the scale-set
    # template's imageReference rather than asking IMDS what the
    # coordinator booted from. The template's reference is the source
    # of truth for what the SCALE-SET will run; IMDS is a fallback
    # (kept below in scaleset_image) for callers that don't pass a
    # template, e.g. running outside an Azure VM. The IMDS path has
    # silent failure modes - the @async + fetch + retry stack can land
    # in a "task returned nothing because @retry exhausted retries on a
    # transient IMDS hiccup" state and return all-empty strings; the
    # caller then errors in image_osdisksize with no useful diagnostic.
    # Extracting from the template here makes the common case
    # deterministic.
    if imagename == "" && sigimagename == ""
        sigimagename, sigimageversion, imagename = _resolve_image_from_template(
            manager, template["value"], subscriptionid, resourcegroup)
    end

    sigimagename,sigimageversion,imagename = scaleset_image(manager, sigimagename, sigimageversion, imagename)
    scaleset_image!(manager, template["value"], sigimagename, sigimageversion, imagename)
    software_sanity_check(manager, imagename == "" ? sigimagename : imagename, customenv)

    _scalesets = scalesets(manager)
    scaleset = ScaleSet(subscriptionid, resourcegroup, group)

    osdisksize = max(osdisksize, image_osdisksize(manager, template["value"], sigimagename, sigimageversion, imagename))

    julia_num_threads = nthreads_filter(julia_num_threads)

    @info "Provisioning $n virtual machines in scale-set $group..."
    scaleset_create_or_update(manager, user, scaleset.subscriptionid, scaleset.resourcegroup, scaleset.scalesetname, sigimagename,
        sigimageversion, imagename, osdisksize, nretry, template, n, ppi, mpi_ranks_per_worker, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig,
        hyperthreading, julia_num_threads, omp_num_threads, exename, exeflags, env, spot, maxprice, spot_base_regular_priority_count, spot_regular_percentage_above_base,
        verbose, customenv, overprovision, use_lvm)

    if waitfor
        @info "Initiating cluster..."
        spinner_tsk = @async spinner(n_current_workers + n * ppi)
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

"""
    addprocs(template::AbstractString, n::Int; kwargs...)

Convenience overload for the public docs' `addprocs("myscaleset", 5)`
form: builds a fresh `AzManager()` and looks the template up by name in
`~/.azmanagers/templates_scaleset.json`. Equivalent to
`addprocs(AzManager(), template, n; kwargs...)`.
"""
function Distributed.addprocs(template::AbstractString, n::Int; kwargs...)
    addprocs(AzManager(), template, n; kwargs...)
end

"""
    addprocs(template::AbstractDict, n::Int; kwargs...)

Convenience overload for an already-loaded template dict (e.g. the value
read from `~/.azmanagers/templates_scaleset.json` via `JSON.parse`).
Builds a fresh `AzManager()` and forwards. Accepts any `AbstractDict`
(plain `Dict`, `JSON.Object`, etc.) so the same call site works regardless
of which JSON parser was used.
"""
function Distributed.addprocs(template::AbstractDict, n::Int; kwargs...)
    addprocs(AzManager(), template, n; kwargs...)
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
        # Bypass atexit hooks. `exit(42)` first runs `jl_atexit_hook`, which
        # in turn waits on every registered atexit callback. On customenv
        # workers an in-flight `Pkg.precompile()` task leaves a hook that
        # never returns, so `exit(42)` deadlocks and the master's `rmprocs`
        # blocks forever on the worker's TCP connection. Calling jl_exit
        # directly terminates the process immediately. Safe here because
        # the worker VM gets deleted by scaleset_pruning anyway; there's
        # nothing to gracefully clean up on the worker side.
        remote_do(id) do
            ccall(:jl_exit, Cvoid, (Int32,), Int32(42))
        end
    catch
    end
    @debug "kill, done remote_do"

    u = config.userdata
    get(u, "localid", 1) > 1 && (return nothing) # an "additional" worker on an instance will have localid>1

    scaleset = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])

    lock(manager.lock)
    try
        add_instance_to_pending_down_list(manager, scaleset, u["instanceid"])
        add_instance_to_deleted_list(manager, scaleset, u["instanceid"])
    catch e
        throw(e)
    finally
        unlock(manager.lock)
    end

    @debug "...kill, pushed."
    nothing
end

"""
    nworkers_provisioned([service=false])

Count of the number of scale-set machines that are provisioned
regardless of their status within the Julia cluster.  If `service=true`,
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
    worker_placement(pid)

Return the CPU/NUMA placement metadata recorded for worker `pid`. Keys are the
ones produced by `ppi`-based placement: `localid`, `ppi`, `physical_cores`,
`julia_threads`, `julia_interactive_threads`, `omp_threads`, `cpu_set`
(e.g. `"0-43"`), `numa_node`, `socket`, and `pinning_backend`. Returns an
empty `Dict` when no placement was recorded (e.g. the worker was launched
outside `addprocs(...; ppi=...)`).
"""
function worker_placement(pid::Int)
    wrkr = Distributed.map_pid_wrkr[pid]
    if isdefined(wrkr, :config) && isdefined(wrkr.config, :userdata)
        return placement_userdata(wrkr.config.userdata)
    end
    Dict()
end

"""
    worker_placements()

Return a `Dict{Int,Dict}` of placement metadata for every active worker. See
[`worker_placement`](@ref) for the per-worker keys.
"""
function worker_placements()
    Dict(pid => worker_placement(pid) for pid in workers())
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
    userdata = Dict(
        "subscriptionid" => lowercase(r["compute"]["subscriptionId"]),
        "resourcegroup" => lowercase(r["compute"]["resourceGroupName"]),
        "scalesetname" => lowercase(r["compute"]["vmScaleSetName"]),
        "instanceid" => split(r["compute"]["resourceId"], '/')[end],
        "priority" => get(r["compute"], "priority", ""),
        "localid" => 1,
        "name" => r["compute"]["name"],
        "mpi" => mpi_size > 0,
        "mpi_size" => mpi_size,
        "physical_hostname" => azure_physical_name())

    # Note: a previous version of this function ran detect_machine_topology
    # / plan_worker_placements / pin_julia_threads here to attach placement
    # metadata to the worker's userdata before the handshake completes. That
    # turned out to be the source of "0/N up forever" hangs: any of those
    # calls can block (Hwloc on a busy /sys, lscpu / numactl waiting on
    # stuck child processes, ThreadPinning.pinthreads against the GC thread
    # in newer Julia), and a `try`/`catch` cannot rescue a hang. The
    # handshake must not depend on best-effort metadata collection.
    # The multi-worker path (ppi > 1) already attaches placement metadata
    # AFTER the worker is registered, in `launch_n_additional_processes`,
    # so that path is unaffected.

    vm = Dict(
        "exeflags" => exeflags,
        "bind_addr" => string(getipaddr(IPv4)),
        "ppi" => ppi,
        "userdata" => userdata)

    _vm = base64encode(JSON.json(vm))

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
        of the cookie.  We do not know the cuase of this, but here we throw
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
            logerror(e, Logging.Debug)
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

function azure_worker_mpi end

# We create our own method here so that we can add `localid` and `cnt` to `wconfig`.  This can
# be useful when we need to understand the layout of processes that are sharing the same hardware.
function Distributed.launch_n_additional_processes(manager::AzManager, frompid, fromconfig, cnt, launched_q)
    @sync begin
        exename = Distributed.notnothing(fromconfig.exename)
        exeflags = something(fromconfig.exeflags, ``)

        # Fold the active named environment's project path into exeflags so
        # both the placement launcher (worker_launch_command) and the
        # no-placement fallback start additional workers with the right
        # --project. Preserves customenv for ppi>1.
        projectinfo = Pkg.project()
        envname = splitpath(projectinfo.path)[end-1]
        if !isempty(envname)
            envdir = joinpath(Pkg.envdir(), envname)
            exeflags = `$exeflags --project=$envdir`
        end

        placement_info = try
            remotecall_fetch(frompid, cnt + 1) do ppi
                topology = AzManagers.detect_machine_topology()
                placements = AzManagers.plan_worker_placements(topology, ppi)
                use_numactl = Sys.which("numactl") !== nothing
                topology, placements, use_numactl
            end
        catch e
            @warn "unable to infer local worker placement; using default launcher"
            logerror(e, Logging.Debug)
            nothing
        end

        if placement_info === nothing
            cmd = `$exename $exeflags --worker`
            new_addresses = remotecall_fetch(Distributed.launch_additional, frompid, cnt, cmd)
            add_launched_workers(manager, frompid, fromconfig, new_addresses, launched_q)
            return
        end

        topology, placements, use_numactl = placement_info
        merge!(
            fromconfig.userdata,
            worker_placement_metadata(topology, placements[1], cnt + 1))

        # Fork ALL additional workers in one call on the primary,
        # mirroring Distributed.launch_additional's structure (parallel
        # fork phase + serial bind_addr read phase) but with one cmd
        # per worker. Calling launch_additional(1, cmd) sequentially N
        # times is the wrong API shape - each call serializes a full
        # launch+read, and with N>1 (e.g. ppi=4 needing 3 additional
        # workers) the cluster gets stuck part-way through. The fork
        # loop must batch and start the children in parallel so they
        # can read their cookies + bind their ports concurrently.
        n_extra = length(placements) - 1
        @info "launch_n_additional_processes: forking $n_extra additional workers on primary frompid=$frompid"
        for (i, p) in enumerate(placements[2:end])
            @info "  placement $i: localid=$(p.localid) cpu_set=$(cpu_set_string(p.cpu_set)) numa_node=$(p.numa_node) socket=$(p.socket)"
        end
        cmds = [worker_launch_command(exename, exeflags, p; use_numactl) for p in placements[2:end]]
        t0 = time()
        new_addresses = remotecall_fetch(frompid, cmds) do cmd_list
            io_objs = []
            addresses = []
            # Phase 1: fork all children in parallel. Each child
            # (numactl-wrapped julia --worker) starts up and blocks
            # reading its cookie from stdin.
            for cmd in cmd_list
                io = open(Base.detach(cmd), "r+")
                Distributed.write_cookie(io)
                push!(io_objs, io.out)
            end
            # Phase 2: collect bind_addrs in order. Each read blocks
            # until that child has bound its port and printed the addr.
            for io in io_objs
                (host, port) = Distributed.read_worker_host_port(io)
                push!(addresses, (host, port))
                Distributed.additional_io_objs[port] = io
            end
            addresses
        end
        @info "  all $n_extra forks done" elapsed_s=round(time()-t0; digits=1) addresses=new_addresses

        # Register each new worker with the master. add_launched_workers
        # queues an @async create_worker per address; @sync at the top
        # of this function waits for all of them to complete.
        for (placement, addr) in zip(placements[2:end], new_addresses)
            add_launched_workers(
                manager,
                frompid,
                fromconfig,
                [addr],
                launched_q;
                topology,
                placement,
                ppi = cnt + 1)
        end
        @info "launch_n_additional_processes: all $n_extra create_worker @asyncs queued"

        # Pin the PRIMARY worker (placements[1]) to its planned cpu_set
        # AFTER all additional workers have been forked. The primary is
        # the julia process started by cloud-init at scale-set boot;
        # unlike additional workers (launched above via
        # `numactl --physcpubind=...` wrapper), it never went through
        # `worker_launch_command(...; use_numactl=true)`, so without this
        # step its kernel-level affinity remains the whole machine even
        # though the metadata merged above claims a specific cpu_set.
        #
        # Order matters: pinning BEFORE the for-loop above restricts the
        # primary's process affinity, which child forks INHERIT, and
        # unprivileged sched_setaffinity can only set affinity to a
        # subset of the current mask. numactl in the child trying to
        # set a disjoint cpu_set (e.g. 60-119 when the primary is on
        # 0-59) fails, the additional worker never starts, and the
        # cluster sits at 1/N forever.
        if use_numactl
            primary_cpu_set = cpu_set_string(placements[1].cpu_set)
            @info "self-pinning primary frompid=$frompid to cpu_set=$primary_cpu_set"
            t0 = time()
            try
                remotecall_fetch(frompid, primary_cpu_set) do cpus
                    try
                        run(`taskset -pc $cpus $(getpid())`)
                    catch e
                        @warn "primary worker self-pin via taskset failed" exception=e cpus
                    end
                end
                @info "primary self-pin done" elapsed_s=round(time()-t0; digits=1)
            catch e
                @warn "could not reach primary worker for self-pin" exception=e
            end
        end
    end
end

function add_launched_workers(
        manager::AzManager,
        frompid,
        fromconfig,
        new_addresses,
        launched_q;
        topology = nothing,
        placement = nothing,
        ppi = nothing)
    for (localid,address) in enumerate(new_addresses)
            (bind_addr, port) = address

            wconfig = Distributed.WorkerConfig()
            for x in [:host, :tunnel, :multiplex, :sshflags, :exeflags, :exename, :enable_threaded_blas]
                Base.setproperty!(wconfig, x, Base.getproperty(fromconfig, x))
            end
            wconfig.bind_addr = bind_addr
            wconfig.port = port
            wconfig.count = fromconfig.count
            # Additional workers (ppi > 1) share the underlying scale-set
            # instance with the primary worker, so inherit `instanceid`
            # and `priority` from `fromconfig`. Without them, `ispreempted`
            # KeyErrors on `u["instanceid"]` during `rmprocs` cleanup
            # (upstream had the same gap; ci.yml's ppi=1 path never
            # exercised it).
            wconfig.userdata = Dict(
                "localid" => placement === nothing ? localid + 1 : placement.localid,
                "physical_hostname" => get(fromconfig.userdata, "physical_hostname", ""),
                "name" => fromconfig.userdata["name"],
                "subscriptionid" => fromconfig.userdata["subscriptionid"],
                "resourcegroup" => fromconfig.userdata["resourcegroup"],
                "scalesetname" => fromconfig.userdata["scalesetname"],
                "instanceid" => get(fromconfig.userdata, "instanceid", ""),
                "priority" => get(fromconfig.userdata, "priority", ""))

            if placement !== nothing && topology !== nothing && ppi !== nothing
                merge!(
                    wconfig.userdata,
                    worker_placement_metadata(topology, placement, ppi))
            end

            let wconfig=wconfig, port=port, addr=bind_addr,
                lid=wconfig.userdata["localid"]
                @async begin
                    @info "    create_worker: connecting" localid=lid bind_addr=addr port=port
                    t_cw = time()
                    pid = Distributed.create_worker(manager, wconfig)
                    @info "    create_worker: connected" localid=lid pid=pid elapsed_s=round(time()-t_cw; digits=1)
                    remote_do(Distributed.redirect_output_from_additional_worker, frompid, pid, port)
                    push!(launched_q, pid)
                end
            end
        end
end

#
# Azure scale-set methods
#

"""
Pull the image identity out of the scale-set template's imageReference.
The template encodes one of two shapes:

  /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Compute/galleries/<G>/images/<I>[/versions/<V>]
  /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Compute/images/<I>

Returns `(sigimagename, sigimageversion, imagename)`. Empties if the
template has neither shape, in which case the caller falls through to
`scaleset_image`'s IMDS-based resolution. When the template has a SIG
reference without an explicit version, we resolve the latest published
version via the Azure SIG API so the eventual `image_osdisksize` SIG
branch has both pieces.
"""
function _resolve_image_from_template(manager::AzManager, template_value, subscriptionid::AbstractString, resourcegroup::AbstractString)
    id = ""
    try
        if haskey(template_value["properties"], "virtualMachineProfile")
            id = template_value["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"]
        else
            id = template_value["properties"]["storageProfile"]["imageReference"]["id"]
        end
    catch
        return "", "", ""
    end

    parts = split(id, "/")
    k_galleries = findfirst(==("galleries"), parts)
    k_images    = findfirst(==("images"),    parts)
    k_versions  = findfirst(==("versions"),  parts)

    if k_galleries !== nothing && k_images !== nothing && k_images + 1 <= length(parts)
        gallery_name = String(parts[k_galleries + 1])
        sigimagename = String(parts[k_images + 1])
        if k_versions !== nothing && k_versions + 1 <= length(parts)
            return sigimagename, String(parts[k_versions + 1]), ""
        end
        # SIG image-def without an explicit version. Resolve latest.
        latest = _latest_sig_version(manager, subscriptionid, resourcegroup, gallery_name, sigimagename)
        return sigimagename, latest, ""
    elseif k_images !== nothing && k_images + 1 <= length(parts)
        # Managed image (no galleries segment).
        return "", "", String(parts[k_images + 1])
    end
    return "", "", ""
end

"""
Return the highest published image-version name (as a string) for the
given SIG image-definition, or "" if no versions are published / the
API call fails. Used by `_resolve_image_from_template` when the
template references a SIG image-def without a specific version.
"""
function _latest_sig_version(manager::AzManager, subscription::AbstractString, resourcegroup::AbstractString, gallery::AbstractString, image::AbstractString)
    try
        url = "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourcegroup/providers/Microsoft.Compute/galleries/$gallery/images/$image/versions?api-version=2022-03-03"
        _r = @retry manager.nretry azrequest(
            "GET",
            manager.verbose,
            url,
            ["Authorization" => "Bearer $(token(manager.session))"])
        r = JSON.parse(String(_r.body))
        names = String[get(v, "name", "") for v in get(r, "value", [])]
        filter!(!isempty, names)
        isempty(names) && return ""
        return string(maximum(VersionNumber.(names)))
    catch e
        @warn "could not resolve latest SIG version" gallery image exception=e
        return ""
    end
end

function scaleset_image(manager::AzManager, sigimagename, sigimageversion, imagename)
    # early exit
    if imagename != "" || (sigimagename != "" && sigimageversion != "")
        return sigimagename, sigimageversion, imagename
    end

    # get machines' metadata
    t = @async begin
        r = @retry manager.nretry HTTP.request("GET", "http://169.254.169.254/metadata/instance/compute/storageProfile/imageReference?api-version=2021-02-01", ["Metadata"=>"true"]; retry=false, redirect=false)
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
        # The two branches above require either managed-image-only
        # (imagename set, sig fields empty) or SIG-with-version
        # (sig fields set, imagename empty). Any other combination means
        # `scaleset_image()` returned something partial - usually because
        # the coordinator's IMDS query for the imageReference URL came
        # back in an unexpected shape (e.g. SIG without /versions/X and
        # the versions-listing API also didn't yield a result). Print
        # what we have so the next CI failure points at the actual gap.
        image_id = try
            if haskey(template["properties"], "virtualMachineProfile")
                template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"]
            else
                template["properties"]["storageProfile"]["imageReference"]["id"]
            end
        catch
            "<unavailable>"
        end
        error("unable to determine os disk size " *
              "(sigimagename=\"$sigimagename\", " *
              "sigimageversion=\"$sigimageversion\", " *
              "imagename=\"$imagename\", " *
              "template_image_id=\"$image_id\")")
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

function build_lvm()
    if isfile("/usr/sbin/azure_nvme.sh")
        @info "Building scratch.."
        run(`sudo bash /usr/sbin/azure_nvme.sh`)
    else 
        @warn "No scratch nvme script found!"
    end
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
        JSON.json(Dict("sku"=>Dict("capacity"=>capacity))))
end

function scaleset_create_or_update(manager::AzManager, user, subscriptionid, resourcegroup, scalesetname, sigimagename, sigimageversion,
        imagename, osdisksize, nretry, template, δn, ppi, mpi_ranks_per_worker, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig, hyperthreading, julia_num_threads,
        omp_num_threads, exename, exeflags, env, spot, maxprice, spot_base_regular_priority_count, spot_regular_percentage_above_base, verbose, custom_environment, overprovision, use_lvm)
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
    _decoded = claims(JWT(;jwt=_t))
    if haskey(_decoded, "unique_name")
        _user = _decoded["unique_name"]

        if !haskey(_template, "tags")
            _template["tags"] = Dict{Any,Any}()
        end
        _template["tags"]["UserUniqueName"] = _user
    end

    key = Dict("path" => "/home/$user/.ssh/authorized_keys", "keyData" => read(ssh_key, String))
    push!(_template["properties"]["virtualMachineProfile"]["osProfile"]["linuxConfiguration"]["ssh"]["publicKeys"], key)
    
    cmd = buildstartupscript_cluster(manager, spot, ppi, mpi_ranks_per_worker, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig, julia_num_threads, omp_num_threads, exename, exeflags, env, user, template["tempdisk"], custom_environment, use_lvm)
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

    # If the target scale-set is mid-deletion, every PUT we issue is rejected
    # by Azure with "OperationNotAllowed ... is marked for deletion" until the
    # deletion completes (minutes for non-empty sets), burning the full retry
    # budget. Fail fast - the caller should retry with a fresh `group` name.
    for _existing in get(r, "value", [])
        if _existing["name"] == scalesetname
            state = get(get(_existing, "properties", Dict()), "provisioningState", "")
            if lowercase(state) == "deleting"
                error("scale-set $resourcegroup/$scalesetname is in " *
                      "provisioningState=Deleting; pick a different " *
                      "`group` name (or wait for the deletion to finish).")
            end
            break
        end
    end

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

    @debug "done checking quota, δn=$(δn)"

    # Track the authoritative capacity in `manager.scalesets` under the manager
    # lock so a concurrent `delete_pending_down_vms` can't clobber the size we
    # PUT (the scaleset create/delete race fixed upstream in #222).
    lock(manager.lock)
    try
        _scaleset = ScaleSet(subscriptionid, resourcegroup, scalesetname)
        _template["sku"]["capacity"] = manager.scalesets[_scaleset] =
            _scaleset ∈ keys(scalesets(manager)) ? manager.scalesets[_scaleset] + δn : δn

        @debug "setting capacity of scale-set $scalesetname to $(_template["sku"]["capacity"]), scaleset=$(manager.scalesets[_scaleset])"

        _r = @retry nretry azrequest(
            "PUT",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname?api-version=2023-03-01",
            ["Content-type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"],
            String(JSON.json(_template)))

        if manager.show_quota
            @info "Quota after requesting that the scale-set is created or grows" remaining_resource(_r)
        end
    catch e
        throw(e)
    finally
        unlock(manager.lock)
    end

    nothing
end

function delete_vms(manager::AzManager, subscriptionid, resourcegroup, scalesetname, ids, nretry, verbose)
    body = Dict("instanceIds"=>ids)
    _r = @retry nretry azrequest(
        "POST",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/delete?forceDeletion=True&api-version=2023-07-01",
        ["Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"],
        JSON.json(body))

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
                        logerror(e, Logging.Debug)
                        run(`sudo rm -rf /scratch$lun`)
                    end
                end
            end
        end
    catch e
        @error "caught error formatting/mounting data disks"
        logerror(e, Logging.Debug)
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

export AzManager, DetachedJob, MachineTopology, NumaTopology, SocketTopology,
    WorkerPlacement, addproc, machine_preempt_channel_future, nphysical_cores,
    nworkers_provisioned, plan_worker_placements, preempted, rmproc, scalesets,
    status, variablebundle, variablebundle!, vm, worker_placement,
    worker_placements, @detach, @detachat

if !isdefined(Base, :get_extension)
    include("../ext/MPIExt.jl")
end

end
