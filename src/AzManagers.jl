module AzManagers

using AzSessions, Base64, CodecZlib, Dates, Distributed, HTTP, JSON, JWTs, Logging, Pkg, Sockets, TOML

# ─── Includes (order matters) ───

include("types.jl")
include("logging.jl")

# ─── Retry infrastructure (must precede azure_api.jl) ───

const RETRYABLE_HTTP_ERRORS = (409, 429, 500)

isretryable(e::HTTP.StatusError) = e.status in RETRYABLE_HTTP_ERRORS
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
    if isa(e, HTTP.StatusError)
        if e.status == 429
            @warn "Azure throttling, retry $i of $retries in $(round(s, digits=1))s"
        elseif e.status == 500
            b = JSON.parse(String(e.response.body))
            code = get(get(b, "error", Dict()), "code", "")
            @warn "Server error ($code), retry $i of $retries in $(round(s, digits=1))s"
        else
            @warn "HTTP $(e.status), retry $i of $retries in $(round(s, digits=1))s"
        end
    else
        @warn "$(typeof(e)), retry $i of $retries in $(round(s, digits=1))s"
        logerror(e, Logging.Debug)
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
                s = min(2.0^(i-1), 256) + rand()
                if status(e) in (429, 500)
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

include("azure_api.jl")
include("handlers.jl")
include("execute.jl")
include("reconcile.jl")
include("connection.jl")
include("cloud_init.jl")
include("worker.jl")
include("provisioning.jl")

# ─── Manifest ───

const _manifest = Dict("resourcegroup"=>"", "ssh_user"=>"", "ssh_private_key_file"=>"", "ssh_public_key_file"=>"", "subscriptionid"=>"")

manifestpath() = joinpath(homedir(), ".azmanagers")
manifestfile() = joinpath(manifestpath(), "manifest.json")

function write_manifest(;
        resourcegroup="",
        subscriptionid="",
        ssh_user="",
        ssh_private_key_file=joinpath(homedir(), ".ssh", "azmanagers_rsa"),
        ssh_public_key_file=joinpath(homedir(), ".ssh", "azmanagers_rsa.pub"))
    manifest = Dict(
        "resourcegroup" => resourcegroup,
        "subscriptionid" => subscriptionid,
        "ssh_user" => ssh_user,
        "ssh_private_key_file" => ssh_private_key_file,
        "ssh_public_key_file" => ssh_public_key_file)
    isdir(manifestpath()) || mkdir(manifestpath(); mode=0o700)
    write(manifestfile(), JSON.json(manifest, 1))
    chmod(manifestfile(), 0o600)
end

function load_manifest()
    if isfile(manifestfile())
        manifest = JSON.parsefile(manifestfile())
        for key in keys(_manifest)
            _manifest[key] = get(manifest, key, "")
        end
    else
        error("Manifest file ($(manifestfile())) does not exist. Use AzManagers.write_manifest to generate a manifest file.")
    end
end

# ─── Global state ───

const _state = Ref{ManagerState}()

function manager_state()
    isassigned(_state) || error("AzManagers not initialized. Call azmanager!() first.")
    _state[]
end

function azmanager!(session::AzSessionAbstract;
        ssh_user="",
        nretry=10,
        verbose=0,
        save_cloud_init_failures=false,
        show_quota=false)

    # If already initialized, update config and return
    if isassigned(_state)
        st = _state[]
        st.session = session
        st.ssh_user = ssh_user
        st.nretry = nretry
        st.verbose = verbose
        st.save_cloud_init_failures = save_cloud_init_failures
        st.show_quota = show_quota
        return st
    end

    port, server = listenany(getipaddr(), 9000)

    st = ManagerState(
        session,
        ssh_user,
        nretry,
        verbose,
        save_cloud_init_failures,
        show_quota,
        Dict{ScaleSet,Int}(),
        Dict{ScaleSet,Dict{Int,InstanceInfo}}(),
        server,
        UInt16(port),
        Channel{WorkerConfig}(parse(Int, get(ENV, "AZMANAGERS_VALIDATED_CHANNEL_SIZE", "512"))),
        Dict{Int,Future}(),
        ReentrantLock())

    _state[] = st

    # Launch async tasks
    @async accept_loop(st)
    @async batch_loop(st)
    @async reconcile_timer(st)

    st
end

# ─── Distributed interface ───

struct AzManager <: ClusterManager end

function Distributed.launch(manager::AzManager, params::Dict, launched::Array, c::Condition)
    wconfigs = params[:wconfigs]
    for wconfig in wconfigs
        push!(launched, wconfig)
        notify(c)
    end
    notify(c)
end

function Distributed.manage(manager::AzManager, id::Integer, config::WorkerConfig, op::Symbol)
    st = manager_state()
    if op == :register
        # nothing for now
    elseif op == :deregister
        actions = on_worker_exit(st, id)
        execute!(st, actions)
    end
end

function Distributed.addprocs(manager::AzManager; wconfigs)
    pids = Int[]
    try
        Distributed.init_multi()
        Distributed.cluster_mgmt_from_master_check()
        lock(Distributed.worker_lock)
        pids = Distributed.addprocs_locked(manager; wconfigs)
    catch e
        @error "addprocs error" exception=(e, catch_backtrace())
    finally
        unlock(Distributed.worker_lock)
    end
    pids
end

function Distributed.setup_launched_worker(manager::AzManager, wconfig, launched_q)
    st = manager_state()
    timeout = parse(Float64, get(ENV, "AZMANAGERS_BATCH_TIMEOUT", "10")) - 2
    interrupted = false

    local pid
    try
        tsk = @async Distributed.create_worker(manager, wconfig)
        tic = time()
        while true
            if istaskdone(tsk)
                pid = fetch(tsk)
                break
            end
            if time() - tic > timeout && !interrupted
                @async Base.throwto(tsk, InterruptException())
                interrupted = true
            end
            if interrupted && (time() - tic) > timeout + 3
                error("create_worker did not terminate after interrupt")
            end
            sleep(1)
        end
    catch e
        u = wconfig.userdata
        instanceid = u["instanceid"]
        @warn "worker failed to register" instanceid scalesetname=get(u, "scalesetname", "unknown") timeout
        lock(st.lock) do
            ss = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
            execute!(st, QueuePendingDown(ss, instanceid))
        end
        return
    end

    u = wconfig.userdata
    push!(launched_q, pid)

    cnt = something(wconfig.count, 1)
    cnt = cnt === :auto ? wconfig.environ[:cpu_threads] : cnt
    cnt -= 1
    if cnt > 0
        Distributed.launch_n_additional_processes(manager, pid, wconfig, cnt, launched_q)
    end
end

function addprocs_with_timeout(; wconfigs)
    timeout = parse(Float64, get(ENV, "AZMANAGERS_BATCH_TIMEOUT", "10"))
    tsk = @async addprocs(AzManager(); wconfigs)
    tic = time()
    pids = Int[]
    interrupted = false

    while true
        elapsed = time() - tic
        if elapsed > timeout && !interrupted
            @warn "interrupting addprocs due to timeout"
            @async Base.throwto(tsk, InterruptException())
            interrupted = true
        end
        if interrupted && (time() - tic) > timeout + 5
            @error "addprocs did not terminate after interrupt, abandoning batch"
            break
        end
        if istaskdone(tsk) && istaskfailed(tsk)
            try fetch(tsk) catch e; @warn "addprocs failed" exception=(e, catch_backtrace()) end
            break
        end
        if istaskdone(tsk) && !istaskfailed(tsk)
            pids = fetch(tsk)
            break
        end
        sleep(1)
    end
    pids
end

# ─── Public queries ───

scalesets() = isassigned(_state) ? _state[].scalesets : Dict{ScaleSet,Int}()

function nworkers_provisioned()
    isassigned(_state) || return 0
    st = _state[]
    total = sum(values(st.scalesets); init=0)
    pending_down_count = 0
    for (_, instances) in st.instances
        for (_, info) in instances
            info.state == PENDING_DOWN && (pending_down_count += 1)
        end
    end
    max(0, total - pending_down_count)
end

# ─── Scaleset provisioning ───

include("templates.jl")

function Distributed.addprocs(template::AbstractString, n::Int; kwargs...)
    isfile(templates_filename_scaleset()) || error("scale-set template file does not exist. See AzManagers.save_template_scaleset")
    templates = JSON.parse(read(templates_filename_scaleset(), String); dicttype=Dict)
    haskey(templates, template) || error("scale-set template file does not contain template: $template")
    addprocs(templates[template], n; kwargs...)
end

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
        julia_num_threads = "$(Threads.nthreads()),$(Threads.nthreads(:interactive))",
        omp_num_threads = parse(Int, get(ENV, "OMP_NUM_THREADS", "1")),
        exename = "julia",
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
        mpi_flags = "-bind-to core:$(get(ENV, "OMP_NUM_THREADS", "1")) --map-by numa",
        nvidia_enable_ecc = true,
        nvidia_enable_mig = false,
        hyperthreading = nothing,
        use_lvm = false)

    (subscriptionid == "" || resourcegroup == "" || user == "") && load_manifest()
    subscriptionid == "" && (subscriptionid = get(template, "subscriptionid", _manifest["subscriptionid"]))
    resourcegroup == "" && (resourcegroup = get(template, "resourcegroup", _manifest["resourcegroup"]))
    user == "" && (user = _manifest["ssh_user"])

    st = azmanager!(session; ssh_user=user, nretry, verbose, save_cloud_init_failures, show_quota)

    sigimagename, sigimageversion, imagename = resolve_image(st, sigimagename, sigimageversion, imagename)
    check_environment(customenv)

    scaleset = ScaleSet(subscriptionid, resourcegroup, group)

    capacity = create_or_update_scaleset!(st, scaleset, template, n;
        sigimagename, sigimageversion, imagename, osdisksize,
        customenv, overprovision, ppi,
        julia_num_threads, omp_num_threads,
        exename, exeflags, env,
        spot, maxprice,
        spot_base_regular_priority_count, spot_regular_percentage_above_base,
        mpi_ranks_per_worker, mpi_flags,
        nvidia_enable_ecc, nvidia_enable_mig, hyperthreading, use_lvm)

    if capacity > 0
        st.scalesets[scaleset] = capacity
    end

    if waitfor
        n_target = (nprocs() == 1 ? 0 : nworkers()) + n
        @info "Waiting for $n_target workers..."
        while nworkers() < n_target
            sleep(5)
        end
    end

    nothing
end

# ─── Cleanup ───

function delete_scalesets()
    isassigned(_state) || return
    st = _state[]
    @sync for ss in keys(st.scalesets)
        @async rmgroup(st, ss)
    end
end

function __init__()
    myid() == 1 && atexit(delete_scalesets)
end

# ─── Exports ───

export AzManager, addproc, machine_preempt_channel_future, nworkers_provisioned, scalesets, write_manifest, load_manifest,
    azure_worker, azure_worker_mpi, mount_datadisks, build_lvm, nvidia_gpucheck, decompress_environment

end # module
