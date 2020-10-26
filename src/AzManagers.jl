module AzManagers

const _kill_list = Task[]
const _scaleset_counts = Dict{Tuple{String,String},Int}()

function wait_for_empty_kill_list()
    wait.(_kill_list)
    empty!(_kill_list)
    nothing
end

__init__() = atexit(AzManagers.wait_for_empty_kill_list)

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

_pollworkers_task = nothing

using AzSessions, Base64, Dates, Distributed, HTTP, JSON, LibGit2, Logging, MPI, Pkg, Printf, Random, Serialization, Sockets

const RETRYABLE_HTTP_ERRORS = (
    409,  # Conflict
    429,  # Too many requests
    500)  # Internal server error

function isretryable(e::HTTP.StatusError)
    e.status ∈ RETRYABLE_HTTP_ERRORS && (return true)
    false
end
isretryable(e::Base.IOError) = true
isretryable(e::HTTP.IOExtras.IOError) = isretryable(e.e)
isretryable(e::Base.EOFError) = true
isretryable(e::Sockets.DNSError) = true
isretryable(e) = false

status(e::HTTP.StatusError) = e.status
status(e) = 999

function retrywarn(i, retries, s, e)
    if isa(e, HTTP.ExceptionRequest.StatusError)
        @debug "$(e.status): $(String(e.response.body)), retry $i of $retries, retrying in $s seconds"
    else
        @warn "warn: $(typeof(e)) -- retry $i, retrying in $s seconds"
    end
end

macro retry(retries, ex::Expr)
    quote
        local r
        for i = 0:$(esc(retries))
            try
                r = $(esc(ex))
                break
            catch e
                (i <= $(esc(retries)) && isretryable(e)) || rethrow(e)
                maximum_backoff = 256
                local s
                if status(e) == 429
                    s = min(2.0^(i-1), maximum_backoff) + rand()
                    for header in e.response.headers
                        if header[1] == "retry-after"
                            s = parse(Int, header[2]) + rand()
                            break
                        end
                    end
                else
                    s = min(2.0^(i-1), maximum_backoff) + rand()
                end
                retrywarn(i, $(esc(retries)), s, e)
                sleep(s)
            end
        end
        r
    end
end

function azrequest(rtype, verbose, url, headers, body=nothing)
    options = (retry=false, status_exception=false)
    if body == nothing
        r = HTTP.request(rtype, url, headers; verbose=verbose, options...)
    else
        r = HTTP.request(rtype, url, headers, body; verbose=verbose, options...)
    end
    
    if r.status >= 300
        throw(HTTP.ExceptionRequest.StatusError(r.status, r))
    end
    
    r
end

mutable struct AzManager{S<:AzSessionAbstract} <: ClusterManager
    vms::Vector{Dict{String,Any}}
    session::S
    
    function AzManager(vms, session::S) where {S<:AzSessionAbstract}
        manager = new{S}(vms, session)
    end
end
AzManager(session=AzSession(;lazy=true)) = AzManager(Dict[], session)

include("templates.jl")

"""
    addprocs(template, ninstances[; kwargs...])

Add Azure scale set instances where template is either a dictionary produced via the `AzManagers.build_sstemplate`
method or a string corresponding to a template stored in `~/.azmanagers/templates_scaleset.json.`

# key word arguments:
* `subscription=AzManagers._manifest["subscriptionid"]`
* `resourcegroup=AzManagers._manifest["resourcegroup"]`
* `sigimagename=""` The name of the SIG image[1].
* `sigimageversion=""` The version of the `sigimagename`[1].
* `imagename=""` The name of the image (alternative to `sigimagename` and `sigimageversion` used for development work).
* `session=AzSession(;lazy=true)` The Azure session used for authentication.
* `group="cbox"` The name of the Azure scale set.  If the scale set does not yet exist, it will be created.
* `ppi=1` The number of Julia processes to start per Azure scale set instance.
* `nretry=20` Number of retries for HTTP REST calls to Azure services.
* `verbose=0` verbose flag passed on to libcurl.
* `user=AzManagers._manifest["ssh_user"]` ssh user.
* `spot=false` use Azure SPOT VMs for the scale-set
* `maxprice=-1` set maximum price per hour for a VM in the scale-set.  `-1` uses the market price.
* `mpi_ranks_per_worker=0` set the number of MPI ranks per Julia worker[2]
* `mpi_flags="-bind-to core:\$(ENV["OMP_NUM_THREADS"]) -map-by numa"` extra flags to pass to mpirun (has effect when `mpi_ranks_per_worker>0`)

# Notes
[1] If `addprocs` is called from an Azure VM, then the default `imagename`,`imageversion` are the
image/version the VM was built with; otherwise, it is the latest version of the image specified in the scale-set template.
[2] This is inteneded for use with Devito.  In particular, it allows Devito to gain performance by using
MPI to do domain decomposition using MPI within a single VM.  If `mpi_ranks_per_worker=0`, then MPI is not
used on the Julia workers.
"""
function Distributed.addprocs(template::Dict, nprocs::Int;
        subscriptionid = "",
        resourcegroup = "",
        sigimagename = "",
        sigimageversion = "",
        imagename = "",
        session = AzSession(;lazy=true),
        group = "cbox",
        ppi = 1,
        nretry = 20,
        verbose = 0,
        user = "",
        spot = false,
        maxprice = -1,
        mpi_ranks_per_worker = 0,
        mpi_flags = "-bind-to core:$(get(ENV, "OMP_NUM_THREADS", 1)) -map-by numa")
    (subscriptionid == "" || resourcegroup == "" || user == "") && load_manifest()
    subscriptionid == "" && (subscriptionid = _manifest["subscriptionid"])
    resourcegroup == "" && (resourcegroup = _manifest["resourcegroup"])
    user == "" && (user = _manifest["ssh_user"])

    manager = AzManager(session)
    pids = addprocs(manager;
        subscriptionid = subscriptionid,
        resourcegroup = resourcegroup,
        sigimagename = sigimagename,
        sigimageversion = sigimageversion,
        imagename = imagename,
        scalesetname = group,
        session = session,
        template = template,
        targetsize = nprocs,
        ppi = ppi,
        nretry = nretry,
        verbose = verbose,
        user = user,
        spot = spot,
        maxprice = maxprice,
        mpi_ranks_per_worker = mpi_ranks_per_worker,
        mpi_flags = mpi_flags)

    @eval(Main, using Distributed)
    @eval(Main, @everywhere using Distributed, AzManagers)

    @debug "calling pollworkers"
    if !isa(_pollworkers_task, Task) || istaskdone(_pollworkers_task)
        global _pollworkers_task = @async pollworkers(manager, verbose)
    end

    pids
end

function Distributed.addprocs(template::AbstractString, nprocs::Int; kwargs...)
    isfile(templates_filename_scaleset()) || error("scale-set template file does not exist.  See `AzManagers.save_template_scaleset`")

    templates_scaleset = JSON.parse(read(templates_filename_scaleset(), String))
    haskey(templates_scaleset, template) || error("scale-set template file does not contain a template with name: $template. See `AzManagers.save_template_scaleset`")

    addprocs(templates_scaleset[template], nprocs; kwargs...)
end

spin(spincount, elapsed_time) = ['◐','◓','◑','◒','✓'][spincount]*@sprintf(" %.2f",elapsed_time)*" seconds"
function spinner(launch_tasks, verb)
    ws = repeat(" ", 5)
    spincount = 1
    starttime = time()
    elapsed_time = 0.0
    ntasks_done = 0
    ntasks = length(launch_tasks)
    while ntasks_done < ntasks
        elapsed_time = time() - starttime
        ntasks_done = mapreduce(launch_task->Int(istaskdone(launch_task)), +, launch_tasks)
        write(stdout, spin(spincount, elapsed_time)*", $ntasks_done/$ntasks vms $verb. $ws\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1
        yield()
        sleep(.25)
    end
    write(stdout, spin(5, elapsed_time)*", $ntasks_done/$ntasks vms $verb. $ws\r")
    write(stdout,"\n")
    nothing
end

function Distributed.launch(manager::AzManager, params::Dict, launched::Array, c::Condition)
    @debug "getting image info"
    sigimagename, sigimageversion, imagename = scaleset_image(manager, params[:template], params[:sigimagename], params[:sigimageversion], params[:imagename])

    @debug "software sanity check"
    custom_environment = software_sanity_check(manager, imagename == "" ? sigimagename : imagename)

    @info "Adding vm's to scale-set $(params[:scalesetname])"
    if custom_environment
        @warn "Updating scale-set VM software due to customized environment, patience please."
    end
    @debug "Waiting for empty kill list"
    wait_for_empty_kill_list()
    @debug "Getting existing vms"
    preexisting_vms = scaleset_listvms(manager, params[:template], params[:subscriptionid], params[:resourcegroup], params[:scalesetname], params[:nretry], params[:verbose])
    @debug "Updating scaleset, custom_environment=$custom_environment"
    n = scaleset_create_or_update(manager, params[:user], params[:subscriptionid], params[:resourcegroup], params[:scalesetname], sigimagename, sigimageversion, imagename, params[:nretry], params[:template], params[:targetsize], params[:spot], params[:maxprice], params[:verbose], custom_environment)
    n < 0 && return

    @debug "Waiting for stable scaleset"
    scaleset_wait_until_stable(manager, params[:subscriptionid], params[:resourcegroup], params[:scalesetname], params[:nretry], length(preexisting_vms)+params[:targetsize], params[:verbose])
    @debug "Listing vms in scaleset"
    vms = scaleset_listvms(manager, params[:template], params[:subscriptionid], params[:resourcegroup], params[:scalesetname], params[:nretry], params[:verbose])

    if haskey(_scaleset_counts, params[:scalesetname])
        _scaleset_counts[(params[:resourcegroup],params[:scalesetname])] += params[:targetsize]
    else
        _scaleset_counts[(params[:resourcegroup],params[:scalesetname])] = params[:targetsize]
    end

    # get the set of machines that are in `vms` and are not in `preexisting_vms`
    manager.vms = Dict[]
    for vm in vms
        i = findfirst(preexisting_vm->vm["name"]==preexisting_vm["name"], preexisting_vms)
        if i == nothing
            push!(manager.vms, vm)
        end
    end

    if custom_environment # TODO: is the if here a little dangerous? The if statement is to save some start-up time.
        @info "Waiting for cloud-init"
        scaleset_wait_for_cloud_init(manager, manager.vms, params[:subscriptionid], params[:resourcegroup], params[:scalesetname], params[:nretry], params[:verbose])
    end

    @info "Initiating cluster..."
    launch_tasks = [Task(nothing) for i = 1:length(manager.vms)]
    spinner_task = @async spinner(launch_tasks, "configured")

    for (i, vm) in enumerate(manager.vms)
        let vm = vm
            launch_tasks[i] = @async try
                Distributed.launch_on_machine(manager, vm, params, launched, c)
            catch e
                @debug "caught launch error for id=$(vm["instanceid"])"
                ioe = IOBuffer()
                showerror(ioe, e)
                @warn "exception launching on machine $(vm["name"]) : $(String(take!(ioe)))"
                close(ioe)
            end
        end
        yield()
    end

    for launch_task in launch_tasks
        wait(launch_task)
    end
    wait(spinner_task)
    @info "Finalizing cluster..."

    notify(c)
end

function launchcmd(omp_num_threads, user, vm)
    load_manifest()

    ssh_id = _manifest["ssh_private_key_file"]
    exeflags = `--worker`
    exename = "julia"
    cmds = """$(Base.shell_escape_posixly(exename)) $(Base.shell_escape_posixly(exeflags))"""
    cmd = `env OMP_NUM_THREADS=$omp_num_threads sh -l -c $cmds`
    `ssh -i $ssh_id -T -a -x -o ClearAllForwardings=yes $user$(vm["bindaddr"]) $(Base.shell_escape_posixly(cmd))`
end

function launchcmd_mpi(mpi_ranks_per_worker, omp_num_threads, mpi_flags, cookie, user, vm)
    load_manifest()

    ssh_id = _manifest["ssh_private_key_file"]
    _cmd = "mpirun -n $mpi_ranks_per_worker -env OMP_NUM_THREADS=$omp_num_threads $mpi_flags julia -e \"using AzManagers, MPI; AzManagers.start_worker_mpi(\\\"$cookie\\\")\""
    `ssh -i $ssh_id -T -a -x -o ClearAllForwardings=yes $user$(vm["bindaddr"]) $_cmd`
end

function Distributed.launch_on_machine(manager::AzManager, vm, params, launched, launch_ntfy::Condition)
    cookie = Distributed.cluster_cookie()
    mpi_ranks_per_worker = params[:mpi_ranks_per_worker]
    mpi_flags = params[:mpi_flags]
    omp_num_threads = get(ENV, "OMP_NUM_THREADS", 16)
    user = params[:user] == "" ? "" : "$(params[:user])@"

    cmd = mpi_ranks_per_worker == 0 ? launchcmd(omp_num_threads, user, vm) : launchcmd_mpi(mpi_ranks_per_worker, omp_num_threads, mpi_flags, cookie, user, vm)

    timeout = Distributed.worker_timeout()
    tic = time()

    local io
    @debug "executing ssh command for id=$(vm["instanceid"])"
    while true
        sleep(10+10*rand()) # this is a massive kludge and plays into the theory that using ssh introduces brittleness!
        io = open(detach(cmd), "r+")
        io.exitcode == 255 || break
        if time() - tic > timeout
            error("unable to connect to $(vm["host"])($(vm["bindaddr"])) in $timeout seconds.")
        else
            @warn "unable to connect to $(vm["host"])($(vm["bindaddr"])) retrying in 1 second."
            sleep(1)
        end
    end
    @debug "done executing ssh command for id=$(vm["instanceid"])"
    Distributed.write_cookie(io)

    wconfig = WorkerConfig()
    wconfig.io = io.out
    wconfig.host = vm["host"]
    wconfig.bind_addr = vm["bindaddr"]
    wconfig.tunnel = false
    wconfig.sshflags = ``
    wconfig.exeflags = `--worker`
    wconfig.exename = "julia"
    wconfig.count = params[:ppi]
    wconfig.max_parallel = 10
    wconfig.enable_threaded_blas = params[:enable_threaded_blas]
    wconfig.userdata = Dict(
        "subscriptionid" => params[:subscriptionid],
        "resourcegroup" => params[:resourcegroup],
        "scalesetname" => params[:scalesetname],
        "nretry" => params[:nretry],
        "verbose" => params[:verbose],
        "instanceid" => vm["instanceid"],
        "name" => vm["name"])

    push!(launched, wconfig)
    notify(launch_ntfy)
end

#
# MPI Specific methods --
# These methods are slightly modified versions of what is in the base Distributed standard library.
#
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
            end

            wait(tsk)
            MPI.Barrier(comm)
        catch e
            io = IOBuffer()
            showerror(io, e)
            @warn "MPI - message_handler_loop_mpi -- caught error: $(String(take!(io)))"
        end    
    end
end

start_worker_mpi_rank0(cookie::AbstractString=readline(stdin); kwargs...) = start_worker_mpi_rank0(stdout, cookie; kwargs...)
function start_worker_mpi_rank0(out::IO, cookie::AbstractString=readline(stdin); close_stdin::Bool=true, stderr_to_stdout::Bool=true)
    Distributed.init_multi()

    close_stdin && close(stdin) # workers will not use it
    stderr_to_stdout && redirect_stderr(stdout)

    init_worker(cookie)
    interface = IPv4(Distributed.LPROC.bind_addr)
    if Distributed.LPROC.bind_port == 0
        port_hint = 9000 + (getpid() % 1000)
        (port, sock) = listenany(interface, UInt16(port_hint))
        Distributed.LPROC.bind_port = port
    else
        sock = listen(interface, Distributed.LPROC.bind_port)
    end
    @async while isopen(sock)
        client = accept(sock)
        process_messages_mpi_rank0(client, client, true)
    end
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

    try
        # To prevent hanging processes on remote machines, newly launched workers exit if the
        # master process does not connect in time.
        Distributed.check_master_connect()
        while true; wait(); end
    catch err
        print(stderr, "unhandled exception on $(myid()): $(err)\nexiting.\n")
    end

    close(sock)
    exit(0)
end

function start_worker_mpi(cookie)
    MPI.Initialized() || MPI.Init()

    comm = MPI.COMM_WORLD
    mpi_size = MPI.Comm_size(comm)
    mpi_rank = MPI.Comm_rank(comm)

    local t
    if mpi_rank == 0
        t = @async start_worker_mpi_rank0(cookie)
    else
        t = @async message_handler_loop_mpi_rankN()
    end

    MPI.Barrier(comm)

    fetch(t)

    MPI.Barrier(comm)
end

#
# end of MPI specfic methods
#

# check if the machine has received a spot preempt message
"""
    preempted([id=myid()])

Check to see if the machine `id::Int` has received an Azure spot preempt message.  Returns
true if a preempt message is received and false otherwise.
"""
function preempted()
    _r = HTTP.request("GET", "http://169.254.169.254/metadata/scheduledevents?api-version=2019-08-01", Dict("Metadata"=>"true"))
    r = JSON.parse(String(_r.body))
    for event in r["Events"]
        if get(event, "EventType", "") == "Preempt"
            @info "event=$event"
            return true
        end
    end
    return false
end
preempted(id) = remotecall_fetch(preempted, id)

# TODO - according to the folks at julia computing, we should not need this method.
function pollworkers(manager::AzManager, verbose)
    while true
        sleep(60)
        @debug "polling workers..."
        yield()
        nprocs() == 1 && break
        scalesetnames = String[]
        for pid in workers()
            pid == 1 && continue
            u = Distributed.map_pid_wrkr[pid].config.userdata
            if haskey(u, "scalesetname")
                u["scalesetname"] ∉ scalesetnames && push!(scalesetnames, u["scalesetname"])
            end
        end
        isempty(scalesetnames) && break

        # TODO - we assume that all workers are in the same subscription and resource group
        subscriptionid = ""
        resourcegroup = ""
        for pid in workers()
            pid == 1 && continue
            u = Distributed.map_pid_wrkr[pid].config.userdata
            if haskey(u, "subscriptionid")
                subscriptionid = u["subscriptionid"]
            end
            if haskey(u, "resourcegroup")
                resourcegroup = u["resourcegroup"]
            end
            (subscriptionid == "" || resourcegroup == "") || break
        end
        (resourcegroup == "" || subscriptionid == "") && continue

        vmnames = []
        try
            for scalesetname in scalesetnames
                _r = azrequest(
                    "GET",
                    verbose,
                    "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/virtualMachines?api-version=2019-12-01",
                    Dict("Authorization"=>"Bearer $(token(manager.session))"))
                r = JSON.parse(String(_r.body))
                vms = getnextlinks!(manager, get(r, "value", []), get(r, "nextLink", ""), 0, verbose)

                for vm in vms
                    push!(vmnames, vm["name"])
                end
            end
        catch e
            @warn "AzManagers pollworkers: unable to fetch vm names"
            continue
        end

        for pid in workers()
            try
                pid == 1 && continue
                pid ∈ Distributed.map_del_wrkr && continue
                u = Distributed.map_pid_wrkr[pid].config.userdata
                if u["name"] ∉ vmnames
                    @info "AzManagers found a zombie worker for pid=$pid, de-registering."
                    yield()
                    Distributed.deregister_worker(pid)
                end
            catch
                @warn "AzManagers pollworkers: unable to inspect worker with pid=$pid"
            end
        end
    end
    @debug "polling is done."
end

function is_vm_in_scaleset(manager::AzManager, config::WorkerConfig)
    u = config.userdata
    _r = @retry u["nretry"] azrequest(
        "GET",
        u["verbose"],
        "https://management.azure.com/subscriptions/$(u["subscriptionid"])/resourceGroups/$(u["resourcegroup"])/providers/Microsoft.Compute/virtualMachineScaleSets/$(u["scalesetname"])/virtualMachines?api-version=2019-12-01",
        Dict("Authorization"=>"Bearer $(token(manager.session))"))

    hasit = false
    r = JSON.parse(String(_r.body))
    for vm in get(r, "value", [])
        if get(vm, "name", "") == u["name"]
            hasit = true
        end
    end
    hasit
end

function _kill(manager::AzManager, id::Int, config::WorkerConfig)
    remote_do(exit, id)

    u = config.userdata

    u == nothing && (return nothing) # rely on additional workers (on an instance) not having their config.userdata populated

    # check if the vm was removed without the help of AzManagers (e.g. a SPOT instance that we did not catch in time)
    if !is_vm_in_scaleset(manager, config)
        @debug "rmprocs: vm is not in scale-set"
        if haskey(Distributed.map_pid_wrkr, id)
            w = Distributed.map_pid_wrkr[id]
            Distributed.set_worker_state(w, Distributed.W_TERMINATED)
        end
        return nothing
    end

    body = String(json(Dict("instanceIds" => [u["instanceid"]])))
    sleep(1+10*rand()) # Azure seems to have fairly strict rate limits for it API.  This tries to reduce the number of retries that are required.
    @debug "posting delete request for id=$id"
    @retry u["nretry"] azrequest(
        "POST",
        u["verbose"],
        "https://management.azure.com/subscriptions/$(u["subscriptionid"])/resourceGroups/$(u["resourcegroup"])/providers/Microsoft.Compute/virtualMachineScaleSets/$(u["scalesetname"])/delete?api-version=2018-06-01",
        Dict("Content-type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"),
        body)

    while true
        local _r
        try
            _r = @retry u["nretry"] azrequest(
                "GET",
                u["verbose"],
                "https://management.azure.com/subscriptions/$(u["subscriptionid"])/resourceGroups/$(u["resourcegroup"])/providers/Microsoft.Compute/virtualMachineScaleSets/$(u["scalesetname"])/virtualmachines/$(u["instanceid"])?api-version=2018-06-01",
                Dict("Authorization"=>"Bearer $(token(manager.session))"))
        catch e
            if isa(e, HTTP.ExceptionRequest.StatusError) && e.status == 404
                break
            end
            @warn "failed to verify removal of $(config.host) from scale-set, manual clean-up may be needed (error=$e)"
            break
        end
        r = JSON.parse(String(_r.body))
        @debug "instance status for id=$id: $(r["properties"]["provisioningState"])"

        if r["properties"]["provisioningState"] != "Succeeded"
            break
        end
        sleep(60+10*rand()) # attempt to avoid Azure's hourly rate limit (error 429) -- is there a better way to do this?
    end
    @debug "Deleting id=$id, _kill_list: done=$(sum([istaskdone(tsk) for tsk in _kill_list])+1), total=$(length(_kill_list))" # '+1' optimistically includes itself

    _scaleset_counts[(u["resourcegroup"],u["scalesetname"])] -= 1
    @debug "scale set count for $(u["scalesetname"]) in $(u["resourcegroup"]) is $(_scaleset_counts[(u["resourcegroup"],u["scalesetname"])])"
    if _scaleset_counts[(u["resourcegroup"],u["scalesetname"])] == 0
        @debug "found an empty scale set, deleting it."
        try
            @retry u["nretry"] azrequest(
                "DELETE",
                u["verbose"],
                "https://management.azure.com/subscriptions/$(u["subscriptionid"])/resourceGroups/$(u["resourcegroup"])/providers/Microsoft.Compute/virtualMachineScaleSets/$(u["scalesetname"])?api-version=2019-12-01",
                Dict("Authorization"=>"Bearer $(token(manager.session))"))
        catch
            @warn "unable to remove empty scaleset $(u["scalesetname"]) in resource group $(u["resourcegroup"])"
        end
    end

    nothing
end

function Distributed.kill(manager::AzManager, id::Int, config::WorkerConfig)
    t = @async _kill(manager, id, config)
    push!(_kill_list, t)
    @debug "removal of instance $id in scaleset $(config.userdata["scalesetname"]) has been scheduled"
    nothing
end

function Distributed.manage(manager::AzManager, id::Integer, config::WorkerConfig, op::Symbol)
    if op == :interrupt
        @info "TODO, what should AzManagers do when it gets an interrupt signal"
    end
end

#
# detached service
#
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
    logurl::String
end
DetachedJob(ip, id) = DetachedJob(Dict("ip"=>string(ip)), string(id), "")

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
                r = HTTP.request("GET", "http://$(vm["ip"]):8081/cofii/detached/ping")
                break
            catch
                tic = time()
            end
        end

        elapsed_time = time() - starttime

        if elapsed_time > timeout
            error("reached timeout ($timeout seconds) while waiting for $waitfor to start.")
        end
        
        write(stdout, spin(spincount, elapsed_time)*", waiting for $waitfor on VM, $(vm["name"]), to start.\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1
        
        sleep(0.5)
    end
    write(stdout, spin(5, elapsed_time)*", waiting for $waitfor on VM, $(vm["name"]), to start.\r")
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

function detached_run(code, ip::String="";
        persist=true,
        vm_template = "",
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
            sigimagename = sigimagename,
            sigimageversion = sigimageversion,
            imagename = imagename,
            nretry = nretry,
            verbose = verbose)
    else
        r = HTTP.request(
            "GET",
            "http://$ip:8081/cofii/detached/vm")
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
        "http://$(vm["ip"]):8081/cofii/detached/run",
        Dict("Content-Type"=>"application/json"),
        json(body))
    r = JSON.parse(String(_r.body))

    @info "detached job id is $(r["id"]) at $(vm["name"]),$(vm["ip"])"
    DetachedJob(vm, string(r["id"]), "")
end

detached_run(code, vm::Dict; kwargs...) = detached_run(code, vm["ip"]; kwargs...)

# conveniece API around REST calls:
"""
    read(job[;stdio=stdout])

returns the stdout from a detached job.
"""
function Base.read(job::DetachedJob; stdio=stdout)
    r = HTTP.request(
        "GET",
        "http://$(job.vm["ip"]):8081/cofii/detached/job/$(job.id)/$(stdio==stdout ? "stdout" : "stderr")")
    String(r.body)
end

"""
    status(job)

returns the status of a detached job.
"""
function status(job::DetachedJob)
    _r = HTTP.request(
        "GET",
        "http://$(job.vm["ip"]):8081/cofii/detached/job/$(job.id)/status")
    r = JSON.parse(String(_r.body))
    r["status"]
end

"""
    wait(job[;stdio=stdout])

blocks until the detached job, `job`, is complete.
"""
function Base.wait(job::DetachedJob)
    HTTP.request(
        "POST",
        "http://$(job.vm["ip"]):8081/cofii/detached/job/$(job.id)/wait")
end

function loguri(job::DetachedJob)
    job.logurl
end

const DETACHED_ROUTER = HTTP.Router()
const DETACHED_JOBS = Dict()
const DETACHED_VM = Ref(Dict())

let DETACHED_ID::Int = 1
    global detached_nextid
    detached_nextid() = (id = DETACHED_ID; DETACHED_ID += 1; id)
end

function detachedservice(port=8081, address=ip"0.0.0.0"; server=nothing, subscriptionid="", resourcegroup="", vmname="")
    HTTP.@register(DETACHED_ROUTER, "POST", "/cofii/detached/run", detachedrun)
    HTTP.@register(DETACHED_ROUTER, "POST", "/cofii/detached/job/*/wait", detachedwait)
    HTTP.@register(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/status", detachedstatus)
    HTTP.@register(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/stdout", detachedstdout)
    HTTP.@register(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/stderr", detachedstderr)
    HTTP.@register(DETACHED_ROUTER, "GET", "/cofii/detached/ping", detachedping)
    HTTP.@register(DETACHED_ROUTER, "GET", "/cofii/detached/vm", detachedvminfo)

    AzManagers.DETACHED_VM[] = Dict("subscriptionid"=>string(subscriptionid), "resourcegroup"=>string(resourcegroup),
        "name"=>string(vmname), "ip"=>string(getipaddr()))

    global_logger(ConsoleLogger(stdout, Logging.Info))

    HTTP.serve(DETACHED_ROUTER, address, port; server=server)
end

function detachedrun(request::HTTP.Request)
    local task, id, r

    try
        r = JSON.parse(String(HTTP.payload(request)))

        if !haskey(r, "code")
            return HTTP.Response(400, Dict("Content-Type"=>"application/json"); body=json(Dict("Malformed body: JSON body must contain the key: code")))
        end

        _tempname = tempname(;cleanup=false)
        @info "_tempname=$_tempname"

        io = open(_tempname, "w")

        if haskey(r, "variablebundle")
            variablebundle!(deserialize(IOBuffer(base64decode(r["variablebundle"]))))
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
            return HTTP.Response(400, Dict("Content-Type"=>"application/json"); body=json(Dict("error"=>"No code to execute, missing end?", "code"=>code)))
        end

        code = join(codelines, "\n")
        @info "code=$code"

        write(io, code)
        close(io)

        id = detached_nextid()
        outfile = "job-$id.out"
        errfile = "job-$id.err"
        task = @async begin
           open(outfile, "w") do out
               open(errfile, "w") do err
                      redirect_stdout(out) do
                          redirect_stderr(err) do
                            try
                                Base.eval(Main, :(include($_tempname)))
                            catch e
                                showerror(stderr, e)

                                write(stderr, "\n\n")

                                title = "Code listing ($_tempname)"
                                write(stderr, title*"\n")
                                write(stderr, "-"^length(title)*"\n")
                                nlines = countlines(_tempname)
                                pad = nlines > 0 ? floor(Int,log10(nlines)) : 0
                                for (iline,line) in enumerate(readlines(_tempname))
                                    write(stderr, "$(lpad(iline,pad)): $line\n")
                                end
                                write(stderr, "\n")
                                flush(stderr)
                                throw(e)
                            end
                          end
                      end
               end
           end
        end
        DETACHED_JOBS[string(id)] = Dict("task"=>task, "request"=>request, "stdout"=>outfile, "stderr"=>errfile, "codefile"=>_tempname, "code"=>code)
    catch e
        io = IOBuffer()
        showerror(io, e, catch_backtrace())
        return HTTP.Response(500, Dict("Content-Type"=>"application/json"); body=json(Dict("error"=>String(take!(io)))))
    end
    if !r["persist"]
        @async begin
            wait(task)
            vm = AzManagers.DETACHED_VM[]
            rmproc(vm; session=sessionbundle(:management))
        end
    end
    return HTTP.Response(200, Dict("Content-Type"=>"application/json"); body=json(Dict("id"=>id)))
end

function detachedstatus(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, Dict("Content-Type"=>"application/text"); body="ERROR: Unable to find job id.")
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, Dict("Content-Type"=>"application/text"); body="ERROR: Job with id=$id does not exist.")
    end

    local status
    try
        task = DETACHED_JOBS[id]["task"]

        if !istaskstarted(task)
            status = "starting"
        elseif istaskfailed(task)
            # todo, attach error message and backtrace from the task
            status = "failed"
        elseif istaskdone(task)
            status = "done"
        else
            status = "running"
        end
    catch e
        return HTTP.Response(500, Dict("Content-Type"=>"application/json"); body=json(Dict("error"=>show(e), "trace"=>show(stacktrace()))))
    end
    HTTP.Response(200, Dict("Content-Type"=>"application/json"); body=json(Dict("id"=>id, "status"=>status)))
end

function detachedstdout(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, Dict("Content-Type"=>"application/text"); body="ERROR: Unable to find job id.")
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, Dict("Content-Type"=>"application/text"); body="ERROR: Job with id=$id does not exist.")
    end

    local stdout
    if isfile(DETACHED_JOBS[id]["stdout"])
        stdout = read(DETACHED_JOBS[id]["stdout"])
    else
        stdout = ""
    end
    HTTP.Response(200, Dict("Content-Type"=>"application/text"); body=stdout)
end

function detachedstderr(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, Dict("Content-Type"=>"application/text"); body="ERROR: Unable to find job id.")
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, Dict("Content-Type"=>"application/text"); body="ERROR: Job with id=$id does not exist.")
    end

    local stderr
    if isfile(DETACHED_JOBS[id]["stderr"])
        stderr = read(DETACHED_JOBS[id]["stderr"])
    else
        stderr = ""
    end

    HTTP.Response(200, Dict("Content-Type"=>"application/text"); body=stderr)
end

function detachedwait(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(400, Dict("Content-Type"=>"application/text"); body="ERROR: Unable to find job id.")
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(400, Dict("Content-Type"=>"application/string"); body="ERROR: Job with id=$id does not exist.")
    end

    try
        task = DETACHED_JOBS[id]["task"]
        wait(task)
    catch e
        io = IOBuffer()
        showerror(io, e)

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

        return HTTP.Response(400, Dict("Content-Type"=>"application/json"); body=json(Dict("error"=>String(take!(io)))))
    end
    HTTP.Response(200, Dict("Content-Type"=>"application/text"); body="OK, job $id is finished")
end

function detachedping(request::HTTP.Request)
    HTTP.Response(200, Dict("Content-Type"=>"applicaton/text"); body="OK")
end

function detachedvminfo(request::HTTP.Request)
    @info "in detachedvminfo"
    HTTP.Response(200, Dict("Content-Type"=>"application/json"); body=json(AzManagers.DETACHED_VM[]))
end

function buildstartupscript(user::String, custom_environment::Bool)
    cmd = """
    #!/bin/sh
    sudo mkdir -m 777 /mnt/scratch
    ln -s /mnt/scratch /scratch
    """
    
    if isfile(joinpath(homedir(), ".gitconfig"))
        gitconfig = read(joinpath(homedir(), ".gitconfig"), String)
        cmd *= """
        
        sudo su - $user << EOF
        echo "$gitconfig" > ~/.gitconfig
        EOF
        """
    end
    if isfile(joinpath(homedir(), ".git-credentials"))
        gitcredentials = read(joinpath(homedir(), ".git-credentials"), String)
        cmd *= """
        
        sudo su - $user << EOF
        echo "$gitcredentials" > ~/.git-credentials
        chmod 600 ~/.git-credentials
        EOF
        """
    end

    if custom_environment
        environmentfolder = joinpath(DEPOT_PATH[1], "environments", "v$(VERSION.major).$(VERSION.minor)")
        repo = LibGit2.GitRepo(environmentfolder)
        environmentname = LibGit2.branch(repo)
        cmd *= """
        
        sudo su - $user <<EOF
        cd $(DEPOT_PATH[1])/environments/v$(VERSION.major).$(VERSION.minor)
        git fetch
        git checkout $environmentname
        julia -e 'using Pkg; pkg"instantiate"; pkg"precompile"'
        EOF
        """
    end

    cmd
end

"""
    addproc(template[; basename="cbox", subscriptionid="myid", resourcegroup="mygroup", nretry=10, verbose=0, session=AzSession(;lazy=true), sigimagename="", sigimageversion="", imagename="", detachedservice=true])

Create a VM, and returns a named tuple `(name,ip,resourcegrup,subscriptionid)` where `name` is the name of the VM, and `ip` is the ip address of the VM.
`resourcegroup` and `subscriptionid` denote where the VM resides on Azure.

# Parameters
* `basename="cbox"` base name for the VM, we append a random suffix to ensure uniqueness
* `subscriptionid=AzManagers._manifest["subscriptionid"]` Existing Azure subscription
* `resourcegorup=AzManagers._manifest["resourcegroup"]` Existing Azure resource group inside the subscription in which the VM is put
* `session=AzSession(;lazy=true)` Session used for OAuth2 authentication
* `sigimagename=""` Azure shared image gallery image to use for the VM (defaults to the template's image)
* `sigimageversion=""` Azure shared image gallery image version to use for the VM (defaults to latest)
* `imagename=""` Azure image name used as an alternative to `sigimagename` and `sigimageversion` (used for development work)
* `nretry=10` Max retries for re-tryable REST call failures
* `verbose=0` Verbosity flag passes to HTTP.jl methods
* `detachedservice=true` start the detached service allowing for RESTful remote code execution
"""
function addproc(vm_template::Dict, nic_template=nothing;
        basename="cbox",
        user="",
        subscriptionid = "",
        resourcegroup = "",
        session = AzSession(;lazy=true),
        sigimagename = "",
        sigimageversion = "",
        imagename = "",
        nretry = 10,
        verbose = 0,
        detachedservice = true)
    load_manifest()
    subscriptionid == "" && (subscriptionid = AzManagers._manifest["subscriptionid"])
    resourcegroup == "" && (resourcegroup = AzManagers._manifest["resourcegroup"])
    ssh_key =  AzManagers._manifest["ssh_public_key_file"]
    user = AzManagers._manifest["ssh_user"]
    timeout = Distributed.worker_timeout()

    vmname = basename*"-"*randstring('a':'z', 6)
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

    vm_template["properties"]["osProfile"]["computerName"] = vmname

    subnetid = vm_template["properties"]["networkProfile"]["networkInterfaces"][1]["id"]

    manager = AzManager(session)
    
    @debug "getting image info"
    sigimagename, sigimageversion, imagename = scaleset_image(manager, vm_template, sigimagename, sigimageversion, imagename)

    @debug "software sanity check"
    custom_environment = software_sanity_check(manager, imagename == "" ? sigimagename : imagename)

    @debug "making nic"
    r = @retry nretry azrequest(
        "PUT",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2019-11-01",
        Dict("Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(session))"),
        String(json(nic_template)))

    nic_id = JSON.parse(String(r.body))["id"]

    vm_template["properties"]["networkProfile"]["networkInterfaces"][1]["id"] = nic_id
    key = Dict("path" => "/home/$user/.ssh/authorized_keys", "keyData" => read(ssh_key, String))
    push!(vm_template["properties"]["osProfile"]["linuxConfiguration"]["ssh"]["publicKeys"], key)

    cmd = buildstartupscript(user, custom_environment)

    cmd *= """

    sudo -u $user ssh-keygen -f /home/$user/.ssh/azmanagers_rsa -N ''
    sudo su - $user -c 'cd /home/$user && julia -e "using AzManagers; AzManagers.detachedservice(;subscriptionid=\\\"$subscriptionid\\\", resourcegroup=\\\"$resourcegroup\\\", vmname=\\\"$vmname\\\")"'
    """

    if custom_environment
        cmd *= """

        sudo su - $user -c 'touch /tmp/julia_instantiate_done'
        """
    end

    _cmd = base64encode(cmd)

    vm_template["properties"]["osProfile"]["customData"] = _cmd

    # vm quota check
    @debug "quota check"
    while true
        navailable_cores, navailable_cores_spot = quotacheck(manager, subscriptionid, vm_template, 1, nretry, verbose)
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
        Dict("Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(session))"),
        String(json(vm_template)))

    spincount = 1
    starttime = tic = time()
    elapsed_time = 0.0
    while true
        if time() - tic > 10
            _r = @retry nretry azrequest(
                "GET",
                verbose,
                "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?api-version=2019-07-01",
                Dict("Authorization"=>"Bearer $(token(session))"))
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
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2020-03-01",
        Dict("Authorization"=>"Bearer $(token(session))"))

    r = JSON.parse(String(_r.body))

    vm = Dict("name"=>vmname, "ip"=>string(r["properties"]["ipConfigurations"][1]["properties"]["privateIPAddress"]),
        "subscriptionid"=>string(subscriptionid), "resourcegroup"=>string(resourcegroup))

    if detachedservice
        detached_service_wait(vm, custom_environment)
    elseif custom_environment
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
"""
function rmproc(vm;
        session = AzSession(;lazy=true),
        verbose = 0,
        nretry = 10)
    timeout = Distributed.worker_timeout()

    resourcegroup = vm["resourcegroup"]
    subscriptionid = vm["subscriptionid"]
    vmname = vm["name"]

    manager = AzManager(session)

    r = @retry nretry azrequest(
        "DELETE",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?api-version=2019-07-01",
        Dict("Authorization"=>"Bearer $(token(session))"))

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
                "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines?api-version=2019-07-01",
                Dict("Authorization" => "Bearer $(token(session))"))

            r = JSON.parse(String(_r.body))
            vms = getnextlinks!(manager, get(r, "value", []), get(r, "nextLink", ""), nretry, verbose)

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

    nicname = vmname*"-nic"

    @retry nretry azrequest(
        "DELETE",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2020-03-01",
        Dict("Authorization"=>"Bearer $(token(session))"))
    nothing
end

# helper methods
function software_sanity_check(manager, imagename)
    envpath = joinpath(DEPOT_PATH[1], "environments", "v$(VERSION.major).$(VERSION.minor)")
    local repo
    try
        repo = LibGit2.GitRepo(envpath)
    catch
        @warn "Julia environment is not versioned"
        return false
    end
    branchname = LibGit2.branch(repo)

    @debug "sofware sanity, imagename=$imagename, branchname=$branchname"

    custom_environment = false
    if imagename != branchname
        # assume that the user has modified and comitted an environment to a branch, and wants that environment initialized on the workers
        custom_environment = true
    end

    _tempname = tempname()
    open(_tempname, "w") do io
        redirect_stdout(io) do
            Pkg.status(;diff=true)
        end
    end
    statuslines = readlines(_tempname)
    isdev = isup = isdown = isadd = isrm = false
    for statusline in statuslines[2:end]
        tokens = split(strip(statusline))
        if length(tokens) > 1
            isdev = occursin("~", tokens[2])
            isup = occursin("↑", tokens[2])
            isdown = occursin("↓", tokens[2])
            isadd = occursin("+", tokens[2])
            isrm = occursin("-", tokens[2])
            (isdev || isup || isdown || isadd || isrm) && break
        end
    end

    if isdev || isup || isdown || isadd || isrm
        @warn "Julia environment has modifications.  It will be inconsistent with the Julia environment on the created VM(s)."
    end
    custom_environment
end

function scaleset_image(manager::AzManager, template, sigimagename, sigimageversion, imagename)
    if sigimagename == "" && sigimageversion == "" && imagename == ""
        # get sigimageversion and sigimagename from the machines' metadata
        r = nothing
        t = @async begin
            r = HTTP.request("GET", "http://169.254.169.254/metadata/instance/compute/storageProfile/imageReference?api-version=2019-06-01", Dict("Metadata"=>"true"); retry=false, redirect=false)
        end
        tic = time()
        while !istaskdone(t)
            (time() - tic) > 5 && break
            sleep(1)
        end

        istaskdone(t) || @async Base.throwto(t, InterruptException)
        if isa(r, HTTP.Messages.Response)
            image = JSON.parse(String(r.body))["id"]
            _image = split(image,"/")
            k = findfirst(x->x=="galleries", _image)
            if k != nothing
                k = findfirst(x->x=="images", _image)
                sigimagename = _image[k+1]
                k = findfirst(x->x=="versions", _image)
                if k != nothing
                    sigimageversion = _image[k+1]
                end
            else
                k = findfirst(x->x=="images", _image)
                imagename = _image[k+1]
            end
        end
    end

    @debug "after inspecting the VM  metaddata, imagename=$imagename, sigimagename=$sigimagename, sigimageversion=$sigimageversion"

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
                id = template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"]
                template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"] = join(split(id, '/')[1:end-1], '/')*"/"*sigimagename
            else # vm
                id = template["properties"]["storageProfile"]["imageReference"]["id"]
                template["properties"]["storageProfile"]["imageReference"]["id"] = join(split(id, '/')[1:end-1], '/')*"/"*sigimagename
            end
        end

        if sigimageversion != ""
            if haskey(template["properties"], "virtualMachineProfile") # scale-set
                template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"] *= "/versions/$sigimageversion"
            else # vm
                template["properties"]["storageProfile"]["imageReference"]["id"] *= "/versions/$sigimageversion"
            end
        end
    end

    if haskey(template["properties"], "virtualMachineProfile") # scale-set
        @debug "using image=$(template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"])"
    else # vm
        @debug "using image=$(template["properties"]["storageProfile"]["imageReference"]["id"])"
    end

    sigimagename, sigimageversion, imagename
end

function scaleset_create_or_update(manager::AzManager, user, subscriptionid, resourcegroup, scalesetname, sigimagename, sigimageversion, imagename, nretry, template, δn, spot, maxprice, verbose, custom_environment)
    load_manifest()
    ssh_key = _manifest["ssh_public_key_file"]

    @debug "scaleset_create_or_update"
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2019-12-01",
        Dict("Authorization"=>"Bearer $(token(manager.session))"))
    r = JSON.parse(String(_r.body))

    _template = deepcopy(template)

    _template["properties"]["virtualMachineProfile"]["osProfile"]["computerNamePrefix"] = string(scalesetname, "-", randstring('a':'z', 4), "-")

    key = Dict("path" => "/home/$user/.ssh/authorized_keys", "keyData" => read(ssh_key, String))
    push!(_template["properties"]["virtualMachineProfile"]["osProfile"]["linuxConfiguration"]["ssh"]["publicKeys"], key)
    
    cmd = buildstartupscript(user, custom_environment)
    _cmd = base64encode(cmd)

    _template["properties"]["virtualMachineProfile"]["osProfile"]["customData"] = _cmd

    if spot
        _template["properties"]["virtualMachineProfile"]["priority"] = "Spot"
        _template["properties"]["virtualMachineProfile"]["evictionPolicy"] = "Delete"
        _template["properties"]["virtualMachineProfile"]["billingProfile"] = Dict("maxPrice"=>maxprice)
    end

    n = 0
    scalesets = get(r, "value", [])
    scaleset_exists = false
    for scaleset in scalesets
        if scaleset["name"] == scalesetname
            n = length(scaleset_listvms(manager, _template, subscriptionid, resourcegroup, scalesetname, nretry, verbose)) #scaleset["sku"]["capacity"]
            _template["properties"]["virtualMachineProfile"]["osProfile"]["computerNamePrefix"] = scaleset["properties"]["virtualMachineProfile"]["osProfile"]["computerNamePrefix"]
            scaleset_exists = true
            break
        end
    end
    n += δn

    if !scaleset_exists
        _template["sku"]["capacity"] = 0
        write(stdout, "template: $(_template)\n")
        json_template = JSON.json(_template)
        write(stdout, "JSON template: $(json_template)\n")

        @retry nretry azrequest(
            "PUT",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname?api-version=2020-06-01",
            Dict("Content-type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"),
            json(_template,1))
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

    @debug "done checking quota, δn=$(δn), n=$n"

    _template["sku"]["capacity"] = n
    @retry nretry azrequest(
        "PUT",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname?api-version=2019-12-01",
        Dict("Content-type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"),
        String(json(_template)))

    n
end

function quotacheck(manager, subscriptionid, template, δn, nretry, verbose)
    location = template["location"]

    # get a mapping from vm-size to vm-family
    f = HTTP.escapeuri("location eq '$location'")

    # resources in southcentralus
    target = "https://management.azure.com/subscriptions/$subscriptionid/providers/Microsoft.Compute/skus?api-version=2019-04-01&\$filter=$f"
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        target,
        Dict("Authorization"=>"Bearer $(token(manager.session))"))

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
        Dict("Authorization"=>"Bearer $(token(manager.session))"))
    r = JSON.parse(String(_r.body))

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

function getnextlinks!(manager::AzManager, value, nextlink, nretry, verbose)
    while nextlink != ""
        _r = @retry nretry azrequest(
            "GET",
            verbose,
            nextlink,
            Dict("Authorization"=>"Bearer $(token(manager.session))"))
        r = JSON.parse(String(_r.body))
        value = [value;get(r,"value",[])]
        nextlink = get(r, "nextLink", "")
    end
    value
end

function ispublic(template)
write(stdout, "INSIDE IS PUBLIC: $(template)\n\n")
    try
        haskey(template["properties"]["virtualMachineProfile"]["networkProfile"]["networkInterfaceConfigurations"][1]["properties"]["ipConfigurations"][1]["properties"], "publicIPAddressConfiguration")
    catch e
        @error "Failed to check for public IP"
        throw(e)
    end
end

function scaleset_listvms(manager::AzManager, template, subscriptionid, resourcegroup, scalesetname, nretry, verbose)
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2018-06-01",
        Dict("Authorization"=>"Bearer $(token(manager.session))"))
    r = JSON.parse(String(_r.body))

    scalesets = get(r, "value", [])
    scalesetnames = [get(scaleset, "name", "") for scaleset in scalesets]
    scalesetname ∉ scalesetnames && return String[]

    @debug "getting network interfaces from scaleset"
    if ispublic(template)
        write(stdout, "The template was eval'd as public\n\n")
        _r = @retry nretry azrequest(
            "GET",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/publicipaddresses?api-version=2018-10-01",
            Dict("Authorization"=>"Bearer $(token(manager.session))"))
    else
        write(stdout, "The template was eval'd as private")
        _r = @retry nretry azrequest(
            "GET",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/microsoft.Compute/virtualMachineScaleSets/$scalesetname/networkInterfaces?api-version=2017-03-30",
            Dict("Authorization"=>"Bearer $(token(manager.session))"))
    end
    r = JSON.parse(String(_r.body))
    networkinterfaces = getnextlinks!(manager, get(r, "value", []), get(r, "nextLink", ""), nretry, verbose)
    write(stdout, "Network Interfaces: $(networkinterfaces)\n\n")
    @debug "done getting network interfaces from scaleset"

    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/virtualMachines?api-version=2018-06-01",
        Dict("Authorization"=>"Bearer $(token(manager.session))"))
    r = JSON.parse(String(_r.body))
    _vms = getnextlinks!(manager, get(r, "value", []), get(r, "nextLink", ""), nretry, verbose)
    @debug "done getting vms"

    networkinterfaces_vmids = [get(get(get(networkinterface, "properties", Dict()), "virtualMachine", Dict()), "id", "") for networkinterface in networkinterfaces]
    vms = Dict{String,String}[]

    for vm in _vms
        if vm["properties"]["provisioningState"] ∈ ("Succeeded", "Updating")
            i = findfirst(id->id == vm["id"], networkinterfaces_vmids)
            if i != nothing
                bind_address = (ispublic(template)) ? networkinterfaces["value"][i]["properties"]["ipAddress"] : networkinterfaces[i]["properties"]["ipConfigurations"][1]["properties"]["privateIPAddress"]
                write(stdout, "BIND_ADDRESS: $(bind_address)\n\n")
                push!(vms, Dict("name"=>vm["name"], "host"=>vm["properties"]["osProfile"]["computerName"], "bindaddr"=>bind_address, "instanceid"=>vm["instanceId"]))
            end
        end
    end
    @debug "done collating vms and nics"
    vms
end

function scaleset_count_stable_instances(manager::AzManager, subscriptionid, resourcegroup, scalesetname, nretry, verbose)
    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/virtualMachines?api-version=2018-06-01",
        Dict("Authorization"=>"Bearer $(token(manager.session))"))
    r = JSON.parse(String(_r.body))
    vms = getnextlinks!(manager, get(r, "value", []), get(r, "nextLink", ""), nretry, verbose)

    length(vms) == 0 ? 0 : mapreduce(vm->Int(get(get(vm, "properties", Dict()), "provisioningState", "")∈("Succeeded", "Updating")), +, vms)
end

function scaleset_wait_until_stable(manager::AzManager, subscriptionid, resourcegroup, scalesetname, nretry, targetsize, verbose)
    spincount = 1
    starttime = tic = time()
    isup = false
    isscalesetup = false
    ws = repeat(" ", 5)
    n = scaleset_count_stable_instances(manager, subscriptionid, resourcegroup, scalesetname, nretry, verbose)
    elapsed_time = 0.0
    while true
        if time() - tic > 5
            _r = @retry nretry azrequest(
                "GET",
                verbose,
                "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2018-06-01",
                Dict("Authorization"=>"Bearer $(token(manager.session))"))
            r = JSON.parse(String(_r.body))
            for scaleset in get(r, "value", Dict())
                if get(scaleset, "name", "") == scalesetname
                    properties = get(scaleset, "properties", Dict())
                    if get(properties, "provisioningState", "") == "Succeeded"
                        isscalesetup = true
                    end
                    n = scaleset_count_stable_instances(manager, subscriptionid, resourcegroup, scalesetname, nretry, verbose)

                    isup = n == targetsize && isscalesetup
                    isup && break
                end
            end
            tic = time()
        end
        elapsed_time = time() - starttime
        write(stdout, spin(spincount, elapsed_time)*", $n/$targetsize vm's running."*ws*"\r")
        flush(stdout)
        isup && break
        spincount = spincount == 4 ? 1 : spincount + 1
        sleep(.25)
    end
    write(stdout, spin(5, elapsed_time)*", $n/$targetsize vm's running."*ws*"\r")
    write(stdout, "\n")
    nothing
end

function scaleset_wait_for_cloud_init_vm(manager::AzManager, vm, subscriptionid, resourcegroup, scalesetname, nretry, verbose)
    r = @retry nretry HTTP.request(
        "POST",
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/virtualmachines/$(vm["instanceid"])/runCommand?api-version=2019-03-01",
        Dict("Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(manager.session))"),
        """{ "commandId": "RunShellScript", "script": ["cloud-init status --wait"] }""";
        verbose=verbose)

    r.status == 200 && (return nothing)
    r.status == 202 || (@warn "Unable to verify cloud-init success for $(vm["instanceid"])."; return nothing)

    i = findfirst(header->header[1]=="Azure-AsyncOperation", r.headers)
    i == nothing && (@warn "Unable to verify cloud-init success for $(vm["instanceid"])."; return nothing)
    azure_async_op = r.headers[i][2]

    timeout = Distributed.worker_timeout()
    tic = time()
    while true
        _r = @retry nretry HTTP.request(
            "GET",
            azure_async_op,
            Dict("Authorization"=>"Bearer $(token(manager.session))");
            verbose=verbose)

        r = JSON.parse(String(_r.body))
        get(r, "status", "") == "Succeeded" && break
        time() - tic > timeout && (@warn "Unable to verity cloud-init success for $(vm["instanceid"])."; break)
        sleep(10)
    end
    nothing
end

function scaleset_wait_for_cloud_init(manager::AzManager, vms, subscriptionid, resourcegroup, scalesetname, nretry, verbose)
    tsks = Task[]
    for vm in vms
        push!(tsks, @async scaleset_wait_for_cloud_init_vm(manager, vm, subscriptionid, resourcegroup, scalesetname, nretry, verbose))
    end
    spinner_task = @async spinner(tsks, "completed cloud-init stage")
    wait.(tsks)
    wait(spinner_task)
end

export AzManager, DetachedJob, addproc, preempted, rmproc, status, variablebundle, variablebundle!, vm, @detach, @detachat

end
