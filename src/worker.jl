#=
Use libCURL because HTTP forces the request to run, partially, on a thread in the default thread-pool
where-as, we would like to run requests to the scaleset metadata server on the interactive thrad-pool.
=#
mutable struct CurlDataStruct
    body::Vector{UInt8}
    currentsize::Csize_t
end

function curl_get_write_callback(curlbuf::Ptr{Cchar}, size::Csize_t, nmemb::Csize_t, datavoid::Ptr{Cvoid})
    datastruct = unsafe_pointer_to_objref(datavoid)::CurlDataStruct

    n = size*nmemb
    newsize = datastruct.currentsize + n
    resize!(datastruct.body, newsize)

    _data = pointer(datastruct.body, datastruct.currentsize+1)
    @ccall memcpy(_data::Ptr{Cvoid}, curlbuf::Ptr{Cvoid}, n::Csize_t)::Ptr{Cvoid}
    datastruct.currentsize = newsize
    return n
end

function curl_get_metadata(url)
    datastruct = CurlDataStruct(UInt8[], 0)

    headers = C_NULL
    headers = curl_slist_append(headers, "Metadata: true")

    curl = curl_easy_init()
    curl_easy_setopt(curl, CURLOPT_URL, url)
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers)
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, @cfunction(curl_get_write_callback, Csize_t, (Ptr{Cchar}, Csize_t, Csize_t, Ptr{Cvoid})))
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, pointer_from_objref(datastruct))

    curl_easy_perform(curl)

    http_code = Array{Clong}(undef, 1)
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, http_code)
    if http_code[1] > 200
        error("Azure metaadata service return $(http_code[1]) response.")
    end

    curl_easy_cleanup(curl)

    datastruct
end

function get_instanceid()
    local r
    try
        # _r = HTTP.request("GET", "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01", ["Metadata"=>"true"])
        _r = curl_get_metadata("http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01")
        r = JSON.parse(String(_r.body))
    catch
        r = Dict()
    end
    get(r, "name", "")
end

"""
    ispreempted,notbefore = preempted([id=myid()|id="instanceid"])

Check to see if the machine `id::Int` has received an Azure spot preempt message.  Returns
(true, notbefore) if a preempt message is received and (false,"") otherwise.  `notbefore`
is the date/time before which the machine is guaranteed to still exist.
"""
function preempted(instanceid::AbstractString, clusterid::Int)
    isempty(instanceid) && (instanceid = get_instanceid())
    clusterid == 0 && (clusterid = myid())
    local _r
    try
        tic = time()
        # _r = HTTP.request("GET", "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01", ["Metadata"=>"true"])
        _r = curl_get_metadata("http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01")
        if time() - tic > 55 # 55 seconds, simply because it is less that 60, and 60 seconds is the eviction notice.
            @debug "$(now()), took longer than 55 seconds to query the meta-data server for scheduled events (elapsed time=$(time() - tic))."
        end
    catch e
        @warn "unable to get scheduledevents" exception=(e, catch_backtrace())
        return false, ""
    end
    r = JSON.parse(String(_r.body))
    events = get(r, "Events", [])
    if !isempty(events)
        @info "IMDS scheduledevents on pid=$clusterid: $(length(events)) event(s)" events
    end
    for event in events
        if get(event, "EventType", "") == "Preempt" && instanceid ∈ get(event, "Resources", [])
            @warn "Machine with id $clusterid ($instanceid) is being pre-empted" now(Dates.UTC) event["NotBefore"] event["EventType"] event["EventSource"]
            return true, event["NotBefore"]
        end
    end
    return false, ""
end

macro spawn_interactive(ex::Expr)
    if VERSION >= v"1.9"
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
    ninteractive = VERSION >= v"1.9" ? Threads.nthreads(:interactive) : 0
    @info "machine_preempt_loop: pid=$(myid()), VERSION=$VERSION, interactive_threads=$ninteractive"
    if VERSION >= v"1.9" && ninteractive > 0
        tsk = @spawn_interactive begin
            preempt_channel = fetch(preempt_channel_future)::Channel{Bool}
            instanceid = get_instanceid()
            clusterid = myid()
            @info "preempt loop started on pid=$clusterid, instanceid=$instanceid"

            poll_count = 0
            while true
                poll_count += 1
                ispreempted, notbefore = preempted(instanceid, clusterid)
                if ispreempted
                    @info "preempt detected on pid=$clusterid after $poll_count polls"
                    put!(preempt_channel, true)
                    throw(SpotPreemptException(instanceid, clusterid, notbefore))
                end
                if poll_count % 30 == 0
                    @info "preempt loop heartbeat: pid=$clusterid, polls=$poll_count, no preempt detected"
                end
                sleep(1)
            end
        end
        fetch(tsk)
    else
        @warn "preempt loop NOT running on pid=$(myid()): requires Julia ≥ 1.9 with interactive threads" VERSION ninteractive
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

    # Wait for the preempt monitor to register the future
    if haskey(manager.preempt_channel_ready, pid)
        # Use a Timer-based watchdog for timeout
        tsk = @async begin
            wait(manager.preempt_channel_ready[pid])
        end
        watchdog = Timer(Float64(timeout)) do _
            if !istaskdone(tsk)
                @async Base.throwto(tsk, InterruptException())
            end
        end
        try
            fetch(tsk)
        catch
            @warn "unable to obtain preemption channel from worker $pid in $timeout seconds"
            return nothing
        finally
            close(watchdog)
        end
    end

    if haskey(manager.preempt_channel_futures, pid)
        return manager.preempt_channel_futures[pid]
    end
    @warn "unable to obtain preemption channel from worker $pid"
    return nothing
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

    local r
    for attempt in 1:3
        try
            _r = HTTP.request("GET", "http://169.254.169.254/metadata/instance?api-version=2021-02-01", ["Metadata"=>"true"]; redirect=false, readtimeout=30)
            r = JSON.parse(String(_r.body))
            break
        catch e
            @warn "IMDS metadata request failed (attempt $attempt/3)" exception=(e, catch_backtrace())
            attempt == 3 && rethrow()
            sleep(5 * attempt)
        end
    end
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

    try
        Sockets.nagle(sock, false)
        Sockets.quickack(sock, true)
    catch e
        @debug "failed to set socket options" exception=(e, catch_backtrace())
    end

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

function buildstartupscript(manager::AzManager, exename::String, user::String, disk::AbstractString, custom_environment::Bool, use_lvm::Bool)

    if use_lvm
        cmd = """
        #!/bin/sh
        sed -i 's/ scripts-user/ [scripts-user, always]/g' /etc/cloud/cloud.cfg
        """
    else
        cmd = """
        #!/bin/bash
        $disk
        sed -i 's/ scripts-user/ [scripts-user, always]/g' /etc/cloud/cloud.cfg
        """
    end

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
            
            sudo su - $user << 'EOF'
            $exename -e 'using AzManagers; AzManagers.decompress_environment("$project_compressed", "$manifest_compressed", "$localpreferences_compressed", "$remote_julia_environment_name")'
            $exename -e 'using Pkg; path=joinpath(Pkg.envdir(), "$remote_julia_environment_name"); Pkg.Registry.update(); Pkg.activate(path); (retry(Pkg.instantiate))(); Pkg.precompile()'
            EOF
            """
        catch e
            @warn "Unable to use a custom environment."
            logerror(e, Logging.Debug)
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

function buildstartupscript_cluster(manager::AzManager, spot::Bool, ppi::Int, mpi_ranks_per_worker::Int, mpi_flags, nvidia_enable_ecc, nvidia_enable_mig, julia_num_threads::String, omp_num_threads::Int, exename::String, exeflags::String, env::Dict, user::String,
        disk::AbstractString, custom_environment::Bool, use_lvm::Bool)

    shell_cmds, remote_julia_environment_name = buildstartupscript(manager, exename, user, disk, custom_environment, use_lvm)

    cookie = Distributed.cluster_cookie()
    master_address = string(getipaddr())
    master_port = manager.port

    envstring = build_envstring(env)

    juliaenvstring = remote_julia_environment_name == "" ? "" : """using Pkg; Pkg.activate(joinpath(Pkg.envdir(), "$remote_julia_environment_name")); """

    # if spot is true, then ensure at least one interactive thread on workers so that one can check for spot evictions periodically.
    if spot && VERSION >= v"1.9"
        _julia_num_threads = split(julia_num_threads, ',')
        julia_num_threads_default = length(_julia_num_threads) > 0 ? parse(Int, _julia_num_threads[1]) : 1
        julia_num_threads_interactive = length(_julia_num_threads) > 1 ? parse(Int, _julia_num_threads[2]) : 0

        if julia_num_threads_interactive == 0
            @debug "Augmenting 'julia_num_threads' option with an interactive thread so it can be used on workers for spot-event polling."
            julia_num_threads_interactive = 1
        end
        julia_num_threads = nthreads_filter("$julia_num_threads_default,$julia_num_threads_interactive")
    end

    _exeflags = isempty(exeflags) ? "-t $julia_num_threads" : "$exeflags -t $julia_num_threads"

    shell_cmds *= """

    sudo su - $user << 'EOF'
    export JULIA_WORKER_TIMEOUT=$(get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
    export OMP_NUM_THREADS=$omp_num_threads
    $envstring
    """

    if use_lvm
        if mpi_ranks_per_worker == 0 
            shell_cmds *= """

            attempt_number=1
            maximum_attempts=5
            exit_code=0
            while [  \$attempt_number -le \$maximum_attempts ]; do
                $exename $_exeflags -e '$(juliaenvstring)try using AzManagers; catch; using Pkg; Pkg.instantiate(); using AzManagers; end; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks(); AzManagers.build_lvm(); AzManagers.azure_worker("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'
                
                exit_code=\$?
                echo "attempt \$attempt_number is done with exit code \$exit_code..."

                if [ "\$exit_code" == "42" ]; then
                    echo "...breaking from retry loop due to exit code 42."
                    break
                fi

                echo "...trying again after sleeping for 5 seconds..."
                sleep 5
                attempt_number=\$(( attempt_number + 1 ))

                echo "the worker startup was tried \$attempt_number times."
            done
            echo "the worker has finished running with exit code \$exit_code."
            EOF
            """
        else
            shell_cmds *= """

            $exename -e '$(juliaenvstring)try using AzManagers; catch; using Pkg; Pkg.instantiate(); using AzManagers; end; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks(); AzManagers.build_lvm()'

            attempt_number=1
            maximum_attempts=5
            exit_code=0
            while [  \$attempt_number -le \$maximum_attempts ]; do
                mpirun -n $mpi_ranks_per_worker $mpi_flags $exename $_exeflags -e '$(juliaenvstring)using AzManagers, MPI; AzManagers.azure_worker_mpi("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'

                exit_code=\$?
                echo "attempt \$attempt_number is done with exit code \$exit_code...."

                if [ "\$exit_code" == "42" ]; then
                    echo "...breaking from retry loop due to exit code 42."
                    break
                fi

                echo "...trying again after sleeping for 5 seconds..."
                sleep 5
                attempt_number=\$(( attempt_number + 1 ))

                echo "the worker startup was tried \$attempt_number times."
            done
            echo "the worker has finished running with exit code \$exit_code."
            EOF
            """
        end

        cloud_cfg = cloudcfg_nvme_scratch()
        boundary = "===Boundary==="
        cmd = """
        MIME-Version: 1.0
        Content-Type: multipart/mixed; boundary="$boundary"

        --$boundary
        Content-Type: text/cloud-config; charset="us-ascii"

        $cloud_cfg

        --$boundary
        Content-Type: text/x-shellscript; charset="us-ascii"

        $shell_cmds

        --$boundary--
        """
    else
        if mpi_ranks_per_worker == 0 
            shell_cmds *= """

            attempt_number=1
            maximum_attempts=5
            exit_code=0
            while [  \$attempt_number -le \$maximum_attempts ]; do
                $exename $_exeflags -e '$(juliaenvstring)try using AzManagers; catch; using Pkg; Pkg.instantiate(); using AzManagers; end; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks(); AzManagers.azure_worker("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'
                
                exit_code=\$?
                echo "attempt \$attempt_number is done with exit code \$exit_code..."

                if [ "\$exit_code" == "42" ]; then
                    echo "...breaking from retry loop due to exit code 42."
                    break
                fi

                echo "...trying again after sleeping for 5 seconds..."
                sleep 5
                attempt_number=\$(( attempt_number + 1 ))

                echo "the worker startup was tried \$attempt_number times."
            done
            echo "the worker has finished running with exit code \$exit_code."
            EOF
            """
        else
            shell_cmds *= """

            $exename -e '$(juliaenvstring)try using AzManagers; catch; using Pkg; Pkg.instantiate(); using AzManagers; end; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks()'

            attempt_number=1
            maximum_attempts=5
            exit_code=0
            while [  \$attempt_number -le \$maximum_attempts ]; do
                mpirun -n $mpi_ranks_per_worker $mpi_flags $exename $_exeflags -e '$(juliaenvstring)using AzManagers, MPI; AzManagers.azure_worker_mpi("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'

                exit_code=\$?
                echo "attempt \$attempt_number is done with exit code \$exit_code...."

                if [ "\$exit_code" == "42" ]; then
                    echo "...breaking from retry loop due to exit code 42."
                    break
                fi

                echo "...trying again after sleeping for 5 seconds..."
                sleep 5
                attempt_number=\$(( attempt_number + 1 ))

                echo "the worker startup was tried \$attempt_number times."
            done
            echo "the worker has finished running with exit code \$exit_code."
            EOF
            """
        end

        cmd = shell_cmds
    end

    cmd
end

function buildstartupscript_detached(manager::AzManager, exename::String, julia_num_threads::String, omp_num_threads::Int, env::Dict, user::String,
        disk::AbstractString, custom_environment::Bool, subscriptionid, resourcegroup, vmname, use_lvm::Bool)

    shell_cmds, remote_julia_environment_name = buildstartupscript(manager, exename, user, disk, custom_environment, use_lvm)

    envstring = build_envstring(env)

    juliaenvstring = remote_julia_environment_name == "" ? "" : """Pkg.activate(joinpath(Pkg.envdir(), "$remote_julia_environment_name")); """

    #=
    if exename is something like `mpirun -n 1 julia`, then we need to remove the `mpirun -n 1` part
    to get the actual julia executable name.  The reason for this is that detached jobs
    run on detached machines in their own process started with `exename`.  If `exename` includes
    mpirun or mpiexec, this wouuld result in a recursive mpi call error due to the
    detached service also being started with mpirun or mpiexec.
    =#
    exename_parts = split(exename, ' ')
    i = findfirst(part->contains(part, "julia"), exename_parts)
    
    if i === nothing
        error("unable to find 'julia' in exename='$exename'")
    end
    exename_nompi = join(exename_parts[i:end], ' ')

    if use_lvm
        shell_cmds *= """

        sudo su - $user << EOF
        $envstring
        export JULIA_WORKER_TIMEOUT=$(get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
        export OMP_NUM_THREADS=$omp_num_threads
        ssh-keygen -f /home/$user/.ssh/azmanagers_rsa -N '' <<<y
        cd /home/$user
        $exename_nompi -t $julia_num_threads -e 'using Pkg; $(juliaenvstring)try using AzManagers; catch; Pkg.instantiate(); using AzManagers; end; AzManagers.mount_datadisks(); AzManagers.build_lvm(); AzManagers.detached_port!($(AzManagers.detached_port())); if Pkg.dependencies()[Base.UUID("db05ebb0-6096-11e9-199b-87b703361841")].version >= v"3.17"; AzManagers.detachedservice(;subscriptionid="$subscriptionid", resourcegroup="$resourcegroup", vmname="$vmname", exename="$exename"); else @warn "exename not supported in Azmanagers < 3.17"; AzManagers.detachedservice(;subscriptionid="$subscriptionid", resourcegroup="$resourcegroup", vmname="$vmname"); end'
        EOF
        """

        cloud_cfg = cloudcfg_nvme_scratch()
        boundary = "===Boundary==="
        cmd = """
        MIME-Version: 1.0
        Content-Type: multipart/mixed; boundary="$boundary"

        --$boundary
        Content-Type: text/cloud-config; charset="us-ascii"

        $cloud_cfg

        --$boundary
        Content-Type: text/x-shellscript; charset="us-ascii"

        $shell_cmds

        --$boundary--
        """

    else
        shell_cmds *= """

        sudo su - $user << EOF
        $envstring
        export JULIA_WORKER_TIMEOUT=$(get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
        export OMP_NUM_THREADS=$omp_num_threads
        ssh-keygen -f /home/$user/.ssh/azmanagers_rsa -N '' <<<y
        cd /home/$user
        $exename_nompi -t $julia_num_threads -e 'using Pkg; $(juliaenvstring)try using AzManagers; catch; Pkg.instantiate(); using AzManagers; end; AzManagers.mount_datadisks(); AzManagers.detached_port!($(AzManagers.detached_port())); if Pkg.dependencies()[Base.UUID("db05ebb0-6096-11e9-199b-87b703361841")].version >= v"3.17"; AzManagers.detachedservice(;subscriptionid="$subscriptionid", resourcegroup="$resourcegroup", vmname="$vmname", exename="$exename"); else @warn "exename not supported in Azmanagers < 3.17"; AzManagers.detachedservice(;subscriptionid="$subscriptionid", resourcegroup="$resourcegroup", vmname="$vmname"); end'
        EOF
        """
        cmd = shell_cmds
    end

    cmd
end

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

    @info "simulate_spot_eviction: pid=$pid, instanceid=$instanceid, scaleset=$scalesetname"

    manager = azmanager()
    session = manager.session

    r = HTTP.request(
        "POST",
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname/virtualMachines/$instanceid/simulateEviction?api-version=2023-03-01",
        ["Authorization" => "Bearer $(token(session))"];
        status_exception=false)
    @info "simulate_spot_eviction response" status=r.status body=String(r.body)
    if r.status >= 300
        @warn "simulate_spot_eviction failed" status=r.status body=String(r.body)
    end
    nothing
end
