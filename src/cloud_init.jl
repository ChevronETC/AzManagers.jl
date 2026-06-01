function shell_quote(value)
    "'" * replace(string(value), "'" => "'\"'\"'") * "'"
end

function valid_environment_name(name)
    occursin(r"^[A-Za-z_][A-Za-z0-9_]*$", string(name))
end

function build_envstring(env::Dict)
    envstring = ""
    for (key,value) in env
        valid_environment_name(key) ||
            throw(ArgumentError("invalid environment variable name: $key"))
        envstring *= "export $key=$(shell_quote(value))\n"
    end
    envstring
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

function nested_mpi_launch_block(
        exename::AbstractString,
        exeflags::AbstractString,
        ppi::Int,
        mpi_ranks_per_worker::Int,
        mpi_flags,
        cookie::AbstractString,
        master_address::AbstractString,
        master_port::Int,
        juliaenvstring::AbstractString)

    rank_payload = "$(juliaenvstring)using AzManagers, MPI; AzManagers.azure_worker_mpi(\"$cookie\", \"$master_address\", $master_port, $ppi, \"$exeflags\")"

    """
    AZM_WORKER_CPU_SETS=( \$($exename -e 'using AzManagers; topology = AzManagers.detect_machine_topology(); for placement in AzManagers.plan_worker_placements(topology, $ppi); print(AzManagers.cpu_set_string(placement.cpu_set), " "); end') )

    if [ \${#AZM_WORKER_CPU_SETS[@]} -ne $ppi ]; then
        echo "expected $ppi worker cpu-sets but got \${#AZM_WORKER_CPU_SETS[@]}" >&2
        exit 1
    fi

    AZM_PIDS=()
    for cpu_set in "\${AZM_WORKER_CPU_SETS[@]}"; do
        mpirun -n $mpi_ranks_per_worker --cpu-set "\$cpu_set" --bind-to cpu-list:ordered $mpi_flags $exename $exeflags -e '$rank_payload' &
        AZM_PIDS+=(\$!)
    done

    exit_code=0
    for pid in "\${AZM_PIDS[@]}"; do
        if ! wait \$pid; then
            child_status=\$?
            exit_code=\$child_status
        fi
    done
    """
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
        elseif ppi == 1
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
        else
            nested_block = nested_mpi_launch_block(
                exename, _exeflags, ppi, mpi_ranks_per_worker, mpi_flags,
                cookie, master_address, master_port, juliaenvstring)
            shell_cmds *= """

            $exename -e '$(juliaenvstring)try using AzManagers; catch; using Pkg; Pkg.instantiate(); using AzManagers; end; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks(); AzManagers.build_lvm()'

            attempt_number=1
            maximum_attempts=5
            exit_code=0
            while [  \$attempt_number -le \$maximum_attempts ]; do
                $nested_block
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
        elseif ppi == 1
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
        else
            nested_block = nested_mpi_launch_block(
                exename, _exeflags, ppi, mpi_ranks_per_worker, mpi_flags,
                cookie, master_address, master_port, juliaenvstring)
            shell_cmds *= """

            $exename -e '$(juliaenvstring)try using AzManagers; catch; using Pkg; Pkg.instantiate(); using AzManagers; end; AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks()'

            attempt_number=1
            maximum_attempts=5
            exit_code=0
            while [  \$attempt_number -le \$maximum_attempts ]; do
                $nested_block
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
