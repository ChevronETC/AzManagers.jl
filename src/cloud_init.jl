# ─── Environment shipping ───

function check_environment(customenv)
    customenv || return
    projectinfo = Pkg.project()
    envpath = normpath(joinpath(projectinfo.path, ".."))
    manifest = TOML.parse(read(joinpath(envpath, "Manifest.toml"), String))
    packages = haskey(manifest, "deps") ? manifest["deps"] : manifest
    for (name, info) in packages
        haskey(info[1], "path") && error("Project has dev'd package '$name' that won't be accessible from workers.")
    end
end

function compress_environment(folder)
    compress = f -> base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(read(f, String))))
    project = compress(joinpath(folder, "Project.toml"))
    manifest = compress(joinpath(folder, "Manifest.toml"))
    lp_path = joinpath(folder, "LocalPreferences.toml")
    lp = isfile(lp_path) ? compress(lp_path) : base64encode(CodecZlib.transcode(ZlibCompressor, UInt8[]))
    (project, manifest, lp)
end

function nthreads_filter(nthreads)
    parts = split(string(nthreads), ',')
    n_default = length(parts) > 0 ? parse(Int, parts[1]) : 1
    n_interactive = length(parts) > 1 ? parse(Int, parts[2]) : 0
    n_interactive > 0 ? "$n_default,$n_interactive" : string(n_default)
end

function build_envstring(env::Dict)
    join(["export $k=$v" for (k, v) in env], "\n")
end

# ─── Cloud-init script builder ───

function build_cloud_init(state::ManagerState, user, template;
        spot, ppi, mpi_ranks_per_worker, mpi_flags,
        nvidia_enable_ecc, nvidia_enable_mig,
        julia_num_threads, omp_num_threads,
        exename, exeflags, env, customenv, use_lvm)

    parts = String[]

    # Shebang + cloud-cfg fix
    disk = get(template, "tempdisk", "")
    push!(parts, use_lvm ? "#!/bin/sh" : "#!/bin/bash\n$disk")
    push!(parts, "sed -i 's/ scripts-user/ [scripts-user, always]/g' /etc/cloud/cloud.cfg")

    # Git config forwarding
    _append_git_config!(parts, user)

    # Custom environment
    remote_env_name = _append_custom_env!(parts, user, exename, customenv)

    # Thread config (spot needs interactive thread for eviction polling)
    julia_num_threads = _ensure_spot_threads(julia_num_threads, spot)
    _exeflags = isempty(exeflags) ? "-t $julia_num_threads" : "$exeflags -t $julia_num_threads"

    # Worker command
    juliaenvstring = remote_env_name == "" ? "" : """using Pkg; Pkg.activate(joinpath(Pkg.envdir(), "$remote_env_name")); """
    worker_cmd = _build_worker_command(; exename, _exeflags, juliaenvstring,
        nvidia_enable_ecc, nvidia_enable_mig, use_lvm, mpi_ranks_per_worker, mpi_flags,
        cookie=Distributed.cluster_cookie(), master_address=string(getipaddr()),
        master_port=state.port, ppi)

    # Main worker block with retry loop
    push!(parts, _worker_block(user, omp_num_threads, build_envstring(env), worker_cmd))

    script = join(parts, "\n")

    # Wrap in MIME multipart if using LVM (cloud-config + shell script)
    use_lvm ? _wrap_lvm_multipart(script) : script
end

# ─── Cloud-init helpers ───

function _append_git_config!(parts, user)
    if isfile(joinpath(homedir(), ".gitconfig"))
        gitconfig = read(joinpath(homedir(), ".gitconfig"), String)
        push!(parts, """
sudo su - $user << EOF
echo '$gitconfig' > ~/.gitconfig
EOF""")
    end
    if isfile(joinpath(homedir(), ".git-credentials"))
        creds = rstrip(read(joinpath(homedir(), ".git-credentials"), String), [' ', '\n'])
        push!(parts, """
sudo su - $user << EOF
echo "$creds" > ~/.git-credentials
chmod 600 ~/.git-credentials
EOF""")
    end
end

function _append_custom_env!(parts, user, exename, customenv)
    customenv || return ""
    try
        projectinfo = Pkg.project()
        folder = normpath(joinpath(projectinfo.path, ".."))
        env_name = splitpath(folder)[end]
        pc, mc, lc = compress_environment(folder)
        push!(parts, """
sudo su - $user << 'EOF'
$exename -e 'using AzManagers; AzManagers.decompress_environment("$pc", "$mc", "$lc", "$env_name")'
$exename -e 'using Pkg; path=joinpath(Pkg.envdir(), "$env_name"); Pkg.Registry.update(); Pkg.activate(path); (retry(Pkg.instantiate))(); Pkg.precompile()'
EOF""")
        return env_name
    catch e
        @warn "Unable to use custom environment"
        logerror(e, Logging.Debug)
        return ""
    end
end

function _ensure_spot_threads(julia_num_threads, spot)
    spot || return julia_num_threads
    parts = split(julia_num_threads, ',')
    n_default = length(parts) > 0 ? parse(Int, parts[1]) : 1
    n_interactive = length(parts) > 1 ? parse(Int, parts[2]) : 0
    n_interactive == 0 && (n_interactive = 1)
    nthreads_filter("$n_default,$n_interactive")
end

function _build_worker_command(; exename, _exeflags, juliaenvstring,
        nvidia_enable_ecc, nvidia_enable_mig, use_lvm,
        mpi_ranks_per_worker, mpi_flags,
        cookie, master_address, master_port, ppi)

    setup = "$(juliaenvstring)try using AzManagers; catch; using Pkg; Pkg.instantiate(); using AzManagers; end"
    gpu_disk = "AzManagers.nvidia_gpucheck($nvidia_enable_ecc, $nvidia_enable_mig); AzManagers.mount_datadisks()$(use_lvm ? "; AzManagers.build_lvm()" : "")"

    if mpi_ranks_per_worker == 0
        """$exename $_exeflags -e '$setup; $gpu_disk; AzManagers.azure_worker("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'"""
    else
        pre = """$exename -e '$setup; $gpu_disk'"""
        mpi = """mpirun -n $mpi_ranks_per_worker $mpi_flags $exename $_exeflags -e '$(juliaenvstring)using AzManagers, MPI; AzManagers.azure_worker_mpi("$cookie", "$master_address", $master_port, $ppi, "$_exeflags")'"""
        "$pre\n\n$mpi"
    end
end

function _worker_block(user, omp_num_threads, envstring, worker_cmd)
    """
sudo su - $user << 'EOF'
export JULIA_WORKER_TIMEOUT=$(get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
export OMP_NUM_THREADS=$omp_num_threads
$envstring

attempt_number=1
maximum_attempts=5
exit_code=0
while [  \$attempt_number -le \$maximum_attempts ]; do
    $worker_cmd

    exit_code=\$?
    echo "attempt \$attempt_number done with exit code \$exit_code..."

    if [ "\$exit_code" == "42" ]; then
        echo "...breaking from retry loop due to exit code 42."
        break
    fi

    echo "...retrying after 5 seconds..."
    sleep 5
    attempt_number=\$(( attempt_number + 1 ))
done
echo "worker finished with exit code \$exit_code."
EOF"""
end

function _wrap_lvm_multipart(script)
    cloud_cfg = """
#cloud-config
disk_setup:
fs_setup:
runcmd:
  - [ bash, /usr/sbin/azure_nvme.sh ]"""

    boundary = "===Boundary==="
    """
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="$boundary"

--$boundary
Content-Type: text/cloud-config; charset="us-ascii"

$cloud_cfg

--$boundary
Content-Type: text/x-shellscript; charset="us-ascii"

$script

--$boundary--"""
end
