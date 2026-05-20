# ─── Worker entry points (called from cloud-init on remote VMs) ───

function azure_worker_init(cookie, master_address, master_port, ppi, exeflags, localid)
    bind_addr = string(getipaddr())
    userdata = _fetch_worker_metadata(bind_addr, ppi, exeflags, localid)

    c = connect(master_address, master_port)
    write(c, cookie)
    write(c, base64encode(JSON.json(Dict(
        "bind_addr" => bind_addr,
        "ppi" => ppi,
        "exeflags" => exeflags,
        "userdata" => userdata))) * "\n")
    c
end

function _fetch_worker_metadata(bind_addr, ppi, exeflags, localid)
    userdata = Dict(
        "bind_addr" => bind_addr,
        "ppi" => ppi,
        "exeflags" => exeflags,
        "localid" => localid,
        "priority" => get(ENV, "AZURE_VM_PRIORITY", "regular"),
        "physical_hostname" => gethostname())
    try
        _r = HTTP.request("GET",
            "http://169.254.169.254/metadata/instance?api-version=2021-02-01",
            ["Metadata" => "true"]; retry=false, redirect=false, connect_timeout=5, readtimeout=5)
        compute = get(JSON.parse(String(_r.body)), "compute", Dict())
        userdata["subscriptionid"] = get(compute, "subscriptionId", "")
        userdata["resourcegroup"] = get(compute, "resourceGroupName", "")
        userdata["name"] = get(compute, "name", gethostname())
        userdata["scalesetname"] = get(compute, "vmScaleSetName", "")
        userdata["instanceid"] = get(compute, "platformFaultDomain", "0")
        lowercase(get(compute, "priority", "regular")) == "spot" && (userdata["priority"] = "spot")
    catch
        userdata["subscriptionid"] = ""
        userdata["resourcegroup"] = ""
        userdata["name"] = gethostname()
        userdata["scalesetname"] = ""
        userdata["instanceid"] = "0"
    end
    userdata
end

function azure_worker_start(c, cookie)
    Distributed.start_worker(c, cookie)
end

function azure_worker(cookie, master_address, master_port, ppi, exeflags)
    for attempt in 1:10
        local c
        try
            c = azure_worker_init(cookie, master_address, master_port, ppi, exeflags, 0)
            azure_worker_start(c, cookie)
        catch e
            @error "worker start failed" attempt cookie master_address master_port ppi
            logerror(e, Logging.Debug)
            attempt == 10 && rethrow()
            @isdefined(c) && try close(c) catch end
        end
        sleep(60)
    end
end

function azure_worker_mpi end

# ─── Environment decompression (called on workers) ───

function decompress_environment(project_compressed, manifest_compressed, lp_compressed, env_name)
    dir = joinpath(Pkg.envdir(), env_name)
    mkpath(dir)
    for (data, file) in [(project_compressed, "Project.toml"),
                         (manifest_compressed, "Manifest.toml"),
                         (lp_compressed, "LocalPreferences.toml")]
        text = String(CodecZlib.transcode(ZlibDecompressor, Vector{UInt8}(base64decode(data))))
        text != "" && write(joinpath(dir, file), text)
    end
end

# ─── GPU utilities ───

function nvidia_has_nvidia_smi()
    Sys.which("nvidia-smi") === nothing && return false
    success(open(`nvidia-smi`))
end

function nvidia_gpumode(feature)
    p = open(`nvidia-smi --query-gpu=$feature.mode.current --format=csv`)
    wait(p)
    success(p) || return Bool[]
    [lowercase(l) == "enabled" for l in readlines(p) if lowercase(l) != "$feature.mode.current"]
end

function nvidia_gpumode!(feature, switch)
    success(open(`sudo nvidia-smi $feature $(switch ? 1 : 0)`)) || @error "failed to toggle NVIDIA $feature"
end

function nvidia_gpucheck(enable_ecc=true, enable_mig=false)
    nvidia_has_nvidia_smi() || return

    ecc = nvidia_gpumode("ecc")
    switch_ecc = (!all(ecc) && enable_ecc) || (any(ecc) && !enable_ecc)
    switch_ecc && nvidia_gpumode!("-e", enable_ecc)

    mig = nvidia_gpumode("mig")
    switch_mig = (!all(mig) && enable_mig) || (any(mig) && !enable_mig)
    switch_mig && nvidia_gpumode!("-mig", enable_mig)

    (switch_ecc || switch_mig) && run(`sudo reboot`)
end

# ─── Disk utilities ───

function mount_datadisks()
    try
        _r = HTTP.request("GET",
            "http://169.254.169.254/metadata/instance?api-version=2021-02-01",
            ["Metadata" => "true"]; redirect=false)
        luns = Set(d["lun"] for d in JSON.parse(String(_r.body))["compute"]["storageProfile"]["dataDisks"])
        blks = JSON.parse(String(read(open(`lsblk -J -o NAME,HCTL,MOUNTPOINTS,TYPE`))))

        for blk in blks["blockdevices"]
            hctl = blk["hctl"]
            mounts = blk["mountpoints"]
            hctl === nothing && continue
            blk["type"] == "disk" || continue
            haskey(blk, "children") && continue
            isempty(mounts) || mounts[1] !== nothing && continue
            lun = split(hctl, ':')[end]
            lun in luns || continue

            name = blk["name"]
            run(`sudo parted /dev/$name --script mklabel gpt mkpart primary ext4 0% 100%`)
            sleep(2)
            run(`sudo mkfs.ext4 -F /dev/$(name)1`)
            run(`sudo mkdir -p /mnt/$name`)
            run(`sudo mount /dev/$(name)1 /mnt/$name`)
            run(`sudo chmod 777 /mnt/$name`)
            @debug "mounted data disk" name="/dev/$name"
        end
    catch e
        @warn "failed to mount data disks" exception=(e, catch_backtrace())
    end
end

function build_lvm()
    if isfile("/usr/sbin/azure_nvme.sh")
        run(`sudo bash /usr/sbin/azure_nvme.sh`)
    else
        @warn "No scratch nvme script found"
    end
end

# ─── Spot preemption (runs on workers) ───

struct SpotPreemptException <: Exception
    notbefore::String
    clusterid::String
end

function machine_preempt_loop(ch)
    while true
        try
            _r = HTTP.request("GET",
                "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01",
                ["Metadata" => "true"]; retry=false, redirect=false, connect_timeout=5, readtimeout=28)
            for event in get(JSON.parse(String(_r.body)), "Events", [])
                get(event, "EventType", "") == "Preempt" && throw(SpotPreemptException(
                    get(event, "NotBefore", ""), gethostname()))
            end
        catch e
            isa(e, SpotPreemptException) && rethrow()
        end
        sleep(5)
    end
end

function machine_preempt_channel_future(pid)
    st = manager_state()
    get(st.preempt_channels, pid, nothing)
end
