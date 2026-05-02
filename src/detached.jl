#
# detached service client API
#

"""
    poll_nic_ready(session, subscriptionid, resourcegroup, nicname; timeout, nretry, verbose)

Poll Azure until NIC `nicname` reaches "Succeeded" state.  Returns the parsed NIC dictionary.
Throws on "Failed" state or timeout.
"""
function poll_nic_ready(session, subscriptionid, resourcegroup, nicname; timeout, nretry, verbose)
    starttime = time()
    while true
        nic_r = @retry nretry azrequest(
            "GET",
            verbose,
            "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2024-03-01",
            ["Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(session))"])
        nic_dic = JSON.parse(String(nic_r.body))
        state = nic_dic["properties"]["provisioningState"]

        state == "Succeeded" && return nic_dic
        state == "Failed" && error("NIC creation failed for $nicname")
        (time() - starttime > timeout) && error("Timed out after $timeout seconds waiting for NIC $nicname to provision (state=$state)")
        sleep(10)
    end
end

"""
    poll_vm_ready(session, subscriptionid, resourcegroup, vmname; timeout, nretry, verbose)

Poll Azure until VM `vmname` reaches "Succeeded" state.  Returns nothing.
Throws on "Failed" state or timeout.  Prints a spinner while waiting.
"""
function poll_vm_ready(session, subscriptionid, resourcegroup, vmname; timeout, nretry, verbose)
    spincount = 1
    starttime = time()
    poll_interval = 10
    while true
        sleep(poll_interval)
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

        elapsed_time = time() - starttime
        if elapsed_time > timeout
            error("reached timeout ($timeout seconds) while creating head VM.")
        end

        write(stdout, spin(spincount, elapsed_time)*", waiting for VM, $vmname, to start.\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1
    end
    elapsed_time = time() - starttime
    write(stdout, spin(5, elapsed_time)*", waiting for VM, $vmname, to start.\r")
    write(stdout, "\n")
end

"""
    poll_vm_deleted(manager, session, subscriptionid, resourcegroup, vmname; timeout, nretry, verbose)

Poll Azure until VM `vmname` no longer appears in the resource group VM list.
Warns on timeout rather than throwing.
"""
function poll_vm_deleted(manager, session, subscriptionid, resourcegroup, vmname; timeout, nretry, verbose)
    spincount = 1
    starttime = time()
    poll_interval = 10
    while true
        sleep(poll_interval)

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

        elapsed_time = time() - starttime
        if elapsed_time > timeout
            @warn "Unable to delete virtual machine in $timeout seconds"
            break
        end

        write(stdout, spin(spincount, elapsed_time)*", waiting for VM, $vmname, to delete.\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1
    end
    elapsed_time = time() - starttime
    write(stdout, spin(5, elapsed_time)*", waiting for VM, $vmname, to delete.\r")
    write(stdout, "\n")
    nothing
end

"""
    poll_detached_service(vm, custom_environment; timeout)

Poll the detached service HTTP endpoint until it responds to a ping.
Throws `DetachedServiceTimeoutException` on timeout.
"""
function poll_detached_service(vm, custom_environment; timeout)
    spincount = 1
    starttime = time()
    poll_interval = 5
    waitfor = custom_environment ? "Julia package instantiation and COFII detached service" : "COFII detached service"
    while true
        sleep(poll_interval)

        try
            HTTP.request("GET", "http://$(vm["ip"]):$(vm["port"])/cofii/detached/ping")
            break
        catch
        end

        elapsed_time = time() - starttime

        if elapsed_time > timeout
            @error "reached timeout ($timeout seconds) while waiting for $waitfor to start."
            throw(DetachedServiceTimeoutException(vm))
        end

        write(stdout, spin(spincount, elapsed_time)*", waiting for $waitfor on VM, $(vm["name"]):$(vm["port"]), to start.\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1
    end
    elapsed_time = time() - starttime
    write(stdout, spin(5, elapsed_time)*", waiting for $waitfor on VM, $(vm["name"]):$(vm["port"]), to start.\r")
    write(stdout, "\n")
end

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
* `exename="\$(Sys.BINDIR)/julia"` name of the julia executable.
* `env=Dict()` Dictionary of environemnt variables that will be exported before starting the detached process
* `detachedservice=true` start the detached service allowing for RESTful remote code execution
* `use_lvm=false` For SKUs that have 1 or more nvme disks, combines all disks as a single mount point /scratch vs /scratch, /scratch1, /scratch2, etc..
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
        exename = "$(Sys.BINDIR)/julia",
        env = Dict(),
        detachedservice = true,
        use_lvm = false)
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

        # For a different PR
        # nic_template = get(nic_templates, vm_template["default_nic"], first(_keys))

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
    sigimagename, sigimageversion, imagename = scaleset_image(manager, sigimagename, sigimageversion, imagename, vm_template["value"])
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
        String(JSON.json(nic_template)))

    sleep(5)

    nic_timeout = parse(Int, get(ENV, "JULIA_WORKER_TIMEOUT", "720"))
    nic_task = @async poll_nic_ready(session, subscriptionid, resourcegroup, nicname; timeout=nic_timeout, nretry, verbose)
    nic_dic = fetch(nic_task)
    nic_id = nic_dic["id"]

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
        cmd = buildstartupscript_detached(manager, exename, nthreads_filter(julia_num_threads), omp_num_threads, env, user,
            disk, customenv, subscriptionid, resourcegroup, vmname, use_lvm)
    else
        cmd,_ = buildstartupscript(manager, exename, user, disk, customenv, use_lvm)
    end
    
    _cmd = base64encode(cmd)

    if length(_cmd) > 64_000
        error("custom data is too large.")
    end

    vm_template["value"]["properties"]["osProfile"]["customData"] = _cmd

    # When the VM is deleted, auto-delete attached nic's and disks.
    vm_template["value"]["properties"]["storageProfile"]["osDisk"]["deleteOption"] = "Delete"
    for network_interface in get(vm_template["value"]["properties"]["networkProfile"], "networkInterfaces", [])
        if !haskey(network_interface, "properties")
            network_interface["properties"] = Dict()
        end
        network_interface["properties"]["deleteOption"] = "Delete"
    end

    for attached_disk in get(vm_template["value"]["properties"]["storageProfile"], "dataDisks", [])
        attached_disk["deleteOption"] = "Delete"
    end

    # vm quota check
    @debug "quota check"
    max_quota_retries = parse(Int, get(ENV, "JULIA_AZMANAGERS_QUOTA_MAX_RETRIES", "60"))
    for quota_attempt in 1:max_quota_retries
        navailable_cores, navailable_cores_spot = quotacheck(manager, subscriptionid, vm_template["value"], 1, nretry, verbose)
        navailable_cores >= 0 && break
        @warn "Insufficient quota for VM. Attempt $quota_attempt/$max_quota_retries, sleeping for 60 seconds."

        if quota_attempt == max_quota_retries
            error("Exhausted $max_quota_retries quota retries (~$(max_quota_retries) minutes). Insufficient quota to provision VM.")
        end

        try
            sleep(60)
        catch e
            isa(e, InterruptException) || rethrow(e)
            @warn "Received interrupt, canceling AzManagers operation."
            return
        end
    end

    @debug "making vm"
    r = @retry nretry azrequest(
        "PUT",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?api-version=2023-09-01",
        ["Content-Type"=>"application/json", "Authorization"=>"Bearer $(token(session))"],
        String(JSON.json(vm_template["value"])))

    vm_task = @async poll_vm_ready(session, subscriptionid, resourcegroup, vmname; timeout, nretry, verbose)
    fetch(vm_task)

    _r = @retry nretry azrequest(
        "GET",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Network/networkInterfaces/$nicname?api-version=2022-09-01",
        ["Authorization"=>"Bearer $(token(session))"])

    r = JSON.parse(String(_r.body))

    vm = Dict("name"=>vmname, "ip"=>string(r["properties"]["ipConfigurations"][1]["properties"]["privateIPAddress"]),
        "subscriptionid"=>string(subscriptionid), "resourcegroup"=>string(resourcegroup), "port"=>string(detached_port()),
        "julia_num_threads"=>string(julia_num_threads), "omp_num_threads"=>string(omp_num_threads), "exename"=>string(exename),
        "size"=>vm_template["value"]["properties"]["hardwareProfile"]["vmSize"])

    if detachedservice
        detached_service_wait(vm, customenv)
    elseif customenv
        @info "There will be a delay before the custom environment is instantiated, but this work is happening asynchronously"
    end

    vm
end

function addproc(vm_template::AbstractString, nic_template=nothing; kwargs...)
    isfile(templates_filename_vm()) || error("if vm_template is a string, then the file $(templates_filename_vm()) must exist.  See AzManagers.save_template_vm.")
    vm_templates = JSON.parse(read(templates_filename_vm(), String); dicttype=Dict)
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

    manager = azmanager!(session, "", nretry, verbose, false, show_quota)

    r = @retry nretry azrequest(
        "DELETE",
        verbose,
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachines/$vmname?api-version=2022-11-01",
        ["Authorization"=>"Bearer $(token(session))"])

    if r.status >= 300
        @warn "Problem removing VM, $vmname, status=$(r.status)"
    else
        @info "VM '$vmname' deletion accepted by Azure (status=$(r.status))"

        # Extract async operation URL for background verification
        async_url = ""
        for (k, v) in r.headers
            if k == "Azure-AsyncOperation"
                async_url = v
                break
            end
        end

        if async_url != "" && isdefined(manager, :events) && isopen(manager.events)
            try
                put!(manager.events, DeletionStarted(vmname, async_url, session))
            catch
            end
        end
    end
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

struct DetachedServiceTimeoutException <: Exception
    vm
end

function detached_service_wait(vm, custom_environment)
    timeout = Distributed.worker_timeout()
    svc_task = @async poll_detached_service(vm, custom_environment; timeout)
    fetch(svc_task)
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
        exename = "$(Sys.BINDIR)/julia",
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
            exename = exename,
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
        "exename" => get(vm, "exename", "julia"),
        "variablebundle" => base64encode(take!(io)),
        "code" => """
        $code
        """)

    _r = HTTP.request(
        "POST",
        "http://$(vm["ip"]):$(vm["port"])/cofii/detached/run",
        ["Content-Type"=>"application/json"],
        JSON.json(body))
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
