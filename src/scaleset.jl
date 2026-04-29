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
            logerror(e, Logging.Debug)
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
            logerror(e, Logging.Debug)
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
            if status(e) in (404, 409)
                @debug "scaleset $(scaleset.scalesetname) not found or already being deleted when attempting to delete vms, skipping."
                if haskey(pending_down(manager), scaleset)
                    delete!(pending_down(manager), scaleset)
                end
            else
                @error "error deleting scaleset vms, manual clean-up may be required."
                logerror(e, Logging.Debug)
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
        logerror(e, Logging.Debug)
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
    worker_timeout = Second(parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "720")))
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
                        logerror(e, Logging.Debug)
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

    _pending_down = pending_down(manager)
    pending_down_count = isempty(_pending_down) ? 0 : mapreduce(length, +, values(_pending_down))
    max(0, n - pending_down_count)
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

#
# Azure scale-set methods
#
function _parse_image_ref(imageref_id::AbstractString)
    parts = split(imageref_id, "/")
    k_galleries = findfirst(x -> x == "galleries", parts)
    k_images = findfirst(x -> x == "images", parts)
    k_versions = findfirst(x -> x == "versions", parts)

    gallery = k_galleries !== nothing ? parts[k_galleries+1] : ""
    sigimagename = (k_galleries !== nothing && k_images !== nothing) ? parts[k_images+1] : ""
    imagename = (k_galleries === nothing && k_images !== nothing) ? parts[k_images+1] : ""
    sigimageversion = k_versions !== nothing ? parts[k_versions+1] : ""

    sigimagename, sigimageversion, imagename
end

function scaleset_image(manager::AzManager, sigimagename, sigimageversion, imagename, template=nothing)
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
        # IMDS unavailable — fall back to template
        if template !== nothing
            return _scaleset_image_from_template(manager, sigimagename, sigimageversion, imagename, template)
        end
        return sigimagename, sigimageversion, imagename
    else
        r = fetch(t)
        image = JSON.parse(String(r.body))["id"]
        if image == ""
            # Marketplace image (no gallery id) — fall back to template
            if template !== nothing
                return _scaleset_image_from_template(manager, sigimagename, sigimageversion, imagename, template)
            end
            return sigimagename, sigimageversion, imagename
        end
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

function _scaleset_image_from_template(manager::AzManager, sigimagename, sigimageversion, imagename, template)
    # Extract image reference from the template
    local imageref_id
    if haskey(template, "properties") && haskey(template["properties"], "virtualMachineProfile")
        imageref_id = get(template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"], "id", "")
    elseif haskey(template, "properties") && haskey(template["properties"], "storageProfile")
        imageref_id = get(template["properties"]["storageProfile"]["imageReference"], "id", "")
    else
        return sigimagename, sigimageversion, imagename
    end

    imageref_id == "" && return sigimagename, sigimageversion, imagename

    t_sigimagename, t_sigimageversion, t_imagename = _parse_image_ref(imageref_id)

    sigimagename == "" && (sigimagename = t_sigimagename)
    imagename == "" && (imagename = t_imagename)

    # If no version in template, query for the latest
    if sigimagename != "" && sigimageversion == "" && t_sigimageversion == ""
        parts = split(imageref_id, "/")
        k_subscriptions = findfirst(x -> x == "subscriptions", parts)
        k_resourcegroups = findfirst(x -> x == "resourceGroups", parts)
        k_galleries = findfirst(x -> x == "galleries", parts)
        if k_subscriptions !== nothing && k_resourcegroups !== nothing && k_galleries !== nothing
            subscription = parts[k_subscriptions+1]
            resourcegroup = parts[k_resourcegroups+1]
            gallery = parts[k_galleries+1]
            _r = @retry manager.nretry azrequest(
                "GET",
                manager.verbose,
                "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourcegroup/providers/Microsoft.Compute/galleries/$gallery/images/$sigimagename/versions?api-version=2022-03-03",
                ["Authorization"=>"Bearer $(token(manager.session))"])
            r = JSON.parse(String(_r.body))
            _versions, _r = getnextlinks!(manager, _r, get(r, "value", String[]), get(r, "nextLink", ""), manager.nretry, manager.verbose)
            versions = VersionNumber.(get.(_versions, "name", ""))
            if length(versions) > 0
                sigimageversion = string(maximum(versions))
            end
        end
    elseif t_sigimageversion != ""
        sigimageversion = t_sigimageversion
    end

    @debug "from template: imagename=$imagename, sigimagename=$sigimagename, sigimageversion=$sigimageversion"
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

    packages = _packages["deps"]

    if custom_environment
        dev_packages = [name for (name, info) in packages if haskey(info[1], "path")]
        if !isempty(dev_packages)
            @warn "Project has dev'd packages ($(join(dev_packages, ", "))). " *
                  "Workers will install these from their git repositories using the current branch."
        end
    end
end

"""
    _detect_dev_packages(manifest_text, base_path) -> Vector{Tuple{String,String,String}}

Detect Pkg.develop'd packages in a Manifest and return their git info as
`(name, repo_url, repo_rev)` tuples. Packages without a discoverable git repo
are logged as warnings and omitted.
"""
function _detect_dev_packages(manifest_text, base_path)
    manifest = TOML.parse(manifest_text)
    dev_pkgs = Tuple{String,String,String}[]

    if !haskey(manifest, "deps")
        return dev_pkgs
    end

    for (pkg_name, entries) in manifest["deps"]
        for entry in entries
            if haskey(entry, "path")
                pkg_path = entry["path"]
                pkg_path = isabspath(pkg_path) ? normpath(pkg_path) : normpath(joinpath(base_path, pkg_path))

                try
                    repo_url = readchomp(Cmd(["git", "-C", pkg_path, "remote", "get-url", "origin"]))
                    repo_rev = readchomp(Cmd(["git", "-C", pkg_path, "rev-parse", "--abbrev-ref", "HEAD"]))
                    if repo_rev == "HEAD"  # detached HEAD → use commit SHA
                        repo_rev = readchomp(Cmd(["git", "-C", pkg_path, "rev-parse", "HEAD"]))
                    end
                    # Convert SSH URLs to HTTPS so workers can authenticate via .git-credentials
                    m = match(r"^git@([^:]+):(.+)$", repo_url)
                    if m !== nothing
                        repo_url = "https://$(m.captures[1])/$(m.captures[2])"
                    end
                    push!(dev_pkgs, (pkg_name, repo_url, repo_rev))
                    @debug "Dev'd package '$pkg_name' at $pkg_path → $repo_url#$repo_rev"
                catch e
                    @warn "Cannot determine git info for dev'd package '$pkg_name' at $pkg_path; " *
                          "it will be removed from the worker environment" exception=e
                end
            end
        end
    end

    dev_pkgs
end

function sanitize_manifest(manifest_text)
    manifest = TOML.parse(manifest_text)
    if haskey(manifest, "deps")
        for (pkg, entries) in manifest["deps"]
            filter!(entry -> !haskey(entry, "path"), entries)
        end
        filter!(kv -> !isempty(kv.second), manifest["deps"])
    end
    io = IOBuffer()
    TOML.print(io, manifest)
    String(take!(io))
end

function sanitize_project(project_text, dev_package_names)
    project = TOML.parse(project_text)
    if haskey(project, "deps")
        for name in dev_package_names
            delete!(project["deps"], name)
        end
    end
    io = IOBuffer()
    TOML.print(io, project)
    String(take!(io))
end

function compress_environment(julia_environment_folder)
    project_text = read(joinpath(julia_environment_folder, "Project.toml"), String)
    manifest_text = read(joinpath(julia_environment_folder, "Manifest.toml"), String)

    # Detect dev'd packages and get their git info before sanitizing
    dev_pkgs = _detect_dev_packages(manifest_text, julia_environment_folder)
    dev_pkg_names = [name for (name, _, _) in dev_pkgs]

    # Remove dev'd packages from both Project and Manifest
    project_text = sanitize_project(project_text, dev_pkg_names)
    manifest_text = sanitize_manifest(manifest_text)

    localpreferences_text = isfile(joinpath(julia_environment_folder, "LocalPreferences.toml")) ? read(joinpath(julia_environment_folder, "LocalPreferences.toml"), String) : ""
    local project_compressed,manifest_compressed,localpreferences_compressed
    with_logger(ConsoleLogger(stdout, Logging.Info)) do
        project_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(project_text)))
        manifest_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(manifest_text)))
        localpreferences_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(localpreferences_text)))
    end

    # Build Pkg.add calls for dev'd packages (workers will clone from git)
    dev_pkg_adds = ""
    for (name, url, rev) in dev_pkgs
        dev_pkg_adds *= """Pkg.add(url="$url", rev="$rev"); """
    end

    project_compressed, manifest_compressed, localpreferences_compressed, dev_pkg_adds
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

function nphysical_cores(template::Dict; session=AzSession())
    ssid = template["subscriptionid"]
    region = template["value"]["location"]
    sku_name = template["value"]["properties"]["hardwareProfile"]["vmSize"]

    _r = HTTP.request("GET", 
        "https://management.azure.com/subscriptions/$ssid/providers/Microsoft.Compute/skus?api-version=2022-11-01", 
        ["Authorization" => "Bearer $(token(session))"])
    r = JSON.parse(String(_r.body))


    filtered_skus = filter(sku -> sku["name"] == sku_name && haskey(sku, "capabilities") && any(location -> location == region, sku["locations"]), r["value"])

    vCPU_details = [(cap["value"], any(cap -> cap["name"] == "HyperThreadingEnabled" && cap["value"] == "true", sku["capabilities"])) for sku in filtered_skus for cap in sku["capabilities"] if cap["name"] == "vCPUs"]
    hyperthreading = vCPU_details[1][2]
    vCPU = vCPU_details[1][1]

    # Number of physical cores
    pCPU = hyperthreading ? div(parse(Int,vCPU),2) : parse(Int,vCPU)
end

function nphysical_cores(template::AbstractString; session=AzSession())
    isfile(templates_filename_vm()) || error("scale-set template file does not exist.  See `AzManagers.save_template_scaleset`")

    templates_scaleset = JSON.parse(read(templates_filename_vm(), String); dicttype=Dict)
    haskey(templates_scaleset, template) || error("scale-set template file does not contain a template with name: $template. See `AzManagers.save_template_scaleset`")
    template = templates_scaleset[template]

    nphysical_cores(template; session)
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
            JSON.json(body)
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
        String(JSON.json(_template)))

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
        JSON.json(body))

    if manager.show_quota
        @info "Quota after requesting deletion of $(length(ids)) virtual machines" remaining_resource(_r)
    end
end

# see https://docs.microsoft.com/en-us/azure/virtual-machines/linux/add-disk

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
