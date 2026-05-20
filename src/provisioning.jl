# ─── Image resolution ───

function resolve_image(state::ManagerState, sigimagename, sigimageversion, imagename)
    if imagename != "" || (sigimagename != "" && sigimageversion != "")
        return sigimagename, sigimageversion, imagename
    end

    image_parts = _fetch_vm_image_ref()
    image_parts === nothing && return sigimagename, sigimageversion, imagename

    k_galleries = findfirst(==("galleries"), image_parts)
    different_image = true

    if sigimagename == "" && imagename == ""
        different_image = false
        k_images = findfirst(==("images"), image_parts)
        if k_galleries !== nothing
            sigimagename = image_parts[k_images + 1]
        else
            imagename = image_parts[k_images + 1]
        end
    end

    if imagename == "" && sigimageversion == ""
        k = findfirst(==("versions"), image_parts)
        if k !== nothing && !different_image
            sigimageversion = image_parts[k + 1]
        else
            sigimageversion = _fetch_latest_version(state, image_parts, k_galleries, sigimagename)
        end
    end

    @debug "resolved image" sigimagename sigimageversion imagename
    sigimagename, sigimageversion, imagename
end

function _fetch_vm_image_ref()
    try
        tsk = @async HTTP.request("GET",
            "http://169.254.169.254/metadata/instance/compute/storageProfile/imageReference?api-version=2021-02-01",
            ["Metadata" => "true"]; retry=false, redirect=false, connect_timeout=5, readtimeout=5)
        tic = time()
        while !istaskdone(tsk) && (time() - tic) < 10; sleep(1) end
        istaskdone(tsk) || (@async Base.throwto(tsk, InterruptException()); return nothing)
        r = fetch(tsk)
        split(JSON.parse(String(r.body))["id"], "/")
    catch
        nothing
    end
end

function _fetch_latest_version(state, image_parts, k_galleries, sigimagename)
    k_sub = findfirst(==("subscriptions"), image_parts)
    k_rg = findfirst(==("resourceGroups"), image_parts)
    gallery = k_galleries !== nothing ? image_parts[k_galleries + 1] : ""
    (k_sub === nothing || k_rg === nothing || gallery == "") && return ""

    sub, rg = image_parts[k_sub + 1], image_parts[k_rg + 1]
    _r = @retry state.nretry azrequest(state, "GET",
        "$BASE_URL/subscriptions/$sub/resourceGroups/$rg/providers/Microsoft.Compute/galleries/$gallery/images/$sigimagename/versions?api-version=2022-03-03",
        auth(state))
    r = JSON.parse(String(_r.body))
    versions, _ = getnextlinks!(state, _r, get(r, "value", []), get(r, "nextLink", ""))
    vnames = VersionNumber.(get.(versions, "name", ""))
    isempty(vnames) ? "" : string(maximum(vnames))
end

# ─── Image reference manipulation ───

function _image_ref(template)
    template["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]
end

function apply_image!(template, sigimagename, sigimageversion, imagename)
    ref = _image_ref(template)
    parts = split(ref["id"], '/')

    if imagename != ""
        ref["id"] = join(parts[1:end-4], '/') * "/images/" * imagename
    else
        if sigimagename != ""
            j = findfirst(==("images"), parts)
            ref["id"] = join(parts[1:j], '/') * "/" * sigimagename
        end
        if sigimageversion != ""
            parts = split(ref["id"], '/')
            j = findfirst(==("versions"), parts)
            ref["id"] = j === nothing ? ref["id"] * "/versions/$sigimageversion" : join(parts[1:j], '/') * sigimageversion
        end
    end
    @debug "using image" id=ref["id"]
end

function image_osdisksize(state::ManagerState, template, sigimagename, sigimageversion, imagename)
    parts = split(_image_ref(template)["id"], '/')
    k_sub = findfirst(==("subscriptions"), parts)
    k_rg = findfirst(==("resourceGroups"), parts)
    k_gal = findfirst(==("galleries"), parts)
    k_img = findfirst(==("images"), parts)
    (k_sub === nothing || k_rg === nothing || k_img === nothing) && return 0

    sub, rg = parts[k_sub + 1], parts[k_rg + 1]
    url = if k_gal !== nothing
        gallery = parts[k_gal + 1]
        img = sigimagename != "" ? sigimagename : parts[k_img + 1]
        ver = sigimageversion != "" ? sigimageversion : "latest"
        "$BASE_URL/subscriptions/$sub/resourceGroups/$rg/providers/Microsoft.Compute/galleries/$gallery/images/$img/versions/$ver?api-version=2022-03-03"
    else
        img = imagename != "" ? imagename : parts[k_img + 1]
        "$BASE_URL/subscriptions/$sub/resourceGroups/$rg/providers/Microsoft.Compute/images/$img?api-version=$API_VERSION_COMPUTE"
    end

    try
        _r = @retry state.nretry azrequest(state, "GET", url, auth(state))
        r = JSON.parse(String(_r.body))
        get(get(get(r, "properties", Dict()), "storageProfile", Dict()), "osDiskImage", Dict()) |> d -> get(d, "sizeInGB", 0)
    catch
        0
    end
end

# ─── Template transforms (each mutates the ARM template dict) ───

function set_computer_prefix!(t, ss::ScaleSet)
    t["properties"]["virtualMachineProfile"]["osProfile"]["computerNamePrefix"] = "$(ss.name)-"
end

function set_osdisk!(t, state, sigimagename, sigimageversion, imagename, osdisksize)
    img_size = image_osdisksize(state, t, sigimagename, sigimageversion, imagename)
    t["properties"]["virtualMachineProfile"]["storageProfile"]["osDisk"]["diskSizeGB"] = max(osdisksize, img_size)
end

function set_user_tag!(t, state)
    try
        decoded = claims(JWT(; jwt=token(state.session)))
        if haskey(decoded, "unique_name")
            t["tags"] = get(t, "tags", Dict{Any,Any}())
            t["tags"]["UserUniqueName"] = decoded["unique_name"]
        end
    catch; end
end

function set_ssh_key!(t, state)
    keydata = read(_manifest["ssh_public_key_file"], String)
    key = Dict("path" => "/home/$(state.ssh_user)/.ssh/authorized_keys", "keyData" => keydata)
    push!(t["properties"]["virtualMachineProfile"]["osProfile"]["linuxConfiguration"]["ssh"]["publicKeys"], key)
end

function set_cloud_init!(t, state, template; kwargs...)
    cmd = build_cloud_init(state, state.ssh_user, template; kwargs...)
    encoded = base64encode(cmd)
    length(encoded) > 64_000 && error("cloud-init custom data too large ($(length(encoded)) bytes)")
    t["properties"]["virtualMachineProfile"]["osProfile"]["customData"] = encoded
end

function set_overprovision!(t, overprovision)
    if overprovision
        t["properties"]["overprovision"] = true
        t["properties"]["doNotRunExtensionsOnOverprovisionedVMs"] = true
    else
        t["properties"]["overprovision"] = false
    end
end

function set_spot!(t; spot, maxprice, spot_base_regular_priority_count, spot_regular_percentage_above_base)
    spot || return
    vm = t["properties"]["virtualMachineProfile"]
    vm["priority"] = "Spot"
    vm["evictionPolicy"] = "Delete"
    vm["billingProfile"] = Dict("maxPrice" => maxprice)

    (spot_base_regular_priority_count > 0 || spot_regular_percentage_above_base > 0) || return
    t["properties"]["orchestrationMode"] = "Flexible"
    vm["networkProfile"]["networkApiVersion"] = "2020-11-01"
    t["properties"]["priorityMixPolicy"] = Dict(
        "baseRegularPriorityCount" => spot_base_regular_priority_count,
        "regularPriorityPercentageAboveBase" => spot_regular_percentage_above_base)
    t["properties"]["platformFaultDomainCount"] = 1
    for k in ("overprovision", "doNotRunExtensionsOnOverprovisionedVMs", "upgradePolicy")
        delete!(t["properties"], k)
    end
end

function set_hyperthreading!(t, hyperthreading)
    hyperthreading === nothing && return
    t["tags"] = get(t, "tags", Dict{Any,Any}())
    t["tags"]["platformsettings.host_environment.disablehyperthreading"] = hyperthreading ? "False" : "True"
end

# ─── Quota ───

function check_quota(state::ManagerState, subscriptionid, template, δn)
    location = template["location"]
    f = HTTP.escapeuri("location eq '$location'")

    _r = @retry state.nretry azrequest(state, "GET",
        "$BASE_URL/subscriptions/$subscriptionid/providers/Microsoft.Compute/skus?api-version=2021-07-01&\$filter=$f",
        auth(state))
    vms = filter(r -> r["resourceType"] == "virtualMachines", JSON.parse(String(_r.body))["value"])

    sku_name = template["sku"]["name"]
    k = findfirst(vm -> vm["name"] == sku_name, vms)
    k === nothing && error("VM size $sku_name not found in $location")

    ncores = parse(Int, let caps = vms[k]["capabilities"]
        k_vcpu = findfirst(c -> c["name"] == "vCPUs", caps)
        k_vcpu === nothing && error("vCPUs capability not found")
        caps[k_vcpu]["value"]
    end)

    _r = @retry state.nretry azrequest(state, "GET",
        "$BASE_URL/subscriptions/$subscriptionid/providers/Microsoft.Compute/locations/$location/usages?api-version=2019-07-01",
        auth(state))
    usages = JSON.parse(String(_r.body))["value"]

    family = vms[k]["family"]
    avail = _usage_remaining(usages, family)
    spot_avail = _usage_remaining(usages, "lowPriorityCores")

    (regular=avail - ncores * δn, spot=spot_avail - ncores * δn)
end

function _usage_remaining(usages, name)
    k = findfirst(u -> u["name"]["value"] == name, usages)
    k === nothing && error("quota entry '$name' not found")
    usages[k]["limit"] - usages[k]["currentValue"]
end

function wait_for_quota!(state, ss, template, δn, spot)
    while true
        quota = check_quota(state, ss.subscriptionid, template, δn)
        avail = spot ? quota.spot : quota.regular
        avail >= 0 && return
        @warn "Insufficient $(spot ? "spot " : "")quota ($(-avail) cores short). Sleeping 60s..." scaleset=ss.name
        try sleep(60) catch e; isa(e, InterruptException) || rethrow(); return end
    end
end

# ─── Service health ───

function check_service_health(state::ManagerState, subscriptionid, region)
    try
        query_start = Dates.format(now(Dates.UTC) - Day(1), "m/d/yyyy")
        filter_str = HTTP.escapeuri("service eq 'Virtual Machines' or service eq 'Virtual Network'")
        _r = @retry state.nretry azrequest(state, "GET",
            "$BASE_URL/subscriptions/$subscriptionid/providers/Microsoft.ResourceHealth/events?api-version=2025-05-01&\$filter=$filter_str&queryStartTime=$query_start",
            auth(state))
        for event in get(JSON.parse(String(_r.body)), "value", [])
            props = get(event, "properties", Dict())
            get(props, "eventType", "") == "ServiceIssue" && get(props, "status", "") == "Active" || continue
            for impact in get(props, "impact", [])
                for ir in get(impact, "impactedRegions", [])
                    rname = lowercase(replace(get(ir, "impactedRegion", ""), " " => ""))
                    if rname == lowercase(replace(region, " " => "")) && get(ir, "status", "") == "Active"
                        return (true, "Active $(get(impact, "impactedService", "?")) incident: $(get(props, "title", "?"))")
                    end
                end
            end
        end
        (false, "")
    catch e
        @warn "service health check failed, proceeding" exception=(e, catch_backtrace())
        (false, "")
    end
end

# ─── Main provisioning pipeline ───

function create_or_update_scaleset!(state::ManagerState, ss::ScaleSet, template::Dict, δn::Int;
        sigimagename, sigimageversion, imagename, osdisksize,
        customenv, overprovision, ppi,
        julia_num_threads, omp_num_threads,
        exename, exeflags, env,
        spot, maxprice,
        spot_base_regular_priority_count, spot_regular_percentage_above_base,
        mpi_ranks_per_worker, mpi_flags,
        nvidia_enable_ecc, nvidia_enable_mig, hyperthreading, use_lvm)

    # Gate: service health
    region = get(get(template, "value", Dict()), "location", "")
    if region != ""
        blocked, reason = check_service_health(state, ss.subscriptionid, region)
        if blocked
            @warn "scaling paused due to Azure incident" reason region scaleset=ss.name
            return -1
        end
    end

    # Build ARM template
    t = deepcopy(template["value"])
    set_computer_prefix!(t, ss)
    set_osdisk!(t, state, sigimagename, sigimageversion, imagename, osdisksize)
    set_user_tag!(t, state)
    set_ssh_key!(t, state)
    apply_image!(t, sigimagename, sigimageversion, imagename)
    julia_num_threads = nthreads_filter(julia_num_threads)
    set_cloud_init!(t, state, template;
        spot, ppi, mpi_ranks_per_worker, mpi_flags,
        nvidia_enable_ecc, nvidia_enable_mig,
        julia_num_threads, omp_num_threads,
        exename, exeflags, env, customenv, use_lvm)
    set_overprovision!(t, overprovision)
    set_spot!(t; spot, maxprice, spot_base_regular_priority_count, spot_regular_percentage_above_base)
    set_hyperthreading!(t, hyperthreading)

    # Capacity + quota
    n = scaleset_capacity(state, ss) + δn
    wait_for_quota!(state, ss, t, δn, spot)

    # PUT to Azure
    @info "scaling scaleset" scaleset=ss.name target=n added=δn spot
    t["sku"]["capacity"] = n
    @retry state.nretry azrequest(state, "PUT", scaleset_detail_url(ss), auth_json(state), JSON.json(t))

    n
end
