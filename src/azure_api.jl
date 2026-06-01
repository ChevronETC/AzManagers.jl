const RETRYABLE_HTTP_ERRORS = (
    409,  # Conflict
    429,  # Too many requests
    500)  # Internal server error

function isretryable(e::HTTP.StatusError)
    e.status ∈ RETRYABLE_HTTP_ERRORS && (return true)
    false
end
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

function retrywarn(i, retries, seconds, e)
    if isa(e, HTTP.ExceptionRequest.StatusError)
        @debug "$(e.status): $(String(e.response.body)), retry $i of $retries, retrying in $seconds seconds"
        if e.status == 429
            remaining_resource_header = nothing
            for header in e.response.headers
                if header[1] == "x-ms-ratelimit-remaining-resource"
                    remaining_resource_header = header
                    break
                end
            end
            @warn "The Azure service is throttling the request, asking to retry after $seconds seconds. Quota information:" remaining_resource_header[2]
        elseif e.status == 500
            body = JSON.parse(String(e.response.body))
            errorcode = get(get(body, "error", Dict()), "code", "")
            @warn "errorcode: $errorcode, retry $i, retrying in $seconds seconds"
        elseif e.status == 409
            body = JSON.parse(String(e.response.body))
            errorcode = get(get(body, "error", Dict()), "code", "")
            errormessage = get(get(body, "error", Dict()), "message", "")
            @warn "($errorcode): $errormessage; retry $i of $retries, retrying in $seconds seconds"
        else
            @warn "status=$(e.status): $(String(e.response.body)), retry $i of $retries, retrying in $seconds seconds"
        end
    else
        @warn "warn: $(typeof(e)) -- retry $i, retrying in $seconds seconds"
        logerror(e, Logging.Debug)
    end
end

macro retry(retries, ex::Expr)
    quote
        result = nothing
        for i = 0:$(esc(retries))
            try
                result = $(esc(ex))
                break
            catch e
                (i < $(esc(retries)) && isretryable(e)) || throw(e)
                maximum_backoff = 256
                seconds = min(2.0^(i - 1), maximum_backoff) + rand()
                if status(e) ∈ (429, 500)
                    for header in e.response.headers
                        if lowercase(header[1]) == "retry-after"
                            seconds = parse(Int, header[2]) + rand()
                            break
                        end
                    end
                end
                retrywarn(i, $(esc(retries)), seconds, e)
                sleep(seconds)
            end
        end
        result
    end
end

function azrequest(rtype, verbose, url, headers, body=nothing)
    if contains(url, "virtualMachineScaleSets")
        manager = azmanager()
        if isdefined(manager, :scaleset_request_counter)
            manager.scaleset_request_counter += 1
        else
            manager.scaleset_request_counter = 1
        end
    end

    options = (retry=false, status_exception=false)
    if body === nothing
        response = HTTP.request(rtype, url, headers; verbose=verbose, options...)
    else
        response = HTTP.request(rtype, url, headers, body; verbose=verbose, options...)
    end

    if response.status >= 300
        throw(HTTP.Exceptions.StatusError(
            response.status,
            response.request.method,
            response.request.target,
            response))
    end

    response
end

function scaleset_request_counter()
    manager = azmanager()
    if isdefined(manager, :scaleset_request_counter)
        return manager.scaleset_request_counter
    else
        return 1
    end
end

function remaining_resource(response)
    remaining_resource_header = ""
    for header in response.headers
        if header[1] == "x-ms-ratelimit-remaining-resource"
            remaining_resource_header = header[2]
        end
    end
    remaining_resource_header
end

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
    catch
        @warn "unable to get scheduledevents."
        return false, ""
    end
    r = JSON.parse(String(_r.body))
    for event in get(r, "Events", [])
        if get(event, "EventType", "") == "Preempt" && instanceid ∈ get(event, "Resources", [])
            @warn "Machine with id $clusterid ($instanceid) is being pre-empted" now(Dates.UTC) event["NotBefore"] event["EventType"] event["EventSource"]
            return true, event["NotBefore"]
        end
    end
    return false, ""
end

function azure_compute_usages_url(subscriptionid, location)
    base_url = "https://management.azure.com/subscriptions/$subscriptionid"
    compute_path = "providers/Microsoft.Compute/locations/$location/usages"
    "$base_url/$compute_path?api-version=2019-07-01"
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
        azure_compute_usages_url(subscriptionid, location),
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

function nphysical_cores(template::AbstractDict; session=AzSession())
    ssid = template["subscriptionid"]
    region = template["value"]["location"]
    sku_name = template["value"]["properties"]["hardwareProfile"]["vmSize"]

    _r = HTTP.request("GET",
        "https://management.azure.com/subscriptions/$ssid/providers/Microsoft.Compute/skus?api-version=2022-11-01",
        ["Authorization" => "Bearer $(token(session))"])
    r = JSON.parse(String(_r.body))

    filtered_skus = filter(sku -> sku["name"] == sku_name && haskey(sku, "capabilities") && any(location -> location == region, sku["locations"]), r["value"])
    isempty(filtered_skus) && error("SKU $sku_name not found in region $region")
    capabilities = filtered_skus[1]["capabilities"]

    # Azure's compute SKUs API exposes hyperthreading via the documented
    # `vCPUsPerCore` capability (1 = HT off, 2 = HT on); the `HyperThreadingEnabled`
    # capability is not reliably emitted for every SKU family (notably the v3
    # family, where it is absent), which would silently return vCPU as if HT
    # were disabled.
    cap_value(name, default) = let k = findfirst(c -> c["name"] == name, capabilities)
        k === nothing ? default : capabilities[k]["value"]
    end

    vCPU = parse(Int, cap_value("vCPUs", "1"))
    vcpus_per_core = parse(Int, cap_value("vCPUsPerCore", "1"))
    div(vCPU, vcpus_per_core)
end

function nphysical_cores(template::AbstractString; session=AzSession())
    isfile(templates_filename_vm()) || error("scale-set template file does not exist.  See `AzManagers.save_template_scaleset`")

    templates_scaleset = JSON.parse(read(templates_filename_vm(), String); dicttype=Dict)
    haskey(templates_scaleset, template) || error("scale-set template file does not contain a template with name: $template. See `AzManagers.save_template_scaleset`")
    template = templates_scaleset[template]

    nphysical_cores(template; session)
end

function collect_nextlink_pages!(request_page, value, nextlink)
    last_response = nothing
    while nextlink != ""
        last_response = request_page(nextlink)
        page = JSON.parse(String(last_response.body))
        value = [value; get(page, "value", [])]
        nextlink = get(page, "nextLink", "")
    end
    value, last_response
end

function getnextlinks!(manager::AzManager, _r, value, nextlink, nretry, verbose)
    request_page = function (url)
        @retry nretry azrequest(
            "GET",
            verbose,
            url,
            ["Authorization"=>"Bearer $(token(manager.session))"])
    end
    value, next_response = collect_nextlink_pages!(request_page, value, nextlink)
    _r = next_response === nothing ? _r : next_response
    value, _r
end

function collect_resourcegraph_pages(request_page, body)
    skiptoken = ""
    data = []
    last_response = nothing
    while true
        if skiptoken != ""
            body["\$skipToken"] = skiptoken
        end
        last_response = request_page(body)
        r = JSON.parse(String(last_response.body))
        data = [data; get(r, "data", [])]
        skiptoken = get(r, "\$skipToken", "")

        if skiptoken == ""
            break
        end
    end

    data, last_response
end

function resourcegraphrequest(manager, body)
    request_page = function (request_body)
        @retry manager.nretry azrequest(
            "POST",
            manager.verbose,
            "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01",
            ["Authorization"=>"Bearer $(token(manager.session))", "Content-Type"=>"application/json"],
            JSON.json(request_body)
        )
    end
    data, _r = collect_resourcegraph_pages(request_page, body)

    if manager.show_quota
        @info "Quota after getting instances for scaleset pruning" remaining_resource(_r)
    end

    data
end
