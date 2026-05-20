# ─── API versions ───

const API_VERSION_COMPUTE = "2023-03-01"
const API_VERSION_COMPUTE_DELETE = "2023-07-01"
const API_VERSION_NETWORK = "2024-05-01"
const API_VERSION_REIMAGE = "2024-07-01"
const API_VERSION_VM = "2024-07-01"

const BASE_URL = "https://management.azure.com"

# ─── URL templates ───

scaleset_url(ss::ScaleSet) =
    "$BASE_URL/subscriptions/$(ss.subscriptionid)/resourceGroups/$(ss.resourcegroup)/providers/Microsoft.Compute/virtualMachineScaleSets/$(ss.name)"

scaleset_list_url(ss::ScaleSet) =
    "$BASE_URL/subscriptions/$(ss.subscriptionid)/resourceGroups/$(ss.resourcegroup)/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=$API_VERSION_COMPUTE"

scaleset_vms_url(ss::ScaleSet) =
    "$(scaleset_url(ss))/virtualMachines?api-version=$API_VERSION_VM&\$expand=instanceView"

scaleset_nics_url(ss::ScaleSet) =
    "$(scaleset_url(ss))/networkInterfaces?api-version=$API_VERSION_COMPUTE"

scaleset_delete_vms_url(ss::ScaleSet) =
    "$(scaleset_url(ss))/delete?forceDeletion=True&api-version=$API_VERSION_COMPUTE_DELETE"

scaleset_reimage_url(ss::ScaleSet) =
    "$(scaleset_url(ss))/reimage?api-version=$API_VERSION_REIMAGE"

scaleset_detail_url(ss::ScaleSet) =
    "$(scaleset_url(ss))?api-version=$API_VERSION_COMPUTE"

# ─── Auth headers ───

auth(state::ManagerState) = ["Authorization" => "Bearer $(token(state.session))"]
auth_json(state::ManagerState) = ["Authorization" => "Bearer $(token(state.session))", "Content-Type" => "application/json"]

# ─── Low-level request helper ───

function azrequest(state::ManagerState, method, url, headers, body=nothing)
    options = (retry=false, status_exception=false)
    r = if body === nothing
        HTTP.request(method, url, headers; verbose=state.verbose, options...)
    else
        HTTP.request(method, url, headers, body; verbose=state.verbose, options...)
    end
    r.status >= 300 && throw(HTTP.Exceptions.StatusError(r.status, r.request.method, r.request.target, r))
    r
end

# ─── Pagination ───

function getnextlinks!(state::ManagerState, _r, value, nextlink)
    while nextlink != ""
        _r = @retry state.nretry azrequest(state, "GET", nextlink, auth(state))
        r = JSON.parse(String(_r.body))
        value = [value; get(r, "value", [])]
        nextlink = get(r, "nextLink", "")
    end
    value, _r
end

# ─── Scaleset queries ───

function list_scalesets(state::ManagerState, ss::ScaleSet)
    _r = @retry state.nretry azrequest(state, "GET", scaleset_list_url(ss), auth(state))
    r = JSON.parse(String(_r.body))
    scalesets, _ = getnextlinks!(state, _r, get(r, "value", []), get(r, "nextLink", ""))
    [get(s, "name", "") for s in scalesets]
end

function list_scaleset_vms(state::ManagerState, ss::ScaleSet)
    local _r
    try
        _r = @retry state.nretry azrequest(state, "GET", scaleset_vms_url(ss), auth(state))
    catch e
        status(e) == 404 && return []
        rethrow()
    end
    r = JSON.parse(String(_r.body))
    vms, _ = getnextlinks!(state, _r, get(r, "value", []), get(r, "nextLink", ""))
    vms
end

function list_scaleset_nics(state::ManagerState, ss::ScaleSet)
    try
        _r = @retry state.nretry azrequest(state, "GET", scaleset_nics_url(ss), auth(state))
        r = JSON.parse(String(_r.body))
        nics, _ = getnextlinks!(state, _r, get(r, "value", []), get(r, "nextLink", ""))
        nic_map = Dict{String,String}()
        for nic in nics
            parts = split(get(nic, "id", ""), '/')
            vm_idx = findfirst(==("virtualMachines"), parts)
            if vm_idx !== nothing && vm_idx < length(parts)
                iid = parts[vm_idx + 1]
                nic_map[iid] = lowercase(get(get(nic, "properties", Dict()), "provisioningState", "unknown"))
            end
        end
        nic_map
    catch e
        @warn "failed to list NICs" scalesetname=ss.name exception=(e, catch_backtrace())
        Dict{String,String}()
    end
end

function scaleset_capacity(state::ManagerState, ss::ScaleSet)
    local r
    try
        _r = @retry state.nretry azrequest(state, "GET", scaleset_detail_url(ss), auth(state))
        r = JSON.parse(String(_r.body))
    catch e
        status(e) == 404 && return 0
        rethrow()
    end
    r["sku"]["capacity"]
end

# ─── Scaleset mutations ───

function scaleset_capacity!(state::ManagerState, ss::ScaleSet, capacity::Int)
    @retry state.nretry azrequest(state, "PATCH", scaleset_detail_url(ss),
        auth_json(state), JSON.json(Dict("sku" => Dict("capacity" => capacity))))
end

function delete_vms(state::ManagerState, ss::ScaleSet, ids)
    isempty(ids) && return
    @retry state.nretry azrequest(state, "POST", scaleset_delete_vms_url(ss),
        auth_json(state), JSON.json(Dict("instanceIds" => ids)))
end

function reimage_vms(state::ManagerState, ss::ScaleSet, ids)
    isempty(ids) && return
    @retry state.nretry azrequest(state, "POST", scaleset_reimage_url(ss),
        auth_json(state), JSON.json(Dict("instanceIds" => ids)))
end

function rmgroup(state::ManagerState, ss::ScaleSet)
    names = list_scalesets(state, ss)
    if ss.name in names
        @retry state.nretry azrequest(state, "DELETE",
            "$(scaleset_url(ss))?forceDeletion=True&api-version=$API_VERSION_COMPUTE_DELETE",
            auth(state))
    end
end
