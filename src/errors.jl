"""
Structured error types for AzManagers.jl

All domain-specific exceptions inherit from `AzManagersError` and carry
contextual fields so that callers (and log consumers) can programmatically
inspect failures without parsing log strings.
"""

# ── abstract root ──────────────────────────────────────────────────────
abstract type AzManagersError <: Exception end

# ── Azure REST API errors ──────────────────────────────────────────────
"""
    AzureAPIError

Wraps an HTTP error from the Azure REST API with the parsed error code and
message extracted from the JSON response body.
"""
struct AzureAPIError <: AzManagersError
    status::Int
    error_code::String
    message::String
    operation::String
    cause::Exception
end

function AzureAPIError(e::Exception; operation::String="")
    if e isa HTTP.StatusError
        body = try JSON.parse(String(e.response.body)) catch; Dict() end
        err = get(body, "error", Dict())
        code = get(err, "code", "Unknown")
        msg  = get(err, "message", String(e.response.body))
        return AzureAPIError(e.status, code, msg, operation, e)
    end
    AzureAPIError(999, "Unknown", sprint(showerror, e), operation, e)
end

function Base.showerror(io::IO, e::AzureAPIError)
    print(io, "AzureAPIError($(e.status))")
    !isempty(e.error_code) && print(io, " [$(e.error_code)]")
    !isempty(e.operation)  && print(io, " during '$(e.operation)'")
    print(io, ": $(e.message)")
end

# ── Quota exhausted ───────────────────────────────────────────────────
struct QuotaExhaustedError <: AzManagersError
    subscriptionid::String
    location::String
    family::String
    requested::Int
    available::Int
    retries::Int
end

function Base.showerror(io::IO, e::QuotaExhaustedError)
    print(io, "QuotaExhaustedError: insufficient quota for family '$(e.family)' in $(e.location) — ")
    print(io, "requested $(e.requested) cores, $(e.available) available after $(e.retries) retries")
end

# ── Worker join timeout ───────────────────────────────────────────────
struct WorkerJoinTimeoutError <: AzManagersError
    scaleset::String
    instanceid::String
    timeout_seconds::Float64
end

function Base.showerror(io::IO, e::WorkerJoinTimeoutError)
    print(io, "WorkerJoinTimeoutError: instance '$(e.instanceid)' in scaleset '$(e.scaleset)' ")
    print(io, "failed to join cluster within $(e.timeout_seconds)s")
end

# ── Cloud-init failure ────────────────────────────────────────────────
struct CloudInitError <: AzManagersError
    instanceid::String
    scaleset::String
    vm_state::String
    message::String
end

function Base.showerror(io::IO, e::CloudInitError)
    print(io, "CloudInitError: instance '$(e.instanceid)' in '$(e.scaleset)' — vm_state=$(e.vm_state)")
    !isempty(e.message) && print(io, ": $(e.message)")
end

# ── Scale-set operation error ─────────────────────────────────────────
struct ScaleSetOperationError <: AzManagersError
    operation::String
    scaleset::String
    message::String
    cause::Union{Exception,Nothing}
end

function Base.showerror(io::IO, e::ScaleSetOperationError)
    print(io, "ScaleSetOperationError: $(e.operation) on '$(e.scaleset)' — $(e.message)")
end

# ── Manifest error ────────────────────────────────────────────────────
struct ManifestError <: AzManagersError
    path::String
    message::String
end

function Base.showerror(io::IO, e::ManifestError)
    print(io, "ManifestError: $(e.message) ($(e.path))")
end

# ── Image resolution error ────────────────────────────────────────────
struct ImageResolutionError <: AzManagersError
    sigimagename::String
    sigimageversion::String
    imagename::String
    message::String
end

function Base.showerror(io::IO, e::ImageResolutionError)
    print(io, "ImageResolutionError: $(e.message)")
    !isempty(e.sigimagename) && print(io, " (sigimagename=$(e.sigimagename))")
    !isempty(e.imagename)    && print(io, " (imagename=$(e.imagename))")
end

# ── Detached service errors ──────────────────────────────────────────
struct DetachedServiceError <: AzManagersError
    vm_name::String
    vm_ip::String
    operation::String
    message::String
    cause::Union{Exception,Nothing}
end

function Base.showerror(io::IO, e::DetachedServiceError)
    print(io, "DetachedServiceError: $(e.operation) on '$(e.vm_name)' ($(e.vm_ip)) — $(e.message)")
end
