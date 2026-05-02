function logerror(e, loglevel=Logging.Warn; context...)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")

    for (exc, bt) in current_exceptions()
        showerror(io, exc, bt)
        println(io)
    end
    if isempty(context)
        @logmsg loglevel String(take!(io))
    else
        @logmsg loglevel String(take!(io)) context...
    end
    close(io)
end

const _manifest = Dict("resourcegroup"=>"", "ssh_user"=>"", "ssh_private_key_file"=>"", "ssh_public_key_file"=>"", "subscriptionid"=>"")

manifestpath() = joinpath(homedir(), ".azmanagers")
manifestfile() = joinpath(manifestpath(), "manifest.json")

"""
    AzManagers.write_manifest(;resourcegroup="", subscriptionid="", ssh_user="", ssh_public_key_file="~/.ssh/azmanagers_rsa.pub", ssh_private_key_file="~/.ssh/azmanagers_rsa")

Write an AzManagers manifest file (~/.azmanagers/manifest.json).  The
manifest file contains information specific to your Azure account.
"""
function write_manifest(;
        resourcegroup="",
        subscriptionid="",
        ssh_user="",
        ssh_private_key_file=joinpath(homedir(), ".ssh", "azmanagers_rsa"),
        ssh_public_key_file=joinpath(homedir(), ".ssh", "azmanagers_rsa.pub"))
    manifest = Dict("resourcegroup"=>resourcegroup, "subscriptionid"=>subscriptionid, "ssh_user"=>ssh_user, "ssh_private_key_file"=>ssh_private_key_file, "ssh_public_key_file"=>ssh_public_key_file)
    try
        isdir(manifestpath()) || mkdir(manifestpath(); mode=0o700)
        write(manifestfile(), JSON.json(manifest, 1))
        chmod(manifestfile(), 0o600)
    catch e
        @error "Failed to write manifest file, $(AzManagers.manifestfile())"
        throw(e)
    end
end

function load_manifest()
    if isfile(manifestfile())
        try
            manifest = JSON.parse(read(manifestfile(), String))
            for key in keys(_manifest)
                _manifest[key] = get(manifest, key, "")
            end
        catch e
            throw(ManifestError(manifestfile(), "manifest file is not valid JSON"))
        end
    else
        throw(ManifestError(manifestfile(), "manifest file does not exist — use AzManagers.write_manifest to generate one"))
    end
end

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

function retrywarn(i, retries, s, e)
    if isa(e, HTTP.StatusError)
        @debug "HTTP retry" status=e.status retry=i retries=retries backoff_seconds=round(s, digits=1)
        if e.status == 429
            remaining_resource = ""
            for header in e.response.headers
                if header[1] == "x-ms-ratelimit-remaining-resource"
                    remaining_resource = header[2]
                    break
                end
            end
            @warn "Azure API throttled (429)" retry=i retries=retries backoff_seconds=round(s, digits=1) remaining_resource
            _try_record_throttle()
        elseif e.status == 500
            b = try JSON.parse(String(e.response.body)) catch; Dict() end
            errorcode = get(get(b, "error", Dict()), "code", "")
            @warn "Azure API server error (500)" error_code=errorcode retry=i retries=retries backoff_seconds=round(s, digits=1)
        elseif e.status == 409
            b = try JSON.parse(String(e.response.body)) catch; Dict() end
            errorcode = get(get(b, "error", Dict()), "code", "")
            errormessage = get(get(b, "error", Dict()), "message", "")
            @warn "Azure API conflict (409)" error_code=errorcode message=errormessage retry=i retries=retries backoff_seconds=round(s, digits=1)
        else
            @warn "Azure API error" status=e.status retry=i retries=retries backoff_seconds=round(s, digits=1)
        end
    else
        @warn "retryable error" error_type=string(typeof(e)) retry=i retries=retries backoff_seconds=round(s, digits=1)
        logerror(e, Logging.Debug)
    end
end

# Helper to record throttle metric without requiring manager to be initialized
function _try_record_throttle()
    try
        manager = azmanager()
        isdefined(manager, :metrics) && record_api_throttle!(manager.metrics)
    catch
    end
end

macro retry(retries, ex::Expr)
    quote
        r = nothing
        _retry_count = 0
        for i = 0:$(esc(retries))
            try
                r = $(esc(ex))
                if _retry_count > 0
                    @debug "operation succeeded after retries" retries=_retry_count
                end
                break
            catch e
                if !(i < $(esc(retries)) && isretryable(e))
                    _try_record_api_error()
                    throw(e)
                end
                _retry_count += 1
                _try_record_api_retry()
                maximum_backoff = 256
                s = min(2.0^(i-1), maximum_backoff) + rand()
                if status(e) ∈ (429,500)
                    for header in e.response.headers
                        if lowercase(header[1]) == "retry-after"
                            s = parse(Int, header[2]) + rand()
                            break
                        end
                    end
                end
                retrywarn(i, $(esc(retries)), s, e)
                sleep(s)
            end
        end
        r
    end
end

function _try_record_api_retry()
    try
        manager = azmanager()
        isdefined(manager, :metrics) && record_api_retry!(manager.metrics)
    catch
    end
end

function _try_record_api_error()
    try
        manager = azmanager()
        isdefined(manager, :metrics) && record_api_error!(manager.metrics)
    catch
    end
end

function azrequest(rtype, verbose, url, headers, body=nothing)
    local manager
    try
        manager = azmanager()
    catch
        manager = nothing
    end

    if manager !== nothing && contains(url, "virtualMachineScaleSets")
        lock(manager.lock) do
            if isdefined(manager, :scaleset_request_counter)
                manager.scaleset_request_counter += 1
            else
                manager.scaleset_request_counter = 1
            end
        end
    end

    # Record API call metric
    if manager !== nothing && isdefined(manager, :metrics)
        record_api_call!(manager.metrics)
    end

    options = (retry=false, status_exception=false)
    if body === nothing
        r = HTTP.request(rtype, url, headers; verbose=verbose, options...)
    else
        r = HTTP.request(rtype, url, headers, body; verbose=verbose, options...)
    end
    
    if r.status >= 300
        throw(HTTP.Exceptions.StatusError(r.status, r.request.method, r.request.target, r))
    end
    
    r
end

function scaleset_request_counter()
    manager = azmanager()
    if isdefined(manager, :scaleset_request_counter)
        return manager.scaleset_request_counter
    else
        return 1
    end
end
