function logerror(e, loglevel=Logging.Info)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")

    my_catch_stack = VERSION < v"1.7" ? Base.catch_stack : current_exceptions

    for (exc, bt) in my_catch_stack()
        showerror(io, exc, bt)
        println(io)
    end
    @logmsg loglevel String(take!(io))
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
            @error "Manifest file ($(AzManagers.manifestfile())) is not valid JSON"
            throw(e)
        end
    else
        @error "Manifest file ($(AzManagers.manifestfile())) does not exist.  Use AzManagers.write_manifest to generate a manifest file."
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
    if isa(e, HTTP.ExceptionRequest.StatusError)
        @debug "$(e.status): $(String(e.response.body)), retry $i of $retries, retrying in $s seconds"
        if e.status == 429
            remaining_resource = nothing
            for header in e.response.headers
                if header[1] == "x-ms-ratelimit-remaining-resource"
                    remaining_resource = header
                    break
                end
            end
            @warn "The Azure service is throttling the request, asking to retry after $s seconds.  Quota information:" remaining_resource[2]
        elseif e.status == 500
            b = JSON.parse(String(e.response.body))
            errorcode = get(get(b, "error", Dict()), "code", "")
            @warn "errorcode: $errorcode, retry $i, retrying in $s seconds"
        elseif e.status == 409
            b = JSON.parse(String(e.response.body))
            errorcode = get(get(b, "error", Dict()), "code", "")
            errormessage = get(get(b, "error", Dict()), "message", "")
            @warn "($errorcode): $errormessage; retry $i of $retries, retrying in $s seconds"
        else
            @warn "status=$(e.status): $(String(e.response.body)), retry $i of $retries, retrying in $s seconds"
        end
    else
        @warn "warn: $(typeof(e)) -- retry $i, retrying in $s seconds"
        logerror(e, Logging.Debug)
    end
end

macro retry(retries, ex::Expr)
    quote
        r = nothing
        for i = 0:$(esc(retries))
            try
                r = $(esc(ex))
                break
            catch e
                (i < $(esc(retries)) && isretryable(e)) || throw(e)
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
