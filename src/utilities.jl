function logerror(e, loglevel=Logging.Info)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")

    for (exc, bt) in current_exceptions()
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

spin(spincount, elapsed_time) = ['◐','◓','◑','◒','✓'][spincount]*@sprintf(" %.2f",elapsed_time)*" seconds"

function software_sanity_check(manager, imagename, custom_environment)
    projectinfo = Pkg.project()
    envpath = normpath(joinpath(projectinfo.path, ".."))
    _packages = TOML.parse(read(joinpath(envpath, "Manifest.toml"), String))

    packages = _packages["deps"]

    if custom_environment
        dev_packages = _detect_dev_packages(read(joinpath(envpath, "Manifest.toml"), String), envpath)
        if !isempty(dev_packages)
            dev_names = join([name for (name, _, _) in dev_packages], ", ")
            error("Project has Pkg.develop'd packages ($dev_names). " *
                  "Workers cannot resolve local paths. Please Pkg.add these packages " *
                  "and push all code changes before using customenv=true.")
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
            for entry in entries
                delete!(entry, "path")
            end
        end
    end
    io = IOBuffer()
    TOML.print(io, manifest)
    String(take!(io))
end

function compress_environment(julia_environment_folder)
    project_text = read(joinpath(julia_environment_folder, "Project.toml"), String)
    manifest_text = read(joinpath(julia_environment_folder, "Manifest.toml"), String)

    manifest_text = sanitize_manifest(manifest_text)

    localpreferences_text = isfile(joinpath(julia_environment_folder, "LocalPreferences.toml")) ? read(joinpath(julia_environment_folder, "LocalPreferences.toml"), String) : ""
    local project_compressed,manifest_compressed,localpreferences_compressed
    with_logger(ConsoleLogger(stdout, Logging.Info)) do
        project_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(project_text)))
        manifest_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(manifest_text)))
        localpreferences_compressed = base64encode(CodecZlib.transcode(ZlibCompressor, Vector{UInt8}(localpreferences_text)))
    end

    project_compressed, manifest_compressed, localpreferences_compressed
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

function add_instance_to_pending_down_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.pending_down, scaleset)
        @debug "pushing worker with id=$instanceid onto pending_down"
        push!(manager.pending_down[scaleset], string(instanceid))
    else
        @debug "creating pending_down vector for id=$instanceid"
        manager.pending_down[scaleset] = Set{String}([string(instanceid)])
    end
    nothing
end

function add_instance_to_pruned_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.pruned, scaleset)
        @debug "pushing worker with id=$instanceid onto pruned"
        push!(manager.pruned[scaleset], string(instanceid))
    else
        @debug "creating pruned vector for id=$instanceid"
        manager.pruned[scaleset] = Set{String}([string(instanceid)])
    end
    nothing
end

function add_instance_to_preempted_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.preempted, scaleset)
        @debug "pushing worker with id=$instanceid onto preempted"
        push!(manager.preempted[scaleset], string(instanceid))
    else
        @debug "creating preempted vector for id=$instanceid"
        manager.preempted[scaleset] = Set{String}([string(instanceid)])
    end
end

function ispreempted(manager::AzManager, config::WorkerConfig)
    u = config.userdata
    scaleset = ScaleSet(u["subscriptionid"], u["resourcegroup"], u["scalesetname"])
    string(u["instanceid"])  ∈ get(manager.preempted, scaleset, Set{String}())
end

function add_instance_to_deleted_list(manager::AzManager, scaleset::ScaleSet, instanceid)
    if haskey(manager.deleted, scaleset)
        @debug "pushing worker with id=$instanceid onto deleted"
        manager.deleted[scaleset][instanceid] = now(Dates.UTC)
    else
        @debug "creating deleted dictionary for id=$instanceid"
        manager.deleted[scaleset] = Dict(instanceid=>now(Dates.UTC))
    end
    nothing
end
