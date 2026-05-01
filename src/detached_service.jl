#
# detached service and REST API
#
const DETACHED_ROUTER = HTTP.Router()
const DETACHED_JOBS = Dict()
const DETACHED_VM = Ref(Dict())

let DETACHED_ID::Int = 1
    global detached_nextid
    detached_nextid() = (id = DETACHED_ID; DETACHED_ID += 1; id)
end

let DETACHED_PORT::Int = 8081
    global detached_port
    detached_port() = DETACHED_PORT
    global detached_port!
    detached_port!(port) = DETACHED_PORT = port
end

function timestamp_metaformatter(level::Logging.LogLevel, _module, group, id, file, line)
    @nospecialize
    timestamp = Dates.format(now(Dates.UTC), "yyyy-mm-ddTHH:MM:SS")
    color = Logging.default_logcolor(level)
    prefix = timestamp*" - "*(level == Logging.Warn ? "Warning" : string(level))*':'
    suffix = ""
    color, prefix, suffix
end

function detachedservice(address=ip"0.0.0.0"; server=nothing, subscriptionid="", resourcegroup="", vmname="", exename="julia")
    HTTP.register!(DETACHED_ROUTER, "POST", "/cofii/detached/run", detachedrun)
    HTTP.register!(DETACHED_ROUTER, "POST", "/cofii/detached/job/*/kill", detachedkill)
    HTTP.register!(DETACHED_ROUTER, "POST", "/cofii/detached/job/*/wait", detachedwait)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/status", detachedstatus)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/stdout", detachedstdout)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/job/*/stderr", detachedstderr)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/ping", detachedping)
    HTTP.register!(DETACHED_ROUTER, "GET", "/cofii/detached/vm", detachedvminfo)

    port = detached_port()

    vm_sn = azure_physical_name()

    AzManagers.DETACHED_VM[] = Dict("subscriptionid"=>string(subscriptionid), "resourcegroup"=>string(resourcegroup),
        "name"=>string(vmname), "ip"=>string(getipaddr()), "port"=>string(port), "physical_hostname" => vm_sn, "exename"=>string(exename))

    global_logger(ConsoleLogger(stdout, Logging.Info; meta_formatter=timestamp_metaformatter))

    HTTP.serve(DETACHED_ROUTER, address, port; server=server)
end

function detachedrun(request::HTTP.Request)
    @info "inside detachedrun"
    local process, id, pid, r

    try
        r = JSON.parse(String(HTTP.payload(request)))

        if !haskey(r, "code")
            return HTTP.Response(400, ["Content-Type"=>"application/json"], JSON.json(Dict("error"=>"Malformed body: JSON body must contain the key: code")); request)
        end

        exename = get(r, "exename", "julia")

        _tempname_logging = tempname(;cleanup=false)
        write(_tempname_logging, """using Logging; global_logger(ConsoleLogger(stdout, Logging.Info))""")

        _tempname_varbundle = tempname(;cleanup=false)
        if haskey(r, "variablebundle")
            write(_tempname_varbundle, """using AzManagers, Base64, Serialization; variablebundle!(deserialize(IOBuffer(base64decode("$(r["variablebundle"])"))))\n""")
        else
            write(_tempname_varbundle, "\n")
        end

        code = r["code"]
        codelines = split(code, "\n")

        if strip(codelines[1]) == "begin"
            popfirst!(codelines)
            while length(codelines) > 0
                occursin("end", pop!(codelines)) && break
            end
        end
        if length(codelines) == 0
            return HTTP.Response(400, ["Content-Type"=>"application/json"], JSON.json(Dict("error"=>"No code to execute, missing end?", "code"=>code)); request)
        end

        code = join(codelines, "\n")

        _tempname = tempname(;cleanup=false)
        write(_tempname, code)

        id = detached_nextid()
        outfile = "job-$id.out"
        errfile = "job-$id.err"
        wrapper_code = """
        __detached_id() = $id
        open("$outfile", "w") do out
            open("$errfile", "w") do err
                redirect_stdout(out) do
                    redirect_stderr(err) do
                        include("$_tempname_logging")
                        include("$_tempname_varbundle")
                        try
                            include("$_tempname")
                        catch e
                            for (exc, bt) in Base.catch_stack()
                                showerror(stderr, exc, bt)
                                println(stderr)
                            end
                            write(stderr, "\\n\\n")
                            title = "Code listing ($_tempname)"
                            write(stderr, title*"\\n")
                            nlines = countlines("$_tempname")
                            pad = nlines > 0 ? floor(Int,log10(nlines)) : 0
                            for (iline,line) in enumerate(readlines("$_tempname"))
                                write(stderr, "\$(lpad(iline,pad)): \$line\\n")
                            end
                            write(stderr, "\\n")
                            flush(stderr)
                            throw(e)
                        end
                    end
                end
            end
        end
        """

        _tempname_wrapper = tempname(;cleanup=false)
        write(_tempname_wrapper, wrapper_code)

        nthreads, nthreads_interactive = Threads.nthreads(), Threads.nthreads(:interactive)
        julia_num_threads = nthreads_filter("$(Threads.nthreads()),$(Threads.nthreads(:interactive))")
        projectdir = dirname(Pkg.project().path)
        exename_parts = split(exename)
        cmd = pipeline(Cmd(vcat(exename_parts, ["-t", julia_num_threads, "--project=$projectdir", _tempname_wrapper])))
        process = open(cmd)
        pid = getpid(process)
        @info "executing $_tempname_wrapper with executable '$exename', $nthreads Julia threads, environment '$projectdir', and pid $pid"

        DETACHED_JOBS[string(id)] = Dict("process"=>process, "request"=>request, "stdout"=>outfile, "stderr"=>errfile, "codefile"=>_tempname, "code"=>code)
    catch e
        io = IOBuffer()
        @error "caught error in detachedrun"
        logerror(e, Logging.Debug)
        return HTTP.Response(500, ["Content-Type"=>"application/json"], JSON.json(Dict("error"=>String(take!(io)))); request)
    end

    Threads.@spawn begin
        try
            wait(process)
        catch
        end
        if !r["persist"]
            vm = AzManagers.DETACHED_VM[]
            rmproc(vm; session=sessionbundle(:management))
        end
    end
    HTTP.Response(200, ["Content-Type"=>"application/json"], JSON.json(Dict("id"=>id, "pid"=>pid)); request)
end

function detachedkill(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    local _process
    try
        _process = DETACHED_JOBS[string(id)]["process"]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "unable to find process id in job $id"; request)
    end

    local response
    try
        kill(_process)
        response = HTTP.Response(200, ["Content-Type"=>"application/text"], "process for job $id killed"; request)
    catch
        response = HTTP.Response(500, ["Content-Type"=>"application/text"], "error deleting process id for job $id"; request)
    end
    response
end

function detachedstatus(request::HTTP.Request)
    @info "inside detachedstatus"
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Job with id=$id does not exist."; request)
    end

    local status
    try
        process = DETACHED_JOBS[id]["process"]

        if process_exited(process)
            status = success(process) ? "done" : "failed"
        elseif process_running(process)
            status = "running"
        else
            status = "starting"
        end
    catch e
        return HTTP.Response(500, ["Content-Type"=>"application/json"], JSON.json(Dict("error"=>show(e), "trace"=>show(stacktrace()))); request)
    end
    HTTP.Response(200, ["Content-Type"=>"application/json"], JSON.json(Dict("id"=>id, "status"=>status)); request)
end

function detachedstdout(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Job with id=$id does not exist."; request)
    end

    local stdout
    if isfile(DETACHED_JOBS[id]["stdout"])
        stdout = read(DETACHED_JOBS[id]["stdout"])
    else
        stdout = ""
    end
    HTTP.Response(200, ["Content-Type"=>"application/text"], stdout; request)
end

function detachedstderr(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(500, ["Content-Type"=>"application/text"], "ERROR: Job with id=$id does not exist."; request)
    end

    local stderr
    if isfile(DETACHED_JOBS[id]["stderr"])
        stderr = read(DETACHED_JOBS[id]["stderr"])
    else
        stderr = ""
    end

    HTTP.Response(200, ["Content-Type"=>"application/text"], stderr; request)
end

function detachedwait(request::HTTP.Request)
    local id
    try
        id = split(request.target, '/')[5]
    catch
        return HTTP.Response(400, ["Content-Type"=>"application/text"], "ERROR: Unable to find job id."; request)
    end

    if !haskey(DETACHED_JOBS, id)
        return HTTP.Response(400, ["Content-Type"=>"application/string"], "ERROR: Job with id=$id does not exist."; request)
    end

    try
        process = DETACHED_JOBS[id]["process"]
        wait(process)
    catch e
        @error "caught error waiting for process for job $id to finish"
        logerror(e, Logging.Debug)

        write(io, "\n\n")

        title = "Code listing ($(DETACHED_JOBS[id]["codefile"]))"
        write(io, title*"\n")
        write(io, "-"^length(title)*"\n")
        lines = split(DETACHED_JOBS[id]["code"])
        nlines = length(lines)
        pad = nlines > 0 ? floor(Int,log10(nlines)) : 0
        for (iline,line) in enumerate(lines)
            write(io, "$(lpad(iline,pad)): $line\n")
        end
        write(io, "\n")

        return HTTP.Response(400, ["Content-Type"=>"application/json"], JSON.json(Dict("error"=>String(take!(io)))); request)
    end
    HTTP.Response(200, ["Content-Type"=>"application/text"], "OK, job $id is finished"; request)
end

function detachedping(request::HTTP.Request)
    HTTP.Response(200, ["Content-Type"=>"applicaton/text"], "OK"; request)
end

function detachedvminfo(request::HTTP.Request)
    HTTP.Response(200, ["Content-Type"=>"application/json"], JSON.json(AzManagers.DETACHED_VM[]); request)
end
