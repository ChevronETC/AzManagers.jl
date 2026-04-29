## Debug version of test_environment.jl — sub-test 1 only (detached VM + customenv)
## Dumps the generated startup script, extends timeout, and SSHes into the VM on failure
## to inspect cloud-init logs and Julia state.

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

ENV["JULIA_DEBUG"] = "AzManagers"

using AzManagers, AzSessions, Distributed, TOML, HTTP, JSON, Random, Test

include(joinpath(@__DIR__, "integration", "common.jl"))

# --- Dump sanitized manifest and dev package detection ---
function dump_environment_debug()
    try
        projectinfo = Pkg.project()
        envpath = normpath(joinpath(projectinfo.path, ".."))
        manifest_text = read(joinpath(envpath, "Manifest.toml"), String)

        # Show detected dev'd packages
        dev_pkgs = AzManagers._detect_dev_packages(manifest_text, envpath)
        if !isempty(dev_pkgs)
            @info "=== DETECTED DEV'D PACKAGES ==="
            for (name, url, rev) in dev_pkgs
                println("  $name → $url#$rev")
            end
        else
            @info "No dev'd packages detected"
        end

        # Show sanitized manifest (after dev removal)
        sanitized = AzManagers.sanitize_manifest(manifest_text)
        @info "=== SANITIZED MANIFEST (AzManagers entry should be GONE) ==="
        m = TOML.parse(sanitized)
        if haskey(m, "deps") && haskey(m["deps"], "AzManagers")
            @warn "AzManagers still in sanitized manifest — this is the bug!"
            println(TOML.print(stdout, Dict("AzManagers" => m["deps"]["AzManagers"])))
        else
            @info "✓ AzManagers correctly removed from sanitized manifest"
        end

        # Show what compress_environment produces
        project_text = read(joinpath(envpath, "Project.toml"), String)
        dev_pkg_names = [name for (name, _, _) in dev_pkgs]
        sanitized_project = AzManagers.sanitize_project(project_text, dev_pkg_names)
        @info "=== SANITIZED PROJECT.toml ==="
        println(sanitized_project)

        # Show the Pkg.add calls that will be injected
        dev_pkg_adds = ""
        for (name, url, rev) in dev_pkgs
            dev_pkg_adds *= """Pkg.add(url="$url", rev="$rev"); """
        end
        @info "=== DEV PKG ADDS (injected into worker cloud-init) ==="
        println(dev_pkg_adds)
    catch e
        @warn "Could not dump environment debug info" exception=e
    end
end

# --- SSH helpers + diagnostics ---
function ssh_cmd(ip, remote_cmd)
    keyfile = expanduser("~/.ssh/azmanagers_rsa")
    full_cmd = "ssh -i $keyfile -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o UserKnownHostsFile=/dev/null cvx@$ip '$(replace(remote_cmd, "'" => "'\\''"))'"
    read(Cmd(["sh", "-c", full_cmd]), String)
end

function ssh_diagnose(vm)
    ip = vm["ip"]
    @info "=== SSH DIAGNOSTICS for $(vm["name"]) @ $ip ==="
    
    cmds = [
        "cloud-init status"                             => "Cloud-init status",
        "tail -100 /var/log/cloud-init-output.log"      => "Cloud-init output (last 100 lines)",
        "grep -i error /var/log/cloud-init.log | tail -20" => "Cloud-init errors",
        "ls -la /home/cvx/.julia/environments/ 2>/dev/null || echo 'no environments dir'" => "Julia environments dir",
        "ls -la /home/cvx/.julia/environments/myproject/ 2>/dev/null || echo 'no myproject env'" => "myproject env dir",
        "cat /home/cvx/.julia/environments/myproject/Project.toml 2>/dev/null || echo 'no Project.toml'" => "Worker Project.toml",
        "head -60 /home/cvx/.julia/environments/myproject/Manifest.toml 2>/dev/null || echo 'no Manifest.toml'" => "Worker Manifest.toml (first 60 lines)",
        "ps aux | grep julia"                           => "Julia processes",
        "which julia && julia --version"                => "Julia binary",
    ]
    
    for (cmd, label) in cmds
        @info "--- $label ---"
        try
            result = ssh_cmd(ip, cmd)
            println(result)
        catch e
            @warn "SSH command failed: $label" exception=e
        end
    end
end

function debug_detached_service_wait(vm, custom_environment; timeout=180.0)
    starttime = time()
    elapsed_time = 0.0
    tic = starttime - 20
    spincount = 1
    waitfor = custom_environment ? "Julia package instantiation and COFII detached service" : "COFII detached service"
    
    # Log periodic SSH checks while waiting
    last_ssh_check = 0.0
    
    while true
        if time() - tic > 5
            try
                r = HTTP.request("GET", "http://$(vm["ip"]):$(vm["port"])/cofii/detached/ping")
                @info "Detached service is UP!"
                return
            catch
                tic = time()
            end
        end

        elapsed_time = time() - starttime
        
        # Every 30s, do a quick SSH check on cloud-init status
        if elapsed_time - last_ssh_check > 30
            last_ssh_check = elapsed_time
            @info "Periodic check at $(round(elapsed_time, digits=1))s..."
            try
                status = ssh_cmd(vm["ip"], "cloud-init status")
                @info "cloud-init status" status=strip(status)
            catch
                @info "SSH not ready yet"
            end
            try
                ps = ssh_cmd(vm["ip"], "ps aux | grep julia")
                @info "julia processes" ps=strip(ps)
            catch; end
        end

        if elapsed_time > timeout
            @error "TIMEOUT ($timeout seconds) waiting for $waitfor"
            ssh_diagnose(vm)
            error("DetachedServiceTimeout after $(timeout)s — see SSH diagnostics above")
        end
        
        write(stdout, "⏳ $(round(elapsed_time, digits=1))s waiting for $waitfor on $(vm["name"]):$(vm["port"])\r")
        flush(stdout)
        spincount = spincount == 4 ? 1 : spincount + 1
        sleep(0.5)
    end
end

# --- Main test ---
println("\n" * "="^80)
println("DEBUG test_environment.jl — sub-test 1 only (detached VM + customenv)")
println("="^80 * "\n")

original_dir = pwd()
tmpdir = mktempdir()
vmname = "dbgenv-$(randstring('a':'z', 4))"
testvm = nothing

try
    cd(tmpdir)
    mkpath("myproject")
    cd("myproject")
    Pkg.activate(".")
    @info "Adding packages to temp project..."
    Pkg.add("AzSessions")
    Pkg.add("Distributed")
    Pkg.add("JSON")
    Pkg.add("HTTP")
    
    # Use Pkg.develop since we're local
    @info "Pkg.develop'ing AzManagers from source..."
    Pkg.develop(PackageSpec(path=joinpath(@__DIR__, "..")))
    
    write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")
    
    @info "=== Local Project.toml ==="
    println(read("Project.toml", String))
    
    @info "=== Local Manifest.toml (AzManagers entry) ==="
    manifest = TOML.parsefile("Manifest.toml")
    if haskey(manifest, "deps") && haskey(manifest["deps"], "AzManagers")
        for entry in manifest["deps"]["AzManagers"]
            println(TOML.print(stdout, Dict("AzManagers" => [entry])))
        end
    end
    
    @info "=== ENVIRONMENT DEBUG (what workers will see) ==="
    dump_environment_debug()
    
    @info "Creating detached VM with customenv=true..." vmname
    global testvm = addproc(TEMPLATENAME;
        basename=vmname,
        session=SESSION,
        customenv=true,
        exename=EXENAME,
        detachedservice=false  # Don't wait for detached service — we'll do our own wait
    )
    
    @info "VM created successfully" testvm
    
    # Now do our own wait with debug diagnostics
    debug_detached_service_wait(testvm, true; timeout=180.0)
    
    @info "Service is up! Running detached job..."
    testjob = @detachat testvm begin
        using Pkg
        pinfo = Pkg.project()
        write(stdout, "project path is $(dirname(pinfo.path))\n")
        write(stdout, "$(readdir(dirname(pinfo.path)))\n")
    end
    wait(testjob)
    testjob_stdout = read(testjob)
    @info "Detached job output:" testjob_stdout
    
    @test contains(testjob_stdout, "myproject")
    @test contains(testjob_stdout, "LocalPreferences.toml")
    @test contains(testjob_stdout, "Manifest.toml")
    @test contains(testjob_stdout, "Project.toml")

catch e
    @error "Test failed" exception=(e, catch_backtrace())
    
    # If VM exists, try to SSH in for diagnostics regardless
    if testvm !== nothing
        try
            ssh_diagnose(testvm)
        catch ssh_e
            @warn "Post-failure SSH diagnostics also failed" exception=ssh_e
        end
    end
finally
    @info "Cleaning up..."
    if testvm !== nothing
        try
            cleanup_vm(testvm)
        catch e
            @warn "VM cleanup failed" exception=e
            # Try direct az cli
            try
                run(`az vm delete --name $(testvm["name"]) --resource-group $RESOURCEGROUP --subscription $SUBSCRIPTIONID --yes --no-wait`)
            catch; end
        end
    end
    cd(original_dir)
    rm(tmpdir; recursive=true, force=true)
end
