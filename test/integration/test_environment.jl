include(joinpath(@__DIR__, "common.jl"))

@testset "custom environment propagation" begin
    original_dir = pwd()

    # Create a temporary project directory
    tmpdir = mktempdir()
    try
        cd(tmpdir)
        mkpath("myproject")
        cd("myproject")
        Pkg.activate(".")
        Pkg.add("AzSessions")
        Pkg.add("Distributed")
        Pkg.add("JSON")
        Pkg.add("HTTP")

        # Determine the AzManagers revision for Pkg.add
        parent_manifest = joinpath(dirname(Pkg.project().path), "..", "Manifest.toml")
        azmanagers_rev = ""
        if isfile(parent_manifest)
            pkgs = TOML.parse(read(parent_manifest, String))
            azmanagers_pkg = get(get(pkgs, "deps", Dict()), "AzManagers", [Dict()])[1]
            azmanagers_rev = get(azmanagers_pkg, "repo-rev", "")
        end
        if azmanagers_rev != ""
            Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))
        else
            Pkg.develop(PackageSpec(path=joinpath(@__DIR__, "..", "..")))
        end

        write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")

        # Test with addproc (detached VM)
        @testset "addproc + customenv" begin
            @info "Sub-test 1/2: Detached VM with customenv=true → expect project files propagated"
            testvm = nothing
            try
                testvm = addproc(TEMPLATENAME; basename="testenv-$(randstring('a':'z',4))", session=SESSION, customenv=true, exename=EXENAME)
                testjob = @detachat testvm begin
                    using Pkg
                    pinfo = Pkg.project()
                    write(stdout, "project path is $(dirname(pinfo.path))\n")
                    write(stdout, "$(readdir(dirname(pinfo.path)))")
                end
                wait(testjob)
                testjob_stdout = read(testjob)
                @test contains(testjob_stdout, "myproject")
                @test contains(testjob_stdout, "LocalPreferences.toml")
                @test contains(testjob_stdout, "Manifest.toml")
                @test contains(testjob_stdout, "Project.toml")
            finally
                testvm !== nothing && cleanup_vm(testvm)
            end
        end

        # Test with addprocs (scale set)
        @testset "addprocs + customenv" begin
            group = unique_group()
            @info "Sub-test 2/2: Scale set with customenv=true → expect project path contains 'myproject'" group
            try
                addprocs(AzManager(), TEMPLATENAME, 1; waitfor=true, group=group, session=SESSION, customenv=true, exename=EXENAME, overprovision=false)
                @everywhere using Pkg
                pinfo = remotecall_fetch(Pkg.project, workers()[1])
                @test contains(pinfo.path, "myproject")
            finally
                cleanup_workers()
                wait_for_scaleset_deletion(group)
            end
        end
    finally
        cd(original_dir)
        rm(tmpdir; recursive=true, force=true)
    end
end
