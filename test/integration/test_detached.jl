include(joinpath(@__DIR__, "common.jl"))

@testset "detached jobs" begin
    # --- @detachat tests: reuse a single VM for multiple sub-tests ---
    testvm = nothing
    @info "Detached VM: provisioning single VM for sub-tests (nthreads, stdout/stderr, status, variablebundle)"
    try
        basename = "testdet-$(randstring('a':'z',4))"
        testvm = addproc(TEMPLATENAME; basename=basename, session=SESSION, julia_num_threads="1,2", exename=EXENAME)

        @testset "nthreads propagation" begin
            testjob = @detachat testvm begin
                write(stdout, "write to stdout\n")
                write(stderr, "nthreads: $(Threads.nthreads()),$(Threads.nthreads(:interactive))\n")
            end
            wait(testjob)
            @test read(testjob) == "write to stdout\n"
            @test read(testjob; stdio=stderr) == "nthreads: 1,2\n"
        end

        @testset "stdout and stderr" begin
            testjob = @detachat testvm begin
                write(stdout, "hello stdout")
                write(stderr, "hello stderr")
            end
            wait(testjob)
            @test read(testjob) == "hello stdout"
            @test read(testjob; stdio=stderr) == "hello stderr"
        end

        @testset "job status" begin
            testjob = @detachat testvm begin
                write(stdout, "done")
            end
            wait(testjob)
            @test status(testjob) == "done"
        end

        @testset "variablebundle" begin
            variablebundle!(a=1.0, b=3.14)
            testjob = @detachat testvm begin
                if variablebundle(:a) ≈ 1.0 && variablebundle(:b) ≈ 3.14
                    write(stdout, "passed")
                else
                    write(stdout, "failed")
                end
            end
            wait(testjob)
            @test contains(read(testjob), "passed")
        end
    finally
        testvm !== nothing && cleanup_vm(testvm)
    end

    # --- @detach tests: VM provisioning + job in one macro call ---
    @testset "@detach with persist=true" begin
        @info "@detach persist=true: provision VM, run 2 jobs on same VM → expect stdout/stderr and status"
        job1 = nothing
        try
            job1 = @detach vm(;vm_template=TEMPLATENAME, session=SESSION, persist=true, exename=EXENAME, basename="detpr") begin
                write(stdout, "detach-job1-stdout")
                write(stderr, "detach-job1-stderr")
            end
            wait(job1)

            @test status(job1) == "done"
            @test read(job1) == "detach-job1-stdout"
            @test read(job1; stdio=stderr) == "detach-job1-stderr"

            # Send a second job to the persisted VM
            job2 = @detachat job1.vm begin
                write(stdout, "detach-job2-stdout")
            end
            wait(job2)
            @test read(job2) == "detach-job2-stdout"
        finally
            job1 !== nothing && cleanup_vm(job1.vm)
        end
    end

    @testset "@detach with persist=false" begin
        @info "@detach persist=false: provision VM, run job → expect auto-cleanup"
        job3 = nothing
        try
            job3 = @detach vm(;vm_template=TEMPLATENAME, session=SESSION, persist=false, exename=EXENAME, basename="detnp") begin
                write(stdout, "auto-destruct")
            end
            wait(job3)
            @test status(job3) == "done"
            @test read(job3) == "auto-destruct"
        finally
            # Safety cleanup in case persist=false auto-delete doesn't work
            job3 !== nothing && cleanup_vm(job3.vm)
        end
    end
end
