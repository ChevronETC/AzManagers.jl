include(joinpath(@__DIR__, "test_utils.jl"))

@testset "AzManagers, addproc, and test if nthreads propagates properly" begin
    @info "[$(elapsed())s] nthreads test: provisioning VM (threads=1,2)..."
    r = randstring('a':'z',4)
    basename = "test$r"
    testvm = addproc(templatename; basename=basename, session=session, julia_num_threads="1,2")
    @info "[$(elapsed())s] nthreads test: VM ready, running detached job..."
    testjob = @detachat testvm begin
        write(stdout, "write to stdout\n")
        write(stderr, "nthreads: $(Threads.nthreads()),$(Threads.nthreads(:interactive))\n")
    end
    with_timeout(()->wait(testjob), 300; msg="wait(testjob)")
    @test read(testjob) == "write to stdout\n"
    @test read(testjob; stdio=stderr) == "nthreads: 1,2\n"
    @info "[$(elapsed())s] nthreads test: cleaning up first VM..."
    with_timeout(()->rmproc(testvm; session=session), 120; msg="rmproc")

    @info "[$(elapsed())s] nthreads test: provisioning VM (reusing name=$basename)..."
    testvm = addproc(templatename, name=basename, session=session)
    @info "[$(elapsed())s] nthreads test: VM ready, running detached job..."
    testjob = @detachat testvm begin
        write(stdout, "write to stdout\n")
        write(stderr, "write to stderr\n")
    end
    with_timeout(()->wait(testjob), 300; msg="wait(testjob)")
    @test read(testjob) == "write to stdout\n"
    @test read(testjob; stdio=stderr) == "write to stderr\n"
    @info "[$(elapsed())s] nthreads test: cleaning up..."
    with_timeout(()->rmproc(testvm; session=session), 120; msg="rmproc")
end

@testset "AzManagers, detach" for kwargs in ( (dummy="dummy"), )

    @info "[$(elapsed())s] detach test: creating persistent detached job..."
    job1 = @detach vm(;vm_template=templatename, session=session, persist=true) begin
        write(stdout, "job1 - stdout string")
        write(stderr, "job1 - stderr string")
    end

    @info "[$(elapsed())s] detach test: sending second job to same server..."
    job2 = @detachat job1.vm begin
        write(stdout, "job2 - stdout string")
        write(stderr, "job2 - stderr string")
    end

    @info "[$(elapsed())s] detach test: waiting for jobs..."
    with_timeout(()->wait(job1), 300; msg="wait(job1)")
    with_timeout(()->wait(job2), 300; msg="wait(job2)")

    @test status(job1) == "done"
    @test read(job1;stdio=stdout) == "job1 - stdout string"
    @test read(job1;stdio=stderr) == "job1 - stderr string"

    @test status(job2) == "done"
    @test read(job2;stdio=stdout) == "job2 - stdout string"
    @test read(job2;stdio=stderr) == "job2 - stderr string"

    @info "[$(elapsed())s] detach test: shutting down detached server..."
    with_timeout(()->rmproc(job1.vm; session=session), 120; msg="rmproc")

    @info "[$(elapsed())s] detach test: creating auto-destruct detached job..."
    job3 = @detach vm(;vm_template=templatename, session=session, persist=false) begin
    end
end

@testset "AzManagers, detach, variablebundle" begin
    @info "[$(elapsed())s] variablebundle test: provisioning VM..."
    r = randstring('a':'z',4)
    basename = "test$r"
    testvm = addproc(templatename; basename=basename, session=session)
    variablebundle!(a=1.0,b=3.14)
    @info "[$(elapsed())s] variablebundle test: running detached job..."
    testjob = @detachat testvm begin
        if variablebundle(:a) ≈ 1.0 && variablebundle(:b) ≈ 3.14
            write(stdout, "passed")
        else
            write(stdout, "failed")
        end
    end
    with_timeout(()->wait(testjob), 300; msg="wait(testjob)")
    @test contains(read(testjob), "passed")
end
