ENV["JULIA_DEBUG"] = "AzManagers"
using Distributed, AzManagers, AzSessions, JSON, Logging, Random, HTTP

session = AzSession(;protocal=AzClientCredentials)
templatename = get(ENV, "AZMANAGERS_TEST_TEMPLATE", "cbox02")
exename = get(ENV, "AZMANAGERS_TEST_EXENAME", "julia")

function timed_rmprocs(wrkrs; timeout=30)
    tsk = @async try rmprocs(wrkrs) catch end
    t0 = time()
    while !istaskdone(tsk) && (time() - t0) < timeout
        sleep(0.5)
    end
    if !istaskdone(tsk)
        @warn "rmprocs timed out after $(timeout)s — skipping"
    else
        @info "rmprocs completed in $(round(time()-t0, digits=1))s"
    end
end

function timed_rmgroup(group; session, timeout=30)
    tsk = @async try AzManagers.rmgroup(group; session=session) catch end
    t0 = time()
    while !istaskdone(tsk) && (time() - t0) < timeout
        sleep(0.5)
    end
    if !istaskdone(tsk)
        @warn "rmgroup timed out after $(timeout)s — skipping"
    else
        @info "rmgroup completed in $(round(time()-t0, digits=1))s"
    end
end

# --- Sub-test 1: Non-spot with explicit threads ---
group = "testdbg" * randstring('a':'z', 4)
@info "=== Sub-test 1: Non-spot, threads=2,0, group=$group ==="
try
    addprocs(AzManager(), templatename, 1;
        waitfor=true, group=group, session=session,
        julia_num_threads="2,0", exename=exename, overprovision=false)

    w = workers()[1]
    ndefault = remotecall_fetch(Threads.nthreads, w)
    ninteractive = remotecall_fetch(Threads.nthreads, w, :interactive)
    @info "Worker $w: default=$ndefault, interactive=$ninteractive"
    @info "Expected: default=2, interactive=0 (may be 1 on Julia ≥1.12)"
catch e
    @error "Sub-test 1 failed" exception=(e, catch_backtrace())
end
@info "Cleaning up sub-test 1..."
timed_rmprocs(workers())
timed_rmgroup(group; session)
@info "Sub-test 1 done"

# --- Sub-test 2: Spot with threads=2,0 ---
group = "testdbg" * randstring('a':'z', 4)
@info "=== Sub-test 2: Spot, threads=2,0, group=$group ==="
try
    addprocs(AzManager(), templatename, 1;
        waitfor=true, group=group, session=session,
        julia_num_threads="2,0", spot=true, exename=exename, overprovision=false)

    w = workers()[1]
    ndefault = remotecall_fetch(Threads.nthreads, w)
    ninteractive = remotecall_fetch(Threads.nthreads, w, :interactive)
    @info "Worker $w: default=$ndefault, interactive=$ninteractive"
    @info "Expected: default=2, interactive=1 (spot auto-adds interactive thread)"
catch e
    @error "Sub-test 2 failed" exception=(e, catch_backtrace())
end
@info "Cleaning up sub-test 2..."
timed_rmprocs(workers())
timed_rmgroup(group; session)
@info "Sub-test 2 done"

# --- Sub-test 3: Spot with explicit interactive threads ---
group = "testdbg" * randstring('a':'z', 4)
@info "=== Sub-test 3: Spot, threads=3,2, group=$group ==="
try
    addprocs(AzManager(), templatename, 1;
        waitfor=true, group=group, session=session,
        julia_num_threads="3,2", spot=true, exename=exename, overprovision=false)

    w = workers()[1]
    ndefault = remotecall_fetch(Threads.nthreads, w)
    ninteractive = remotecall_fetch(Threads.nthreads, w, :interactive)
    @info "Worker $w: default=$ndefault, interactive=$ninteractive"
    @info "Expected: default=3, interactive=2"
catch e
    @error "Sub-test 3 failed" exception=(e, catch_backtrace())
end
@info "Cleaning up sub-test 3..."
timed_rmprocs(workers())
timed_rmgroup(group; session)
@info "Sub-test 3 done"

@info "=== All sub-tests complete ==="
