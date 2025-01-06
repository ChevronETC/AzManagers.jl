using Distributed, AzManagers, AzSessions

#
# create a session-bundle that will be sent to the detached job.
#
sessionbundle!(
    management = AzSession())

#
# create a detached job, and persist the server
#
job1 = @detach vm(;session=sessionbundle(:management),persist=true) begin
    write(stdout, "some stdout stuff, 1\n")
    write(stderr, "some stderr stuff, 1\n")
    @info "write a message using Logging, 1"
end

status(job1)
read(job1)
read(job1;stdio=stderr)

#
# send a new job to the server started above
#
job2 = @detachat job1.vm["ip"] begin
    write(stdout, "some stdout stuff, 2\n")
    write(stderr, "some stderr stuff, 2\n")
    @info "write a message using Logging, 2"
end

#
# get job status
#
status(job1)
status(job2)

#
# wait for jobs to finish
#
wait(job1)
wait(job2)

#
# get stdout from each job
#
write(stdout, read(job1))
write(stdout, read(job2))

#
# shut-down the detached server
#
rmproc(job1.vm; session=sessionbundle(:management))

#
# create a new job on a new detached server that auto-destructs upon completion of its work
#
job3 = @detach vm(;session=sessionbundle(:management),persist=false) begin
    @info "running detached stuff, 3"
end

#
# more involved example with storage and scale-sets
#
sessionbundle!(
    management = AzSession(),
    storage = AzSession(;scope="openid+offline_access+https://storage.azure.com/user_impersonation"))

job4 = @detach vm(;session=sessionbundle(:management),persist=true) begin
    using Distributed, AzManagers, AzStorage, AzSessions
    using AzManagers: addprocs_azure
    addprocs_azure("cbox04", 4; session=sessionbundle(:management))

    for pid in workers()
        hostname = remotecall_fetch(gethostname, pid)
        @info "pid=$pid, hostname=$hostname"
    end

    using Random
    x = randstring('a':'z',4)
    container = AzContainer("foobar-$x"; storageaccount="samkaplan0", session=sessionbundle(:storage))
    mkpath(container)
    write(container, "foobar-$x", "hello")
    @info "contents=$(read(container, "foobar-$x", String))"
    rm(container)

    rmprocs(workers())
end

status(job4)
wait(job4)
read(job4)

rmproc(job4.vm; session=sessionbundle(:management))
