ENV["JULIA_DEBUG"] = "AzManagers"
using Distributed, AzManagers, AzSessions, JSON, Logging, Random, HTTP

session = AzSession(;protocal=AzClientCredentials)

group = "testdbg" * randstring('a':'z', 4)
@info "Starting addprocs with group=$group"

addprocs(AzManager(), "cbox02", 4;
    waitfor=true, ppi=1, group=group, session=session, exename="julia", overprovision=true)

@info "Workers: $(nworkers())"
@info "Cleaning up"
try rmprocs(workers()) catch end
try AzManagers.rmgroup(group; session=session) catch end
