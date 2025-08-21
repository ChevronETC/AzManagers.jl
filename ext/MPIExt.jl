module MPIExt

# using AzManagers, Distributed, Logging, MPI, Sockets
# import AzManagers.azmanager, AzManagers.logerror, AzManagers.azure_worker_init

# #
# # MPI specific methods --
# # These methods are slightly modified versions of what is in the Julia distributed standard library
# #

# function AzManagers.azure_worker_mpi(cookie, master_address, master_port, ppi, exeflags)
#     itry = 0
#     while true
#         itry += 1
#         local c
#         try
#             MPI.Initialized() || MPI.Init()

#             comm = MPI.COMM_WORLD
#             mpi_size = MPI.Comm_size(comm)
#             mpi_rank = MPI.Comm_rank(comm)

#             local t
#             if mpi_rank == 0
#                 c = azure_worker_init(cookie, master_address, master_port, ppi, exeflags, mpi_size)
#                 t = @async start_worker_mpi_rank0(c, cookie)
#             else
#                 t = @async message_handler_loop_mpi_rankN()
#             end

#             MPI.Barrier(comm)
#             fetch(t)
#             MPI.Barrier(comm)
#         catch e
#             @error "error starting worker, attempt $itry, cookie=$cookie, master_address=$master_address, master_port=$master_port, ppi=$ppi"
#             logerror(e, Logging.Error)
#             if itry > 10
#                 throw(e)
#             end
#             if @isdefined c
#                 try
#                     close(c)
#                 catch
#                 end
#             end
#         end
#         sleep(60)
#     end
# end

# function process_messages_mpi_rank0(r_stream::TCPSocket, w_stream::TCPSocket, incoming::Bool=true)
#     @async process_tcp_streams_mpi_rank0(r_stream, w_stream, incoming)
# end

# function process_tcp_streams_mpi_rank0(r_stream::TCPSocket, w_stream::TCPSocket, incoming::Bool)
#     Sockets.nagle(r_stream, false)
#     Sockets.quickack(r_stream, true)
#     Distributed.wait_connected(r_stream)
#     if r_stream != w_stream
#         Sockets.nagle(w_stream, false)
#         Sockets.quickack(w_stream, true)
#         Distributed.wait_connected(w_stream)
#     end
#     message_handler_loop_mpi_rank0(r_stream, w_stream, incoming)
# end

# function message_handler_loop_mpi_rank0(r_stream::IO, w_stream::IO, incoming::Bool)
#     wpid=0          # the worker r_stream is connected to.
#     boundary = similar(Distributed.MSG_BOUNDARY)

#     comm = MPI.Initialized() ? MPI.COMM_WORLD : nothing

#     try
#         version = Distributed.process_hdr(r_stream, incoming)
#         serializer = Distributed.ClusterSerializer(r_stream)

#         # The first message will associate wpid with r_stream
#         header = Distributed.deserialize_hdr_raw(r_stream)
#         msg = Distributed.deserialize_msg(serializer)
#         Distributed.handle_msg(msg, header, r_stream, w_stream, version)
#         wpid = worker_id_from_socket(r_stream)
#         @assert wpid > 0

#         readbytes!(r_stream, boundary, length(Distributed.MSG_BOUNDARY))

#         while true
#             Distributed.reset_state(serializer)
#             header = Distributed.deserialize_hdr_raw(r_stream)
#             # println("header: ", header)

#             try
#                 msg = Distributed.invokelatest(Distributed.deserialize_msg, serializer)
#             catch e
#                 # Deserialization error; discard bytes in stream until boundary found
#                 boundary_idx = 1
#                 while true
#                     # This may throw an EOF error if the terminal boundary was not written
#                     # correctly, triggering the higher-scoped catch block below
#                     byte = read(r_stream, UInt8)
#                     if byte == Distributed.MSG_BOUNDARY[boundary_idx]
#                         boundary_idx += 1
#                         if boundary_idx > length(Distributed.MSG_BOUNDARY)
#                             break
#                         end
#                     else
#                         boundary_idx = 1
#                     end
#                 end

#                 # remotecalls only rethrow RemoteExceptions. Any other exception is treated as
#                 # data to be returned. Wrap this exception in a RemoteException.
#                 remote_err = RemoteException(myid(), CapturedException(e, catch_backtrace()))
#                 # println("Deserialization error. ", remote_err)
#                 if !Distributed.null_id(header.response_oid)
#                     ref = Distributed.lookup_ref(header.response_oid)
#                     put!(ref, remote_err)
#                 end
#                 if !Distributed.null_id(header.notify_oid)
#                     Distributed.deliver_result(w_stream, :call_fetch, header.notify_oid, remote_err)
#                 end
#                 continue
#             end
#             readbytes!(r_stream, boundary, length(Distributed.MSG_BOUNDARY))

#             if comm !== nothing
#                 header = MPI.bcast(header, 0, comm)
#                 msg = MPI.bcast(msg, 0, comm)
#                 version = MPI.bcast(version, 0, comm)
#             end

#             tsk = Distributed.handle_msg(msg, header, r_stream, w_stream, version)

#             if comm !== nothing
#                 wait(tsk) # TODO - this seems needed to not cause a race in the MPI logic, but I'm not sure what the side-effects are.
#                 MPI.Barrier(comm)
#             end
#         end
#     catch e
#         # Check again as it may have been set in a message handler but not propagated to the calling block above
#         if wpid < 1
#             wpid = worker_id_from_socket(r_stream)
#         end

#         if wpid < 1
#             println(stderr, e, CapturedException(e, catch_backtrace()))
#             println(stderr, "Process($(myid())) - Unknown remote, closing connection.")
#         elseif !(wpid in Distributed.map_del_wrkr)
#             werr = Distributed.worker_from_id(wpid)
#             oldstate = werr.state
#             Distributed.set_worker_state(werr, Distributed.W_TERMINATED)

#             # If unhandleable error occurred talking to pid 1, exit
#             if wpid == 1
#                 if isopen(w_stream)
#                     @error "Fatal error on process $(myid())" exception=e,catch_backtrace()
#                 end
#                 exit(1)
#             end

#             # Will treat any exception as death of node and cleanup
#             # since currently we do not have a mechanism for workers to reconnect
#             # to each other on unhandled errors
#             Distributed.deregister_worker(wpid)
#         end

#         isopen(r_stream) && close(r_stream)
#         isopen(w_stream) && close(w_stream)

#         if (myid() == 1) && (wpid > 1)
#             if oldstate != Distributed.W_TERMINATING
#                 println(stderr, "Worker $wpid terminated.")
#                 rethrow()
#             end
#         end

#         return nothing
#     end
# end

# function message_handler_loop_mpi_rankN()
#     comm = MPI.COMM_WORLD
#     header,msg,version = nothing,nothing,nothing
#     while true
#         try
#             header = MPI.bcast(header, 0, comm)
#             msg = MPI.bcast(msg, 0, comm)
#             version = MPI.bcast(version, 0, comm)

#             # ignore the message unless it is of type CallMsg{:call}, CallMsg{:call_fetch}, CallWaitMsg, RemoteDoMsg
#             if typeof(msg) ∈ (Distributed.CallMsg{:call}, Distributed.CallMsg{:call_fetch}, Distributed.CallWaitMsg, Distributed.RemoteDoMsg)
#                 # Cast the call_fetch message to a call method since we only want the fetch from MPI rank 0.
#                 if typeof(msg) ∈ (Distributed.CallMsg{:call_fetch}, Distributed.CallWaitMsg)
#                     msg = Distributed.CallMsg{:call}(msg.f, msg.args, msg.kwargs)
#                 end

#                 tsk = Distributed.handle_msg(msg, header, devnull, devnull, version)
#                 wait(tsk)
#             end

#             MPI.Barrier(comm)
#         catch e
#             @warn "MPI - message_handler_loop_mpi"
#             logerror(e, Logging.Warn)
#         end
#     end
# end

# start_worker_mpi_rank0(cookie::AbstractString=readline(stdin); kwargs...) = start_worker_mpi_rank0(stdout, cookie; kwargs...)
# function start_worker_mpi_rank0(out::IO, cookie::AbstractString=readline(stdin); close_stdin::Bool=true, stderr_to_stdout::Bool=true)
#     Distributed.init_multi()

#     if close_stdin # workers will not use it
#         redirect_stdin(devnull)
#         close(stdin)
#     end
#     stderr_to_stdout && redirect_stderr(stdout)

#     Distributed.init_worker(cookie)
#     interface = IPv4(Distributed.LPROC.bind_addr)
#     if Distributed.LPROC.bind_port == 0
#         port_hint = 9000 + (getpid() % 1000)
#         (port, sock) = listenany(interface, UInt16(port_hint))
#         Distributed.LPROC.bind_port = port
#     else
#         sock = listen(interface, Distributed.LPROC.bind_port)
#     end

#     t = errormonitor(@async while isopen(sock)
#         client = accept(sock)

#         cookie_from_master = read(client, Distributed.HDR_COOKIE_LEN)
#         if cookie_from_master[1] == 0x00
#             error("received cookie with at least one null character")
#         end

#         if String(cookie_from_master) != cookie
#             error("received invalid cookie.")
#         end

#         process_messages_mpi_rank0(client, client, false)
#     end)
#     print(out, "julia_worker:")  # print header
#     print(out, "$(string(Distributed.LPROC.bind_port))#") # print port
#     print(out, Distributed.LPROC.bind_addr)
#     print(out, '\n')
#     flush(out)

#     Sockets.nagle(sock, false)
#     Sockets.quickack(sock, true)

#     if ccall(:jl_running_on_valgrind,Cint,()) != 0
#         println(out, "PID = $(getpid())")
#     end

#     manager = azmanager()
#     manager.worker_socket = out

#     try
#         while true
#             Distributed.check_master_connect()
#             @info "message loop..."
#             wait(t)
#             istaskfailed(t) && fetch(t)
#             sleep(10)
#         end
#     catch e
#         throw(e)
#     finally
#         close(sock)
#     end
# end

end