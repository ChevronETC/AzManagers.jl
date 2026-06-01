"""
    SocketTopology(id, cpu_set)

One physical CPU socket (package) detected on a worker VM. `id` is the OS
socket index (0-based on Linux); `cpu_set` lists the physical CPU IDs that
belong to this socket, sorted ascending.
"""
struct SocketTopology
    id::Int
    cpu_set::Vector{Int}
end

"""
    NumaTopology(id, cpu_set, socket)

One NUMA node detected on a worker VM. `id` is the OS NUMA node index,
`cpu_set` is the sorted physical CPU IDs in that node, and `socket` is the
parent socket id (or `nothing` if the node straddles sockets or no socket
could be matched).
"""
struct NumaTopology
    id::Int
    cpu_set::Vector{Int}
    socket::Union{Int,Nothing}
end

"""
    MachineTopology(physical_cores, logical_cpus, sockets, numa_nodes, hyperthreading)

Complete CPU topology of a single VM, as produced by
[`detect_machine_topology`](@ref). `physical_cores` is the total physical
core count, `logical_cpus` enumerates every logical CPU (including SMT
siblings), and `sockets` / `numa_nodes` describe the package/NUMA layout.
`hyperthreading` is `true` when more than one logical CPU maps to a
physical core.
"""
struct MachineTopology
    physical_cores::Int
    logical_cpus::Vector{Int}
    sockets::Vector{SocketTopology}
    numa_nodes::Vector{NumaTopology}
    hyperthreading::Bool
end

"""
    WorkerPlacement(localid, cpu_set, numa_node, socket,
                    julia_threads, julia_interactive_threads, omp_threads)

Per-worker CPU assignment computed by [`plan_worker_placements`](@ref).
`localid` is the 1-based index of the worker on its VM. `cpu_set` is the
sorted physical CPU IDs the worker owns; `numa_node` / `socket` are the
enclosing topology domain ids when the CPU set fits inside exactly one of
them (otherwise `nothing`). The thread fields drive `JULIA_NUM_THREADS`
and `OMP_NUM_THREADS` for the worker process.
"""
struct WorkerPlacement
    localid::Int
    cpu_set::Vector{Int}
    numa_node::Union{Int,Nothing}
    socket::Union{Int,Nothing}
    julia_threads::Int
    julia_interactive_threads::Int
    omp_threads::Int
end

"""
    MpiRankPlacement(worker_localid, rank_index, cpu_set,
                     numa_node, socket, omp_threads)

CPU assignment for one MPI rank that belongs to a Julia worker.
`worker_localid` matches the parent `WorkerPlacement.localid`, `rank_index`
is the 0-based rank inside the worker's `mpirun`, and `cpu_set` is the
contiguous CPU slice this rank pins to (a subset of the worker's CPU set).
"""
struct MpiRankPlacement
    worker_localid::Int
    rank_index::Int
    cpu_set::Vector{Int}
    numa_node::Union{Int,Nothing}
    socket::Union{Int,Nothing}
    omp_threads::Int
end

"""
    parse_cpu_list(value) -> Vector{Int}

Parse an `lscpu` / cgroup-style CPU list (e.g. `"0-3,7,12-15"`) into a
sorted, deduplicated vector of CPU IDs. Throws `ArgumentError` if a comma-
separated entry is not a single integer or `N-M` range.
"""
function parse_cpu_list(value::AbstractString)
    cpus = Int[]
    for part in split(value, ',')
        cpu_range = split(strip(part), '-')
        if length(cpu_range) == 1
            push!(cpus, parse(Int, cpu_range[1]))
        elseif length(cpu_range) == 2
            append!(cpus, parse(Int, cpu_range[1]):parse(Int, cpu_range[2]))
        else
            throw(ArgumentError("invalid CPU list: $value"))
        end
    end
    sort(unique(cpus))
end

"""
    parse_lscpu_json_topology(text) -> MachineTopology

Parse the output of `lscpu --json` and return a `MachineTopology`. Reads
the `Socket(s)`, `Core(s) per socket`, `Thread(s) per core`, and per-node
`NUMA nodeN CPU(s)` fields; falls back to a single NUMA-per-socket layout
if the JSON does not include any `NUMA node` entries.
"""
function parse_lscpu_json_topology(text::AbstractString)
    data = JSON.parse(text)
    fields = Dict{String,String}()
    for entry in get(data, "lscpu", [])
        key = replace(get(entry, "field", ""), r":$" => "")
        fields[key] = get(entry, "data", "")
    end

    sockets_count = parse(Int, fields["Socket(s)"])
    cores_per_socket = parse(Int, fields["Core(s) per socket"])
    threads_per_core = parse(Int, get(fields, "Thread(s) per core", "1"))
    physical_cores = sockets_count * cores_per_socket
    logical_cpus = if haskey(fields, "On-line CPU(s) list")
        parse_cpu_list(fields["On-line CPU(s) list"])
    else
        collect(0:(parse(Int, fields["CPU(s)"]) - 1))
    end

    physical_cpus = logical_cpus[1:physical_cores]
    sockets = SocketTopology[]
    for socket in 0:(sockets_count - 1)
        first_index = socket * cores_per_socket + 1
        last_index = first_index + cores_per_socket - 1
        push!(sockets, SocketTopology(socket, physical_cpus[first_index:last_index]))
    end

    numa_nodes = NumaTopology[]
    for (key, value) in sort(collect(fields); by=first)
        match_result = match(r"^NUMA node([0-9]+) CPU\(s\)$", key)
        match_result === nothing && continue

        node_id = parse(Int, match_result.captures[1])
        cpu_set = intersect(parse_cpu_list(value), physical_cpus)
        socket = domain_for_cpu_set(cpu_set, sockets)
        push!(numa_nodes, NumaTopology(node_id, cpu_set, socket))
    end

    if isempty(numa_nodes)
        numa_nodes = [
            NumaTopology(socket.id, socket.cpu_set, socket.id)
            for socket in sockets
        ]
    end

    MachineTopology(
        physical_cores,
        physical_cpus,
        sockets,
        numa_nodes,
        threads_per_core > 1 || length(logical_cpus) > physical_cores)
end

"""
    parse_lscpu_topology(text) -> MachineTopology

Parse the comma-separated output of `lscpu -p=CPU,CORE,SOCKET[,NODE]` and
return a `MachineTopology`. This is the most portable backend — every
modern Linux `lscpu` supports the `-p` format — and is the fallback when
the JSON and `numactl` probes fail. The `NODE` column is optional; when
absent, NUMA layout mirrors socket layout.
"""
function parse_lscpu_topology(text::AbstractString)
    first_cpu_by_core = Dict{Tuple{Int,Int},Int}()
    socket_by_cpu = Dict{Int,Int}()
    numa_by_cpu = Dict{Int,Int}()
    all_cpus = Int[]

    for raw_line in split(text, '\n')
        line = strip(raw_line)
        (isempty(line) || startswith(line, "#")) && continue

        fields = split(line, ',')
        length(fields) >= 3 || continue

        cpu = parse(Int, fields[1])
        core = parse(Int, fields[2])
        socket = parse(Int, fields[3])
        numa_node = length(fields) >= 4 ? parse(Int, fields[4]) : socket

        push!(all_cpus, cpu)
        key = (socket, core)
        first_cpu_by_core[key] = min(get(first_cpu_by_core, key, cpu), cpu)
        socket_by_cpu[cpu] = socket
        numa_by_cpu[cpu] = numa_node
    end

    physical_cpus = sort(collect(values(first_cpu_by_core)))
    sockets = topology_domains(SocketTopology, physical_cpus, socket_by_cpu)
    numa_nodes = topology_domains(
        NumaTopology,
        physical_cpus,
        numa_by_cpu,
        socket_by_cpu)

    MachineTopology(
        length(physical_cpus),
        physical_cpus,
        sockets,
        numa_nodes,
        length(all_cpus) > length(physical_cpus))
end

"""
    topology_domains(SocketTopology, cpus, socket_by_cpu) -> Vector{SocketTopology}

Group `cpus` by their socket id (looked up in `socket_by_cpu`) and return
one `SocketTopology` per distinct socket, sorted by socket id.
"""
function topology_domains(
        ::Type{SocketTopology},
        cpus::Vector{Int},
        socket_by_cpu::Dict{Int,Int})
    by_socket = Dict{Int,Vector{Int}}()
    for cpu in cpus
        socket = socket_by_cpu[cpu]
        push!(get!(by_socket, socket, Int[]), cpu)
    end

    [
        SocketTopology(socket, sort(cpu_set))
        for (socket, cpu_set) in sort(collect(by_socket); by=first)
    ]
end

"""
    topology_domains(NumaTopology, cpus, numa_by_cpu, socket_by_cpu) -> Vector{NumaTopology}

Group `cpus` by NUMA node id and return one `NumaTopology` per node sorted
by id. The `socket` field is filled in when all CPUs in a node share a
single socket, and is `nothing` if the node straddles multiple sockets.
"""
function topology_domains(
        ::Type{NumaTopology},
        cpus::Vector{Int},
        numa_by_cpu::Dict{Int,Int},
        socket_by_cpu::Dict{Int,Int})
    by_numa = Dict{Int,Vector{Int}}()
    for cpu in cpus
        numa_node = numa_by_cpu[cpu]
        push!(get!(by_numa, numa_node, Int[]), cpu)
    end

    nodes = NumaTopology[]
    for (numa_node, cpu_set) in sort(collect(by_numa); by=first)
        sockets = unique(socket_by_cpu[cpu] for cpu in cpu_set)
        socket = length(sockets) == 1 ? first(sockets) : nothing
        push!(nodes, NumaTopology(numa_node, sort(cpu_set), socket))
    end
    nodes
end

"""
    parse_numactl_topology(text) -> MachineTopology

Parse the output of `numactl --hardware` and return a `MachineTopology`.
Each `node N cpus: ...` line becomes both a `SocketTopology` and a
`NumaTopology`; `numactl` does not distinguish package vs. node, so the
two collapse here. Throws `ArgumentError` if the input has no node lines.
"""
function parse_numactl_topology(text::AbstractString)
    node_cpus = Vector{Pair{Int,Vector{Int}}}()
    for raw_line in split(text, '\n')
        line = strip(raw_line)
        match_result = match(r"^node ([0-9]+) cpus:\s*(.*)$", line)
        match_result === nothing && continue
        node_id = parse(Int, match_result.captures[1])
        cpu_text = strip(String(match_result.captures[2]))
        cpus = isempty(cpu_text) ? Int[] :
            sort(unique(parse.(Int, split(cpu_text))))
        push!(node_cpus, node_id => cpus)
    end

    isempty(node_cpus) &&
        throw(ArgumentError("no NUMA node cpus reported by numactl --hardware"))

    sort!(node_cpus; by=first)
    logical_cpus = sorted_unique_cpus([cpus for (_, cpus) in node_cpus])
    physical_cores = length(logical_cpus)

    sockets = [SocketTopology(id, cpus) for (id, cpus) in node_cpus]
    numa_nodes = [NumaTopology(id, cpus, id) for (id, cpus) in node_cpus]

    MachineTopology(physical_cores, logical_cpus, sockets, numa_nodes, false)
end

function _hwloc_node_os_index(node, fallback::Int)
    if hasproperty(node, :os_index)
        return Int(node.os_index)
    elseif hasproperty(node, :logical_index)
        return Int(node.logical_index)
    end
    fallback
end

function _hwloc_walk_collect_pus!(cpus::Vector{Int}, node)
    if node.type_ == :PU
        push!(cpus, _hwloc_node_os_index(node, length(cpus)))
    end
    for child in node.children
        _hwloc_walk_collect_pus!(cpus, child)
    end
    cpus
end

function _hwloc_collect_cpuset(object)
    cpus = Int[]
    _hwloc_walk_collect_pus!(cpus, object)
    sort!(unique!(cpus))
end

function _hwloc_walk_collect_type!(matches::Vector, node, target_type::Symbol)
    if node.type_ == target_type
        push!(matches, node)
    end
    for child in node.children
        _hwloc_walk_collect_type!(matches, child, target_type)
    end
    matches
end

_hwloc_collect_descendants(root, target_type::Symbol) =
    _hwloc_walk_collect_type!(Any[], root, target_type)

"""
    hwloc_topology() -> MachineTopology

Probe the local machine topology using `Hwloc.jl` and return a
`MachineTopology` describing sockets, NUMA nodes, physical cores, and
hyper-threading status.

Throws `ErrorException` if Hwloc returns no processing units (e.g. when the
underlying `hwloc` library cannot read `/sys` inside a restricted container).
Used as the preferred backend by [`detect_machine_topology`](@ref).
"""
function hwloc_topology()
    root = Hwloc.topology_load()

    package_objs = _hwloc_collect_descendants(root, :Package)
    numa_objs = _hwloc_collect_descendants(root, :NUMANode)
    pu_objs = _hwloc_collect_descendants(root, :PU)
    core_objs = _hwloc_collect_descendants(root, :Core)

    isempty(pu_objs) && throw(ErrorException("Hwloc returned no PU objects"))

    logical_cpus = sort(unique(
        _hwloc_node_os_index(pu, idx - 1) for (idx, pu) in enumerate(pu_objs)))

    physical_core_count = max(length(core_objs), 1)
    pus_per_core = max(length(pu_objs) ÷ physical_core_count, 1)
    hyperthreading = pus_per_core > 1

    physical_cpus = Int[]
    for core in core_objs
        pus = _hwloc_collect_cpuset(core)
        isempty(pus) || push!(physical_cpus, first(pus))
    end
    sort!(physical_cpus)
    isempty(physical_cpus) && (physical_cpus = logical_cpus)

    sockets = SocketTopology[]
    for (idx, pkg) in enumerate(package_objs)
        cpu_set = intersect(_hwloc_collect_cpuset(pkg), physical_cpus)
        push!(sockets, SocketTopology(_hwloc_node_os_index(pkg, idx - 1), cpu_set))
    end
    if isempty(sockets)
        push!(sockets, SocketTopology(0, physical_cpus))
    end

    numa_nodes = NumaTopology[]
    if isempty(numa_objs)
        for socket in sockets
            push!(numa_nodes, NumaTopology(socket.id, socket.cpu_set, socket.id))
        end
    else
        for (idx, numa) in enumerate(numa_objs)
            cpu_set = intersect(_hwloc_collect_cpuset(numa), physical_cpus)
            socket_id = domain_for_cpu_set(cpu_set, sockets)
            push!(numa_nodes,
                NumaTopology(
                    _hwloc_node_os_index(numa, idx - 1),
                    cpu_set,
                    socket_id))
        end
    end

    MachineTopology(
        length(physical_cpus),
        logical_cpus,
        sockets,
        numa_nodes,
        hyperthreading)
end

"""
    detect_machine_topology() -> MachineTopology

Detect the local CPU topology, returning a `MachineTopology`. Probes are tried
in order until one succeeds:

  1. [`hwloc_topology`](@ref) (preferred, uses `Hwloc.jl`)
  2. `lscpu --json`
  3. `numactl --hardware`
  4. `lscpu -p=CPU,CORE,SOCKET,NODE` (with `,NODE` dropped on older `lscpu`)

The final `lscpu -p` step is expected to succeed on any modern Linux host;
the earlier probes provide richer information when available.
"""
function detect_machine_topology()
    try
        return hwloc_topology()
    catch err
        @debug "Hwloc topology probe unavailable" err
    end

    try
        return parse_lscpu_json_topology(read(`lscpu --json`, String))
    catch err
        @debug "lscpu --json topology probe failed" err
    end

    try
        return parse_numactl_topology(read(`numactl --hardware`, String))
    catch err
        @debug "numactl --hardware topology probe failed" err
    end

    output = try
        read(`lscpu -p=CPU,CORE,SOCKET,NODE`, String)
    catch
        read(`lscpu -p=CPU,CORE,SOCKET`, String)
    end
    parse_lscpu_topology(output)
end

"""
    validate_ppi_options(ppi, mpi_ranks_per_worker)

Validate the combined procs-per-instance / MPI fan-out arguments before any
topology detection runs. Throws `ArgumentError` when `ppi` is not positive
or `mpi_ranks_per_worker` is negative.
"""
function validate_ppi_options(ppi::Int, mpi_ranks_per_worker::Int)
    ppi > 0 ||
        throw(ArgumentError("ppi must be positive"))
    mpi_ranks_per_worker >= 0 ||
        throw(ArgumentError("mpi_ranks_per_worker cannot be negative"))
    nothing
end

"""
    cpu_set_string(cpu_set) -> String

Render a sorted vector of CPU IDs as a compact range string suitable for
`taskset`, `numactl --physcpubind=`, and Open MPI `--cpu-set`. Contiguous
runs collapse to `start-end`; isolated CPUs are listed individually
(e.g. `[0,1,2,5,8,9] -> "0-2,5,8-9"`). Returns an empty string for an
empty input.
"""
function cpu_set_string(cpu_set::Vector{Int})
    isempty(cpu_set) && return ""

    ranges = String[]
    first_cpu = last_cpu = first(cpu_set)
    for cpu in cpu_set[2:end]
        if cpu == last_cpu + 1
            last_cpu = cpu
        else
            push!(ranges, first_cpu == last_cpu ? string(first_cpu) : "$first_cpu-$last_cpu")
            first_cpu = last_cpu = cpu
        end
    end
    push!(ranges, first_cpu == last_cpu ? string(first_cpu) : "$first_cpu-$last_cpu")
    join(ranges, ",")
end

"""
    julia_threads_string(placement) -> String

Format the worker's thread count as a Julia `-t` / `JULIA_NUM_THREADS`
argument. On Julia 1.9+ this is `"N,I"` (default plus interactive pool);
on older Julia, just `"N"`.
"""
function julia_threads_string(placement::WorkerPlacement)
    if placement.julia_interactive_threads > 0
        return "$(placement.julia_threads),$(placement.julia_interactive_threads)"
    end
    string(placement.julia_threads)
end

"""
    worker_placement_metadata(topology, placement, ppi;
                              pinning_backend = "numactl") -> Dict{String,Any}

Serialise a worker's placement into the dictionary that the master process
later exposes via `worker_placement(pid)`. `pinning_backend` records which
mechanism actually pinned threads (`"numactl"`, `"ThreadPinning"`, or
`"none"`) so the master can tell users what backend was active.
"""
function worker_placement_metadata(
        topology::MachineTopology,
        placement::WorkerPlacement,
        ppi::Int;
        pinning_backend = "numactl")
    Dict(
        "localid" => placement.localid,
        "ppi" => ppi,
        "physical_cores" => topology.physical_cores,
        "julia_threads" => placement.julia_threads,
        "julia_interactive_threads" => placement.julia_interactive_threads,
        "omp_threads" => placement.omp_threads,
        "cpu_set" => cpu_set_string(placement.cpu_set),
        "numa_node" => placement.numa_node,
        "socket" => placement.socket,
        "pinning_backend" => pinning_backend)
end

const PLACEMENT_USERDATA_KEYS = Set([
    "localid",
    "ppi",
    "physical_cores",
    "julia_threads",
    "julia_interactive_threads",
    "omp_threads",
    "cpu_set",
    "numa_node",
    "socket",
    "pinning_backend"])

"""
    placement_userdata(userdata) -> Dict

Filter an arbitrary user-data dictionary down to the keys that describe a
worker placement, discarding everything else. Used when the master pulls
the worker's metadata out of a larger config bundle.
"""
function placement_userdata(userdata::AbstractDict)
    Dict(key => userdata[key] for key in PLACEMENT_USERDATA_KEYS if haskey(userdata, key))
end

"""
    sorted_unique_cpus(cpu_sets) -> Vector{Int}

Return the sorted union of CPU IDs across an iterable of CPU sets.
"""
function sorted_unique_cpus(cpu_sets)
    sort!(collect(union((Set(cpu_set) for cpu_set in cpu_sets)...)))
end

"""
    split_evenly(cpu_set, count) -> Vector{Vector{Int}}

Split `cpu_set` into `count` contiguous chunks of nearly equal size. When
`length(cpu_set)` is not divisible by `count`, the first `rem` chunks each
get one extra element so callers can distribute the remainder predictably.
Errors if `count` is non-positive or larger than the CPU count.
"""
function split_evenly(cpu_set::Vector{Int}, count::Int)
    count > 0 || throw(ArgumentError("count must be positive"))
    length(cpu_set) >= count || throw(ArgumentError("count cannot exceed CPU count"))

    base_size = div(length(cpu_set), count)
    remainder = rem(length(cpu_set), count)
    chunks = Vector{Int}[]
    first_index = 1

    for index in 1:count
        chunk_size = base_size + (index <= remainder ? 1 : 0)
        last_index = first_index + chunk_size - 1
        push!(chunks, cpu_set[first_index:last_index])
        first_index = last_index + 1
    end

    chunks
end

"""
    domain_for_cpu_set(cpu_set, domains) -> Union{Int,Nothing}

Find the topology domain (socket or NUMA node) whose `cpu_set` is a
superset of the given CPU set, and return its `id`. Returns `nothing` if
the CPU set straddles multiple domains.
"""
function domain_for_cpu_set(cpu_set::Vector{Int}, domains)
    cpu_values = Set(cpu_set)
    for domain in domains
        if issubset(cpu_values, Set(domain.cpu_set))
            return domain.id
        end
    end
    nothing
end

"""
    spread_domain_indices(ndomains, count) -> Vector{Int}

Pick `count` domain indices (1-based) spread evenly across `ndomains`
available domains. Used when `ppi` is smaller than the NUMA node count,
so workers fan out across nodes instead of bunching on the first few.
"""
function spread_domain_indices(ndomains::Int, count::Int)
    [floor(Int, (index - 1) * ndomains / count) + 1 for index in 1:count]
end

"""
    placement_from_cpu_set(localid, topology, cpu_set) -> WorkerPlacement

Build a `WorkerPlacement` for the given CPU set: sorts the set, derives
the enclosing NUMA / socket domain ids (or `nothing` if none fully
contains it), and sets thread counts equal to the CPU count. On Julia 1.9+
one slot is reserved as an interactive thread.
"""
function placement_from_cpu_set(
        localid::Int,
        topology::MachineTopology,
        cpu_set::Vector{Int})
    sorted_cpu_set = sort(cpu_set)
    thread_count = length(sorted_cpu_set)
    interactive_threads = VERSION >= v"1.9" ? 1 : 0
    numa_node = domain_for_cpu_set(sorted_cpu_set, topology.numa_nodes)
    socket = domain_for_cpu_set(sorted_cpu_set, topology.sockets)

    WorkerPlacement(
        localid,
        sorted_cpu_set,
        numa_node,
        socket,
        thread_count,
        interactive_threads,
        thread_count)
end

"""
    plan_worker_placements(topology, ppi) -> Vector{WorkerPlacement}

Plan one `WorkerPlacement` per Julia worker on a single VM, where `ppi` is
the procs-per-instance count. The policy is:

  * `ppi == 1`             — one worker owns the whole VM.
  * `ppi == sockets`       — one worker per socket.
  * `ppi <= numa_nodes`    — workers spread across NUMA domains (via
    [`spread_domain_indices`](@ref)) to maximize memory bandwidth.
  * otherwise              — fall back to splitting the union of NUMA
    cpusets into `ppi` contiguous chunks (warns when the division is
    uneven).

Throws `ArgumentError` if `ppi` is non-positive or exceeds
`topology.physical_cores`.
"""
function plan_worker_placements(topology::MachineTopology, ppi::Int)
    ppi > 0 || throw(ArgumentError("ppi must be positive"))
    if ppi > topology.physical_cores
        throw(ArgumentError("ppi cannot exceed physical cores"))
    end

    if ppi == 1
        cpu_set = sorted_unique_cpus([topology.logical_cpus])
        return [placement_from_cpu_set(1, topology, cpu_set)]
    end

    if ppi == length(topology.sockets) && !isempty(topology.sockets)
        return [
            placement_from_cpu_set(index, topology, topology.sockets[index].cpu_set)
            for index in 1:ppi
        ]
    end

    if ppi <= length(topology.numa_nodes) && !isempty(topology.numa_nodes)
        node_indices = ppi == length(topology.numa_nodes) ?
            collect(1:ppi) :
            spread_domain_indices(length(topology.numa_nodes), ppi)

        return [
            placement_from_cpu_set(index, topology, topology.numa_nodes[node_index].cpu_set)
            for (index, node_index) in enumerate(node_indices)
        ]
    end

    cpu_set = if !isempty(topology.numa_nodes)
        sorted_unique_cpus([node.cpu_set for node in topology.numa_nodes])
    else
        sorted_unique_cpus([topology.logical_cpus])
    end
    chunks = split_evenly(cpu_set, ppi)
    if rem(length(cpu_set), ppi) != 0
        @warn "CPU cores do not divide evenly across workers" cpu_count=length(cpu_set) ppi
    end

    [
        placement_from_cpu_set(index, topology, chunk)
        for (index, chunk) in enumerate(chunks)
    ]
end

"""
    plan_mpi_rank_placements(placement, mpi_ranks_per_worker) -> Vector{MpiRankPlacement}

Subdivide a `WorkerPlacement`'s CPU set into `mpi_ranks_per_worker`
contiguous chunks and emit one `MpiRankPlacement` per chunk. Ranks
inherit the worker's NUMA / socket domains. Warns when the rank count
does not divide the CPU set evenly; errors when it exceeds the CPU count.
"""
function plan_mpi_rank_placements(
        placement::WorkerPlacement,
        mpi_ranks_per_worker::Int)
    mpi_ranks_per_worker > 0 ||
        throw(ArgumentError("mpi_ranks_per_worker must be positive"))
    if mpi_ranks_per_worker > length(placement.cpu_set)
        throw(ArgumentError(
            "mpi_ranks_per_worker $(mpi_ranks_per_worker) exceeds the " *
            "worker CPU count $(length(placement.cpu_set))"))
    end

    chunks = split_evenly(placement.cpu_set, mpi_ranks_per_worker)
    if rem(length(placement.cpu_set), mpi_ranks_per_worker) != 0
        @warn "MPI ranks do not divide CPU set evenly" worker_localid=placement.localid cpu_count=length(placement.cpu_set) mpi_ranks_per_worker
    end

    [
        MpiRankPlacement(
            placement.localid,
            rank_index - 1,
            chunk,
            placement.numa_node,
            placement.socket,
            length(chunk))
        for (rank_index, chunk) in enumerate(chunks)
    ]
end

"""
    plan_mpi_placements(topology, ppi, mpi_ranks_per_worker)
        -> Vector{Tuple{WorkerPlacement, Vector{MpiRankPlacement}}}

Compose [`plan_worker_placements`](@ref) and
[`plan_mpi_rank_placements`](@ref) into a single per-VM plan: for each
Julia worker on the VM, return the worker placement paired with its MPI
rank placements.
"""
function plan_mpi_placements(
        topology::MachineTopology,
        ppi::Int,
        mpi_ranks_per_worker::Int)
    worker_placements = plan_worker_placements(topology, ppi)
    [
        (placement, plan_mpi_rank_placements(placement, mpi_ranks_per_worker))
        for placement in worker_placements
    ]
end

"""
    numactl_arguments(placement) -> Vector{String}

Render the `numactl` flags that pin the given placement: a
`--physcpubind=...` always, plus `--membind=N` when the placement sits
inside a single NUMA node. Defined for both `WorkerPlacement` and
`MpiRankPlacement`.
"""
function numactl_arguments(placement::WorkerPlacement)
    args = ["--physcpubind=$(cpu_set_string(placement.cpu_set))"]
    if placement.numa_node !== nothing
        push!(args, "--membind=$(placement.numa_node)")
    end
    args
end

function numactl_arguments(placement::MpiRankPlacement)
    args = ["--physcpubind=$(cpu_set_string(placement.cpu_set))"]
    if placement.numa_node !== nothing
        push!(args, "--membind=$(placement.numa_node)")
    end
    args
end

"""
    mpirun_command(placement, ranks, exename, rank_payload;
                   extra_flags = "") -> String

Build the shell command that launches one `mpirun` for a single worker's
ranks. Always emits `--cpu-set <list> --bind-to cpu-list:ordered`, which
requires Open MPI — other MPI implementations reject these flags.
`extra_flags` is appended verbatim; it must not contain another
`--bind-to`, since Open MPI rejects duplicate binding directives.
"""
function mpirun_command(
        placement::WorkerPlacement,
        ranks::Vector{MpiRankPlacement},
        exename,
        rank_payload::AbstractString;
        extra_flags::AbstractString = "")
    isempty(ranks) && throw(ArgumentError("rank list must be non-empty"))
    cpu_list = cpu_set_string(placement.cpu_set)
    base = "mpirun -n $(length(ranks)) --cpu-set $(cpu_list) --bind-to cpu-list:ordered"
    flags = isempty(strip(extra_flags)) ? "" : " " * strip(extra_flags)
    base * flags * " " * exename * " " * rank_payload
end

"""
    numactl_prefix(placement) -> String

Render `"numactl <args> "` as a single shell prefix string. Returns
`""` when the placement has no CPUs assigned, so the caller can prepend
unconditionally.
"""
function numactl_prefix(placement::WorkerPlacement)
    isempty(placement.cpu_set) && return ""
    "numactl " * join(numactl_arguments(placement), " ") * " "
end

"""
    worker_launch_command(exename, exeflags, placement;
                          use_numactl) -> Cmd

Build the `Cmd` that launches a Julia worker process for the given
placement. When `use_numactl` is `true` the command is wrapped in
`numactl` with the placement's `--physcpubind` / `--membind` flags;
otherwise just `exename exeflags -t <threads> --worker`.
"""
function worker_launch_command(
        exename,
        exeflags,
        placement::WorkerPlacement;
        use_numactl::Bool)
    threads = julia_threads_string(placement)
    if use_numactl
        return `numactl $(numactl_arguments(placement)) $exename $exeflags -t $threads --worker`
    end
    `$exename $exeflags -t $threads --worker`
end

"""
    placement_environment(placement) -> Dict{String,String}

Return the environment variables that should be set on a worker process to
match its placement: `JULIA_NUM_THREADS`, `OMP_NUM_THREADS`,
`OMP_PROC_BIND=close`, and `OMP_PLACES=cores`.
"""
function placement_environment(placement::WorkerPlacement)
    Dict(
        "JULIA_NUM_THREADS" => julia_threads_string(placement),
        "OMP_NUM_THREADS" => string(placement.omp_threads),
        "OMP_PROC_BIND" => "close",
        "OMP_PLACES" => "cores")
end

"""
    placement_export_string(placement) -> String

Render [`placement_environment`](@ref) as a single shell-friendly `export
K=V; export K=V; ...` string for embedding into cloud-init / remote
launch scripts.
"""
function placement_export_string(placement::WorkerPlacement)
    build_envstring(placement_environment(placement))
end

"""
    pin_julia_threads(cpu_set) -> Bool

Pin the current process' Julia threads to the given physical CPU IDs using
`ThreadPinning.pinthreads`. Returns `true` when pinning succeeds and `false`
otherwise (including when `cpu_set` is empty).

Pinning is layered on top of OS-level `numactl` pinning of the worker
process; the in-process pin further restricts where individual Julia threads
may run inside that allowed CPU set. Errors raised by `ThreadPinning` are
logged at `@debug` and converted to a `false` return so a failed pin does
not crash the worker.
"""
function pin_julia_threads(cpu_set::Vector{Int})
    isempty(cpu_set) && return false
    try
        ThreadPinning.pinthreads(cpu_set)
        true
    catch err
        @debug "thread pinning failed; relying on numactl pinning" err
        false
    end
end
