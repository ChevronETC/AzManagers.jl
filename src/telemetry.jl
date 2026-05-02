"""
Telemetry and metrics for AzManagers.jl

Provides a `ManagerMetrics` struct that accumulates operational counters and
timing data throughout the lifetime of an `AzManager` instance.  A snapshot
can be retrieved via `AzManagers.metrics()`.
"""

mutable struct ManagerMetrics
    # Worker lifecycle
    workers_joined::Int
    workers_lost::Int
    workers_preempted::Int
    workers_pruned::Int
    workers_join_failed::Int
    connections_validation_failed::Int

    # Scale-set lifecycle
    scalesets_created::Int
    scalesets_deleted::Int

    # Azure API
    api_calls::Int
    api_retries::Int
    api_errors::Int
    api_throttles::Int

    # Timestamps
    last_prune_time::DateTime
    last_clean_time::DateTime
    created_at::DateTime

    # Worker join durations (seconds) — kept bounded
    worker_join_durations::Vector{Float64}

    lock::ReentrantLock

    function ManagerMetrics()
        new(
            0, 0, 0, 0, 0, 0,  # worker counters
            0, 0,            # scaleset counters
            0, 0, 0, 0,     # API counters
            DateTime(0), DateTime(0), now(Dates.UTC),  # timestamps
            Float64[],       # join durations
            ReentrantLock()
        )
    end
end

# ── Thread-safe increment helpers ─────────────────────────────────────

function record_api_call!(m::ManagerMetrics)
    lock(m.lock) do
        m.api_calls += 1
    end
end

function record_api_retry!(m::ManagerMetrics)
    lock(m.lock) do
        m.api_retries += 1
    end
end

function record_api_error!(m::ManagerMetrics)
    lock(m.lock) do
        m.api_errors += 1
    end
end

function record_api_throttle!(m::ManagerMetrics)
    lock(m.lock) do
        m.api_throttles += 1
    end
end

function record_worker_joined!(m::ManagerMetrics, duration_seconds::Float64=0.0)
    lock(m.lock) do
        m.workers_joined += 1
        if duration_seconds > 0
            push!(m.worker_join_durations, duration_seconds)
            # Keep bounded — retain last 1000 entries
            if length(m.worker_join_durations) > 1000
                deleteat!(m.worker_join_durations, 1:length(m.worker_join_durations)-1000)
            end
        end
    end
end

function record_worker_lost!(m::ManagerMetrics)
    lock(m.lock) do
        m.workers_lost += 1
    end
end

function record_worker_preempted!(m::ManagerMetrics)
    lock(m.lock) do
        m.workers_preempted += 1
    end
end

function record_worker_pruned!(m::ManagerMetrics)
    lock(m.lock) do
        m.workers_pruned += 1
    end
end

function record_worker_join_failed!(m::ManagerMetrics)
    lock(m.lock) do
        m.workers_join_failed += 1
    end
end

function record_connection_validation_failed!(m::ManagerMetrics)
    lock(m.lock) do
        m.connections_validation_failed += 1
    end
end

function record_scaleset_created!(m::ManagerMetrics)
    lock(m.lock) do
        m.scalesets_created += 1
    end
end

function record_scaleset_deleted!(m::ManagerMetrics)
    lock(m.lock) do
        m.scalesets_deleted += 1
    end
end

function record_prune_time!(m::ManagerMetrics)
    lock(m.lock) do
        m.last_prune_time = now(Dates.UTC)
    end
end

function record_clean_time!(m::ManagerMetrics)
    lock(m.lock) do
        m.last_clean_time = now(Dates.UTC)
    end
end

# ── Snapshot ──────────────────────────────────────────────────────────

"""
    metrics() -> NamedTuple

Return a point-in-time snapshot of all operational metrics.
"""
function metrics()
    manager = azmanager()
    isdefined(manager, :metrics) || return (;)
    m = manager.metrics
    lock(m.lock) do
        (;
            workers_joined      = m.workers_joined,
            workers_lost        = m.workers_lost,
            workers_preempted   = m.workers_preempted,
            workers_pruned      = m.workers_pruned,
            workers_join_failed = m.workers_join_failed,
            connections_validation_failed = m.connections_validation_failed,
            scalesets_created   = m.scalesets_created,
            scalesets_deleted   = m.scalesets_deleted,
            api_calls           = m.api_calls,
            api_retries         = m.api_retries,
            api_errors          = m.api_errors,
            api_throttles       = m.api_throttles,
            last_prune_time     = m.last_prune_time,
            last_clean_time     = m.last_clean_time,
            uptime_seconds      = round(Dates.value(now(Dates.UTC) - m.created_at) / 1000, digits=1),
            median_join_seconds = isempty(m.worker_join_durations) ? NaN : sort(m.worker_join_durations)[div(length(m.worker_join_durations)+1, 2)],
        )
    end
end

"""
    log_health_summary(manager)

Emit a structured `@info` log line summarizing cluster health.
Called periodically from the `CleanTick` handler.
"""
function log_health_summary(manager)
    isdefined(manager, :metrics) || return
    m = manager.metrics

    _pending_down = pending_down(manager)
    pending_down_count = isempty(_pending_down) ? 0 : mapreduce(length, +, values(_pending_down))

    lock(m.lock) do
        @info "cluster health summary" workers=(nprocs() == 1 ? 0 : nworkers()) provisioned=nworkers_provisioned() pending_down=pending_down_count workers_joined=m.workers_joined workers_lost=m.workers_lost workers_preempted=m.workers_preempted workers_pruned=m.workers_pruned workers_join_failed=m.workers_join_failed api_calls=m.api_calls api_retries=m.api_retries api_throttles=m.api_throttles scalesets_created=m.scalesets_created scalesets_deleted=m.scalesets_deleted
    end
end
