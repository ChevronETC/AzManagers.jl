const SPIN_CHARS = ['◐', '◓', '◑', '◒']

# ─── Generic Spinner ─────────────────────────────────────────────────────────

"""
    Spinner

A generic async spinner that renders a status message independently of any work task.
The spinner animates at a fixed interval and calls a user-provided `message_fn(elapsed)`
to get the current status string.
"""
mutable struct Spinner
    task::Task
    stop::Threads.Atomic{Bool}
    starttime::Float64
end

"""
    start_spinner(message_fn; interval=0.25) -> Spinner

Launch an async task that renders `message_fn(elapsed_seconds)` at `interval` cadence.
The function should return a `String` to display (without newline).

Example:
```julia
s = start_spinner(t -> "waiting for VM to start (\$(round(t, digits=1))s)")
# ... do blocking work ...
stop_spinner!(s)
```
"""
function start_spinner(message_fn::Function; interval::Float64=1.0)
    stop_flag = Threads.Atomic{Bool}(false)
    starttime = time()

    task = errormonitor(@async _spinner_loop(message_fn, stop_flag, starttime, interval))

    Spinner(task, stop_flag, starttime)
end

function _spinner_loop(message_fn::Function, stop_flag::Threads.Atomic{Bool}, starttime::Float64, interval::Float64)
    spinidx = 1
    try
        while !stop_flag[]
            elapsed = time() - starttime
            msg = message_fn(elapsed)
            char = SPIN_CHARS[spinidx]
            @printf(stdout, "\r\e[K  %c %s", char, msg)
            flush(stdout)
            spinidx = spinidx == 4 ? 1 : spinidx + 1
            sleep(interval)
        end
    catch e
        if !(e isa InvalidStateException)
            @debug "spinner error" exception=(e, catch_backtrace())
        end
    end
end

"""
    stop_spinner!(spinner; final_message=nothing)

Stop the spinner and optionally print a final status line.
If `final_message` is provided, it replaces the spinner line.
"""
function stop_spinner!(spinner::Spinner; final_message::Union{Nothing,String}=nothing)
    spinner.stop[] = true
    if !istaskdone(spinner.task)
        try; wait(spinner.task); catch; end
    end
    if final_message !== nothing
        @printf(stdout, "\r\e[K  ✓ %s\n", final_message)
        flush(stdout)
    end
end

# ─── Worker Progress Display (specialized) ───────────────────────────────────

"""
    ProgressDisplay

A specialized spinner for tracking worker connections.
Wakes on `workers_changed` condition for immediate updates and auto-stops
when the target worker count is reached.
"""
mutable struct ProgressDisplay
    target::Int
    manager::AzManager
    task::Task
    stop::Threads.Atomic{Bool}
    starttime::Float64
end

"""
    start_progress(manager, target) -> ProgressDisplay

Launch an async spinner that displays worker connection progress.
Wakes on `manager.workers_changed` or every 250ms for animation.
Stops automatically when `nworkers() >= target`.
"""
function start_progress(manager::AzManager, target::Int)
    stop_flag = Threads.Atomic{Bool}(false)
    starttime = time()

    task = errormonitor(@async _progress_loop(manager, target, stop_flag, starttime))

    ProgressDisplay(target, manager, task, stop_flag, starttime)
end

function _progress_loop(manager::AzManager, target::Int, stop_flag::Threads.Atomic{Bool}, starttime::Float64)
    spinidx = 1
    render_interval = 1.0

    try
        while !stop_flag[]
            n = nprocs() == 1 ? 0 : nworkers()
            elapsed = time() - starttime

            # Render
            char = SPIN_CHARS[spinidx]
            @printf(stdout, "\r\e[K  %c %d/%d workers up (%.1fs)", char, n, target, elapsed)
            flush(stdout)

            spinidx = spinidx == 4 ? 1 : spinidx + 1

            # Target reached — print final line and exit
            if n >= target
                @printf(stdout, "\r\e[K  %d/%d workers running (%.1fs)\n", n, target, elapsed)
                flush(stdout)
                return
            end

            # Wait for worker change or animation tick
            _wait_for_change_or_timeout(manager.workers_changed, render_interval)
        end
    catch e
        if !(e isa InvalidStateException)
            @debug "progress display error" exception=(e, catch_backtrace())
        end
    end
end

"""
    wait_for_progress(display; timeout=nothing) -> Bool

Block until the target worker count is reached or timeout expires.
Throws on timeout.
"""
function wait_for_progress(display::ProgressDisplay; timeout=nothing)
    if timeout === nothing
        timeout = parse(Int, get(ENV, "JULIA_WORKER_TIMEOUT", "720")) + 120
    end

    deadline = display.starttime + timeout

    while !istaskdone(display.task)
        if time() > deadline
            n = nprocs() == 1 ? 0 : nworkers()
            elapsed = time() - display.starttime
            @printf(stdout, "\r\e[K  %d/%d workers up (%.1fs) — timed out\n", n, display.target, elapsed)
            flush(stdout)
            stop_progress(display)
            error("Timed out after $(round(elapsed, digits=1))s waiting for workers: $n/$(display.target) up. Consider increasing JULIA_WORKER_TIMEOUT.")
        end
        sleep(0.5)
    end

    true
end

"""
    stop_progress(display)

Signal the progress display to stop rendering.
"""
function stop_progress(display::ProgressDisplay)
    display.stop[] = true
    if !istaskdone(display.task)
        try; wait(display.task); catch; end
    end
end

# ─── Internal utilities ──────────────────────────────────────────────────────

"""
    _wait_for_change_or_timeout(cond, timeout_secs)

Wait on a `Threads.Condition` with a maximum timeout.
Returns when notified or after `timeout_secs`, whichever comes first.
"""
function _wait_for_change_or_timeout(cond::Threads.Condition, timeout_secs::Float64)
    done = Threads.Atomic{Bool}(false)
    timer = Timer(timeout_secs) do _
        done[] = true
        lock(cond) do
            notify(cond)
        end
    end

    lock(cond) do
        if !done[]
            wait(cond)
        end
    end
    close(timer)
    nothing
end
