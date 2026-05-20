# AzManagers v2 — System Invariants

## 1. Handlers return Actions, never execute side effects directly

All event handlers are pure decision functions. They accept state and event data,
and return one or more `Action` values describing what should happen. A separate
`execute!` dispatch layer performs the actual side effects (Azure API calls, state
mutations, worker registration).

This guarantees:
- Handlers are testable without Azure credentials or live infrastructure
- Every action taken by the system is loggable and auditable before execution
- The reconciliation loop can inspect, filter, or reorder actions before committing
- Error isolation: a failed execution does not corrupt handler logic

```
event → handler(state, event_data) → Action(s) → execute!(state, action) → side effects
```
