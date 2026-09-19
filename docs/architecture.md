# Architecture

The bridge exposes the TXmlConnector Windows amd64 DLL through gRPC. It owns
native memory and session lifetime; broker authentication, order correlation,
and trading decisions belong to the client. The DLL is supplied separately.

## Components

| Component | Responsibility |
|---|---|
| `cmd/txmlconnector` | Process startup, listeners, signals, and bounded shutdown |
| `internal/config` | CLI flags, paths, and resource limits |
| `internal/bridge` | Connection ownership, command execution, event delivery, health, and metrics |
| `internal/native` | Windows ABI bindings, serialized DLL access, and memory ownership |
| `api/connect.proto`, `proto/` | Wire contract and generated Go bindings |

```text
Client: one physical gRPC connection
  │
  ├─ SendCommand → validation → command queue → session executor
  │                                                │
  │                                          native adapter → DLL
  │                                                │
  │                  synchronous XML result ←───────┘
  │
  └─ FetchResponseData ← event queue ← native callback ← DLL
                                           │
                                  deferred pointer release
                                           │
                                     native cleaner
```

The module is independent of its host workspace and the original bridge.
Logging and error handling use `log/slog`, `errors`, and `fmt.Errorf`.

## Connection ownership

One process owns one DLL session, controlled by one client. The first physical
gRPC connection to invoke an RPC becomes the owner. Commands and the sole
callback stream must share that connection. Both RPCs reject other connections
with `AlreadyExists` and reason `CLIENT_ALREADY_ATTACHED`.

Ownership lasts while the client transport is connected. When it closes, the
bridge enters `resetting`, waits for its RPC handlers to exit, and serializes
native cleanup behind any executing command. `UnInitialize` ends the old broker
session and joins callbacks; the bridge then discards buffered events and calls
`Initialize` again. Only after cleanup succeeds can another connection claim the
service. RPCs during cleanup return `SESSION_NOT_READY`.

Closing only the callback stream releases its subscription without disconnecting
the broker or releasing the transport owner. The same owner can reopen the stream;
other connections are still rejected. Queued events remain available, subject to
the normal buffer limits. Events already sent have no replay guarantee.

Reconnecting creates a fresh broker session: the client must send `connect` and
restore its state. Unknown command outcomes and cleanup failures remain terminal
faults; disconnection does not clear them.

Connection ownership is not authentication. The deployment must restrict access
to a trusted application and must not merge multiple clients into one backend
connection through a proxy.

## Session lifecycle

```text
starting → ready → resetting → ready
              ├─→ faulted → stopping → stopped
              └───────────→ stopping
```

`ready` means the bridge can accept commands, not that the broker is connected.
Broker connectivity is reported in `server_status` callbacks and does not itself
change bridge readiness.

A fault is terminal for the current process. Event loss, callback read/parse
errors, native reset failures, or an unknown command outcome prevent further
commands. Restarting creates a new session; it does not recover broker state.

## Command execution

Commands follow `queued → running → completed`. A single executor calls the
native adapter. The waiting queue is bounded; rejection of a full queue does
not dispatch the command.

The bridge validates XML structure, a nonempty command `id`, and size limits.
It does not maintain a command allowlist: the DLL validates command names and
parameters. Each dispatched command results in one `SendCommand` call, with no
retry on native errors or buffer failures.

Cancellation before dispatch guarantees that the DLL was not called. After
dispatch, cancellation ends the wait but cannot interrupt the native call. The
outcome becomes unknown and the session faults. A client-side deadline can hide
the server's response, so a transport error alone is not evidence of nonexecution.

## Native calls and memory

All DLL API calls share a mutex, including initialization, command execution,
shutdown, and `FreeMemory`. Return values follow the DLL contract; a stale
Windows `GetLastError` does not invalidate a returned XML pointer.

Synchronous responses are copied into Go memory and freed before returning.
Callbacks copy their payload, enqueue the event without waiting for capacity,
and retire the native pointer for later release. A cleaner frees retired
pointers under the same DLL mutex. A callback never acquires that mutex: a
native call may itself be waiting for the callback to return.

During shutdown, the adapter stops the cleaner, calls `UnInitialize`, and drains
the remaining pointers after native callbacks finish. The process has an outer
shutdown deadline because a hung DLL call cannot be safely terminated inside Go.

## Events and resource limits

The event queue is bounded by message count and total XML bytes; each message
also has a size limit. Overflow faults the session instead of silently dropping
events and continuing. Delivery preserves enqueue order. There is no persistent
journal, replay, or client acknowledgement; a successful stream send is not proof
that the client processed the event.

Native allocations waiting for `FreeMemory` are outside the Go queue byte limit.
If a DLL call stalls, these allocations may accumulate. Process memory limits
and external supervision remain necessary.

## Result semantics

Synchronous command results, RPC failures, and asynchronous events are distinct:

- `<result success="false">` remains a successful RPC containing the original
  rejection XML. Clients must inspect it.
- A synchronous `<error>` becomes gRPC `Internal` with the connector's error text.
- Missing, unreadable, or invalid native responses produce an unknown outcome
  and fault the session.
- Asynchronous `<error>` messages remain stream events. The bridge does not
  attribute them to the last command.

A command acknowledgement or transaction ID is not confirmation of an executed
trade. Clients correlate transactions, orders, and trades and reconcile unknown
outcomes with the broker. There is no deduplication or exactly-once guarantee.
The [README](../README.md#grpc-and-errors) lists status codes and structured
`google.rpc.ErrorInfo` reasons.

## Wire compatibility

The local schema preserves protobuf package `transaqConnector`, service
`ConnectService`, method names, and field numbers. Both methods transport XML
in the `message` field. Generated bindings live in `go-txmlconnector/proto` and
are checked in; no upstream source package is required.

Wire compatibility does not supply missing client behavior. Clients must handle
callback errors, stream loss, command rejections, and recovery explicitly.

See [operations](operations.md) for deployment, diagnostics, and recovery.
