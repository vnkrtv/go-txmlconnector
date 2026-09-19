# go-txmlconnector

A Go gRPC bridge to the Windows amd64 TXmlConnector DLL, inspired by
[kmlebedev/txmlconnector](https://github.com/kmlebedev/txmlconnector).

The project is a standalone Go module with its own protobuf schema and generated
bindings. It preserves the existing wire interface and provides serialized DLL
access, explicit error handling, bounded queues, structured logs, and metrics.
It has no source dependency on the original project or a trading application.

## Architecture

```text
One gRPC client connection
  ├─ SendCommand ───────► bounded command queue ─► executor ─► DLL
  └─ FetchResponseData ◄─ bounded event queue ◄── native callback
                                                        │
                                           deferred FreeMemory
```

- **Native adapter** (`internal/native`): pure Go Windows bindings, without cgo
  or MinGW. A shared mutex serializes all DLL calls, including `FreeMemory`.
  Callbacks copy XML and defer native memory release to a cleaner goroutine;
  they never wait for command execution or event queue capacity.
- **Session** (`internal/bridge`): one command executor, bounded command and
  event queues, deadlines, and lifecycle management. Each dispatched command
  is sent once; the bridge never retries it. Command IDs pass through to the DLL
  without a hardcoded list of supported commands.
- **Transport**: the first connection to make an RPC owns the DLL session and
  its single callback stream. Both RPCs must use the same `grpc.ClientConn`.
  Other connections receive `AlreadyExists` (`CLIENT_ALREADY_ATTACHED`).
- **Lifecycle**: a disconnected owner triggers `ready → resetting → ready`.
  The executor closes and reinitializes the DLL, discards the old event backlog,
  and accepts the next client. Closing only the callback stream allows the same
  owner to subscribe again. Event overflow and unknown command outcomes still
  fault the session and require reconciliation and a process restart.

A DLL call cannot be canceled safely. Deadlines stop waiting, but an in-flight
native call may continue. Native memory awaiting release is outside the Go queue
limits; use process/container memory limits. Shutdown has a fixed time budget.

See [architecture](docs/architecture.md) for ownership, execution, and memory
management, and [operations](docs/operations.md) for diagnostics and recovery.

## Build and run

Use Go 1.24.11 from the repository root:

```sh
go test -race ./...
go vet ./...
make build VERSION=local
```

The output is `bin/txmlconnector.exe`. Portable tests run on Linux; loading the
DLL requires Windows amd64 or a compatible Wine runtime. Supply the DLL separately.

On Windows (PowerShell):

```powershell
.\bin\txmlconnector.exe -dll C:\txml\txmlconnector64-6.43.2.24.0.dll -native-log-dir C:\txml\logs
```

### Docker

Place `txmlconnector64-6.43.2.24.0.dll` in the repository root (ignored by Git), then:

```sh
docker compose up --build
```

The image builds local sources and runs the Windows executable under Wine 10.
Compose exposes gRPC on `127.0.0.1:50052` and HTTP on `127.0.0.1:9092`, with no
automatic restart. For a standalone build or a different DLL in the build context:

```sh
docker build --platform linux/amd64 \
  --build-arg DLL_SOURCE=txmlconnector64-6.43.2.24.0.dll \
  --build-arg VERSION=local -t go-txmlconnector:local .
```

### Configuration

Configuration uses CLI flags. Broker credentials arrive in the client's XML
`connect` command; `TC_*` environment variables are not used.

| Flag | Default | Purpose |
|---|---|---|
| `-dll` | `txmlconnector64-6.43.2.24.0.dll` | Native library path |
| `-native-log-dir` | `logs` | Private DLL log directory |
| `-native-log-level` | `1` | DLL log level, 1–3 |
| `-log-level` | `info` | JSON stdout: debug/info/warn/error |
| `-grpc-address` | `127.0.0.1:50051` | gRPC listener |
| `-http-address` | `127.0.0.1:9090` | Health and metrics listener |
| `-command-timeout` | `15s` | Command deadline, including queue time |
| `-shutdown-timeout` | `10s` | Shutdown and initialization wait budget |
| `-command-queue` | `64` | Waiting commands, plus one executing |
| `-event-queue` | `1024` | Buffered callback messages |
| `-max-message-bytes` | `4194304` | Per-message XML limit, up to 64 MiB |
| `-max-event-bytes` | `33554432` | Total buffered event bytes |

## gRPC and errors

The [schema](api/connect.proto) preserves protobuf package `transaqConnector`,
service `ConnectService`, and field numbers. Local Go bindings are in
`go-txmlconnector/proto`.

- `SendCommand`: accepts XML in `message` and returns the DLL's XML unchanged
  when the response is a valid result.
- `FetchResponseData`: streams callback XML in enqueue order. The
  `txml-stream: attached` header confirms registration; a second stream is rejected.

| Condition | Result |
|---|---|
| `<result success="true">` | gRPC OK, original XML |
| `<result success="false">` | gRPC OK, original XML; the client must handle rejection |
| Synchronous `<error>` | `Internal`, original error text, `CONNECTOR_ERROR` |
| Null response, read/free failure, or invalid response | `Internal`, `COMMAND_OUTCOME_UNKNOWN`; session faults |
| Invalid or oversized command XML | `InvalidArgument`; no DLL call |
| Full command queue | `ResourceExhausted`, `COMMAND_QUEUE_FULL`; no DLL call |
| Session not ready | `FailedPrecondition`, `SESSION_NOT_READY`; no DLL call |
| Cancellation/deadline before dispatch | `Canceled`/`DeadlineExceeded`, `COMMAND_NOT_DISPATCHED` |
| Cancellation/deadline after dispatch | `Canceled`/`DeadlineExceeded`, `COMMAND_OUTCOME_UNKNOWN`; session faults |

Structured reasons use `google.rpc.ErrorInfo`, domain `txmlconnector`. A client
transport deadline may hide the server's details: without reliable evidence that
a command was not dispatched, treat its outcome as unknown and reconcile with
the broker before taking further action. Do not automatically retry trading commands.

Asynchronous `<error>` messages remain callback events. Clients must handle them;
the bridge does not associate them with an order without correlation data.

There is no replay, client ACK, deduplication, or exactly-once guarantee.
Ownership follows the physical connection. After disconnection and native cleanup,
a new connection can claim the service and establish a fresh broker session.
Do not multiplex different clients through one proxy connection. TLS and client
authentication are not implemented; restrict access to one trusted application.

## Observability and recovery

- `/livez`: HTTP 200 while the HTTP server responds.
- `/readyz`: HTTP 200 when the bridge is ready, otherwise 503; includes session
  state, fault reason, queue usage, and client attachment. Broker connectivity
  is reported separately through `server_status`.
- `/metrics`: Prometheus command counts and latency, queue depth/bytes, callback
  counts/errors, session faults, readiness, stream/client attachment, and last
  reported broker connection state. Metrics use the `txml_` prefix.

JSON logs use `log/slog` and include request ID, command ID, outcome, and duration.
Command labels come directly from the XML `id`. Application logs exclude XML
payloads and broker error text; native DLL logs may contain sensitive XML and
need restricted access and retention.

On a session fault, stop new trading actions, retain diagnostics, reconcile
orders and trades with the broker, fix the cause, then restart and restore client
state. Monitor readiness, session faults, queue saturation, and command latency.

## Validation

[GitHub Actions](.github/workflows/ci.yml) runs on pushes, pull requests, and
manual dispatch in this standalone repository. It checks Linux and Windows code
with the pinned linter, runs tests with the race detector, and cross-compiles the
Windows service and native tests. Go is selected from `go.mod`; the linter version
comes from the Makefile. CI requires no DLL or broker credentials: native DLL
and live bridge smoke tests remain opt-in runtime checks.

Linting uses golangci-lint v2.5.0 and the repository's `.golangci.yaml`:

```sh
make lint-install  # Install the pinned linter; add Go's binary directory to PATH.
make fmt           # Format code and imports with gofmt and goimports.
make lint          # Check the host platform.
make lint-windows  # Check Windows amd64, including the DLL adapter.
```

On Linux, run both targets to cover platform-specific code. Checks include error
handling, static analysis, formatting, imports, and spelling. Set
`GOLANGCI=/path/to/golangci-lint` to use a specific executable.

Ordinary tests use a fake native library. To test the real DLL without a broker
account or network access:

```sh
docker build --platform linux/amd64 --target smoke -t go-txmlconnector:smoke .
docker run --rm --network none --memory 1g go-txmlconnector:smoke
```

For a full bridge smoke test, start a fresh, isolated service with no trading session:

```sh
TXML_SMOKE_TARGET=127.0.0.1:50052 TXML_SMOKE_HTTP=http://127.0.0.1:9092 \
  go test ./internal/bridge -run '^TestLiveDLLBridge$' -v
```

This test claims the connection, checks commands, callbacks, errors, ownership,
and metrics, then verifies that a second client can take over after disconnection.
Without these variables, the live test is skipped.

Race tests, vet, Windows cross-build, and native/bridge smoke tests with DLL
6.43.2.24.0 under Wine 10 have passed. Broker login, trading commands, and real
market-data load still require a separate demo-environment test.
