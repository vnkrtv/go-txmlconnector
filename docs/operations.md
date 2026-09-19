# Operations

## Runtime and deployment

The native runtime is Windows amd64. The Docker image runs the Windows executable
under Wine 10 on Linux amd64. The DLL is provided separately; pin its version and
checksum along with the image version. Build commands, flags, and protobuf
generation instructions are in the [README](../README.md).

Run one bridge process per DLL session with one controlling client. Keep gRPC
and HTTP listeners on a trusted network; the service does not implement TLS or
client authentication. Compose binds published ports to localhost and disables
automatic restart. Set the external stop timeout above `-shutdown-timeout` and
verify signal delivery in the target Windows/Wine environment.

## Health, metrics, and logs

| Endpoint | Meaning |
|---|---|
| `/livez` | HTTP 200 while the HTTP server responds |
| `/readyz` | HTTP 200 in `ready`, otherwise 503; reports state, reason, queue usage, and owner attachment |
| `/metrics` | Prometheus metrics, including Go/process collectors |

Readiness does not establish broker connectivity. The latest delivered
`server_status` is reflected in `txml_broker_connected`: `1` for connected,
`0` for disconnected/error, and `-1` before a known status.

| Metrics | Use |
|---|---|
| `txml_commands_total`, `txml_command_duration_seconds` | Command outcomes and latency, including queue time |
| `txml_command_queue_depth` | Waiting command pressure |
| `txml_event_queue_depth`, `txml_event_queue_bytes` | Callback buffer pressure |
| `txml_events_received_total`, `txml_events_sent_total` | Event flow; sends are not client acknowledgements |
| `txml_callback_errors_total` | Connector error events parsed during delivery |
| `txml_session_faults_total` | First terminal fault, labeled by reason |
| `txml_ready`, `txml_stream_active`, `txml_client_attached` | Session and transport state |

Alert on faults, loss of readiness, sustained queue saturation, and increased
command latency. Command labels use the XML `id` directly, without an allowlist.

Application logs are JSON on stdout and include local request ID, command ID,
outcome, and duration. They exclude XML payloads and broker error text. The DLL
writes separate native logs that may contain sensitive XML even at level 1;
restrict access, retention, and collection accordingly.

## Recovery and switching services

Normal client disconnection does not require a container restart. Readiness is
briefly unavailable while the bridge closes and reinitializes the native session;
then a new client can connect. Expect `client disconnected; resetting connector`
and `connector ready for next client` in the logs. Closing only the callback stream
allows the same client connection to subscribe again.

A faulted session cannot resume. Stop new trading actions and preserve logs and
metrics. Reconcile active orders, trades, and unknown command outcomes with the
broker before restarting the bridge and restoring client state. Do not resend
an order solely because its RPC timed out.

When switching implementations, stop commands and the existing session owner
before starting the replacement. Check readiness, establish the client stream,
wait for broker status, and restore state before trading. Rollback follows the
same procedure: stop the replacement and reconcile first. Do not run competing
owners against the same trading login.

## Validation boundaries

Portable unit and gRPC tests run without the DLL and cover errors, cancellation,
queue limits, connection ownership, and concurrency. Native smoke tests exercise
initialization, callbacks, a version request, an unsupported command, memory
release, and shutdown. The live bridge test adds gRPC, health, metrics, and owner
handover, verifying that a new client can send commands and receive callbacks
after the previous owner disconnects.

The tested native combination is DLL 6.43.2.24.0 under Wine 10. These smoke tests
do not establish broker login, trading correctness, or behavior under real
market-data load. Validate those in a separate demo environment before deployment.
