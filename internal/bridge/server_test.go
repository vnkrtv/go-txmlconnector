package bridge

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go-txmlconnector/proto"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const acceptedResultXML = `<result success="true"/>`

type testConnector struct {
	receive func(string, error)
	send    func(string) (string, error)
	calls   atomic.Int32
	closed  atomic.Bool
	starts  atomic.Int32
	onStart func() error
	onClose func() error
}

func (c *testConnector) Start(receive func(string, error)) error {
	c.starts.Add(1)
	c.receive = receive
	if c.onStart != nil {
		return c.onStart()
	}
	return nil
}
func (c *testConnector) Send(command string) (string, error) { c.calls.Add(1); return c.send(command) }
func (c *testConnector) Close() error {
	c.closed.Store(true)
	if c.onClose != nil {
		return c.onClose()
	}
	return nil
}

func setup(t *testing.T, c *testConnector, cfg Config) (*Session, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	s, err := NewSession(c, cfg, NewMetrics(registry))
	require.NoError(t, err)
	go s.Run()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.WaitStarted(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, s.Close(ctx))
	})
	return s, registry
}

func client(t *testing.T, session *Session) pb.ConnectServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := NewGRPCServer(session)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return pb.NewConnectServiceClient(conn)
}

func TestWireCompatibilityAndErrorPreservation(t *testing.T) {
	for _, tc := range []struct {
		name, response, reason string
		code                   codes.Code
	}{
		{"accepted", `<result success="true" transactionid="42"/>`, "", codes.OK},
		{"rejected", `<result success="false"><message>Недостаточно средств</message></result>`, "", codes.OK},
		{"exception", `<error>Нет подключения к серверу</error>`, "CONNECTOR_ERROR", codes.Internal},
		{"arbitrary response", `<server_status connected="false"/>`, "", codes.OK},
		{"malformed", `<result`, "COMMAND_OUTCOME_UNKNOWN", codes.Internal},
		{"empty", ``, "COMMAND_OUTCOME_UNKNOWN", codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &testConnector{send: func(string) (string, error) { return tc.response, nil }}
			s, _ := setup(t, c, DefaultConfig())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			response, err := client(t, s).SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="neworder"/>`})
			require.Equal(t, tc.code, status.Code(err))
			if err == nil {
				require.Equal(t, tc.response, response.Message)
			} else {
				require.Equal(t, tc.reason, errorReason(err))
			}
			if tc.name == "exception" {
				require.Contains(t, err.Error(), "Нет подключения к серверу")
			}
			require.EqualValues(t, 1, c.calls.Load())
		})
	}
}

func errorReason(err error) string {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.Reason
		}
	}
	return ""
}

func TestNativeFailurePreservesCauseAndPoisonsSession(t *testing.T) {
	c := &testConnector{send: func(string) (string, error) { return "", errors.New("DLL returned null response") }}
	s, _ := setup(t, c, DefaultConfig())
	_, err := s.Send(context.Background(), `<command id="neworder"/>`)
	require.ErrorContains(t, err, "DLL returned null response")
	require.Equal(t, "COMMAND_OUTCOME_UNKNOWN", errorReason(err))
	_, err = s.Send(context.Background(), `<command id="neworder"/>`)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.EqualValues(t, 1, c.calls.Load())
}

func TestInvalidCommandsNeverReachDLL(t *testing.T) {
	c := &testConnector{send: func(string) (string, error) { t.Error("unexpected native call"); return "", nil }}
	cfg := DefaultConfig()
	cfg.MaxMessageBytes = 128
	s, _ := setup(t, c, cfg)
	for _, input := range []string{"", `<command/>`, `<result/>`, `<command id="neworder"/><command id="neworder"/>`, `<command id="neworder">`, "<command id=\"neworder\">\x00</command>", strings.Repeat("x", 129), `<!DOCTYPE command><command id="neworder"/>`} {
		_, err := s.Send(context.Background(), input)
		require.Equal(t, codes.InvalidArgument, status.Code(err), input)
	}
	require.Zero(t, c.calls.Load())
}

func TestCancellationWhileQueuedDoesNotDispatchOrFault(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c := &testConnector{send: func(string) (string, error) { close(entered); <-release; return acceptedResultXML, nil }}
	s, _ := setup(t, c, DefaultConfig())
	first := make(chan error, 1)
	go func() { _, err := s.Send(context.Background(), `<command id="neworder"/>`); first <- err }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() { _, err := s.Send(ctx, `<command id="neworder"/>`); second <- err }()
	require.Eventually(t, func() bool { return len(s.commands) == 1 }, time.Second, time.Millisecond)
	cancel()
	err := <-second
	require.Equal(t, "COMMAND_NOT_DISPATCHED", errorReason(err))
	close(release)
	require.NoError(t, <-first)
	require.Equal(t, "ready", s.Snapshot().State)
	require.Eventually(t, func() bool { return len(s.commands) == 0 }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, c.calls.Load())
}

func TestCancellationAfterDispatchHasUnknownOutcome(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c := &testConnector{send: func(string) (string, error) { close(entered); <-release; return acceptedResultXML, nil }}
	s, _ := setup(t, c, DefaultConfig())
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Send(ctx, `<command id="neworder"/>`); done <- err }()
	<-entered
	cancel()
	require.Equal(t, "COMMAND_OUTCOME_UNKNOWN", errorReason(<-done))
	require.Equal(t, "faulted", s.Snapshot().State)
	_, err := s.Send(context.Background(), `<command id="neworder"/>`)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.EqualValues(t, 1, c.calls.Load())
}

func TestCommandQueueIsBounded(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c := &testConnector{send: func(string) (string, error) { close(entered); <-release; return acceptedResultXML, nil }}
	cfg := DefaultConfig()
	cfg.CommandQueue = 1
	s, _ := setup(t, c, cfg)
	first := make(chan error, 1)
	go func() { _, err := s.Send(context.Background(), `<command id="neworder"/>`); first <- err }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() { _, err := s.Send(ctx, `<command id="neworder"/>`); second <- err }()
	require.Eventually(t, func() bool { return len(s.commands) == 1 }, time.Second, time.Millisecond)
	_, err := s.Send(context.Background(), `<command id="neworder"/>`)
	require.Equal(t, "COMMAND_QUEUE_FULL", errorReason(err))
	cancel()
	<-second
	close(release)
	require.NoError(t, <-first)
}

func TestStreamEventsAndSingleConsumer(t *testing.T) {
	c := &testConnector{send: func(string) (string, error) { return acceptedResultXML, nil }}
	s, _ := setup(t, c, DefaultConfig())
	cl := client(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := cl.FetchResponseData(ctx, &pb.DataRequest{})
	require.NoError(t, err)
	_, err = stream.Header()
	require.NoError(t, err)
	second, err := cl.FetchResponseData(ctx, &pb.DataRequest{})
	require.NoError(t, err)
	_, err = second.Recv()
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	for _, event := range []string{`<?xml version="1.0"?><orders/>`, `<error>Внутренняя ошибка</error>`} {
		c.receive(event, nil)
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.Equal(t, event, response.Message)
	}
	cancel()
	require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return !s.subscribed }, time.Second, time.Millisecond)
	require.Equal(t, stateReady, s.Snapshot().State)
	reopened, err := cl.FetchResponseData(context.Background(), &pb.DataRequest{})
	require.NoError(t, err)
	_, err = reopened.Header()
	require.NoError(t, err)
	c.receive(`<orders/>`, nil)
	message, err := reopened.Recv()
	require.NoError(t, err)
	require.Equal(t, `<orders/>`, message.Message)
	require.Zero(t, c.calls.Load(), "stream closure must not issue implicit disconnect")
}

func TestOverflowPoisonsSessionAndReadiness(t *testing.T) {
	c := &testConnector{}
	cfg := DefaultConfig()
	cfg.EventQueue = 1
	s, registry := setup(t, c, cfg)
	handler := Handler(s, registry)
	c.receive(`<orders/>`, nil)
	c.receive(`<trades/>`, nil)
	require.Equal(t, "event_queue_full", s.Snapshot().Reason)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Contains(t, w.Body.String(), `txml_session_faults_total{reason="event_queue_full"} 1`)
	require.Contains(t, w.Body.String(), "txml_ready 0")
}

func TestCallbackReadFailurePoisonsSession(t *testing.T) {
	c := &testConnector{}
	s, _ := setup(t, c, DefaultConfig())
	c.receive("", errors.New("native.Read: invalid pointer"))
	require.Equal(t, "callback_read_failed", s.Snapshot().Reason)
}

func TestConcurrentCommandsAreSerialized(t *testing.T) {
	var active atomic.Int32
	var overlap atomic.Bool
	c := &testConnector{send: func(string) (string, error) {
		if active.Add(1) != 1 {
			overlap.Store(true)
		}
		defer active.Add(-1)
		return acceptedResultXML, nil
	}}
	s, _ := setup(t, c, DefaultConfig())
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Send(context.Background(), `<command id="neworder"/>`)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	require.False(t, overlap.Load())
	require.EqualValues(t, 32, c.calls.Load())
}

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func TestLogsAndMetricsDoNotContainPayloads(t *testing.T) {
	var output lockedBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(old)
	c := &testConnector{send: func(string) (string, error) { return `<error>secret-password</error>`, nil }}
	s, registry := setup(t, c, DefaultConfig())
	_, err := s.Send(context.Background(), `<command id="custom_command"><password>secret-password</password></command>`)
	require.ErrorContains(t, err, "secret-password", "error text must reach the caller")
	w := httptest.NewRecorder()
	Handler(s, registry).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	output.mu.Lock()
	logs := output.String()
	output.mu.Unlock()
	for _, text := range []string{logs, w.Body.String()} {
		require.NotContains(t, text, "secret-password")
		require.Contains(t, text, "custom_command")
	}
}

func TestUnknownCommandIsPassedUnchangedAndRejectionPreserved(t *testing.T) {
	command := `<command id="future_dll_command"><parameter>value</parameter></command>`
	rejection := `<result success="false"><message>Unsupported command</message></result>`
	var received string
	c := &testConnector{send: func(message string) (string, error) {
		received = message
		return rejection, nil
	}}
	s, registry := setup(t, c, DefaultConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client(t, s).SendCommand(ctx, &pb.SendCommandRequest{Message: command})
	require.NoError(t, err)
	require.Equal(t, command, received)
	require.Equal(t, rejection, response.Message)
	w := httptest.NewRecorder()
	Handler(s, registry).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Contains(t, w.Body.String(), `txml_commands_total{command="future_dll_command",outcome="rejected"} 1`)
}

func TestShutdownClosesActiveStream(t *testing.T) {
	c := &testConnector{}
	s, _ := setup(t, c, DefaultConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client(t, s).FetchResponseData(ctx, &pb.DataRequest{})
	require.NoError(t, err)
	_, err = stream.Header()
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))
	_, err = stream.Recv()
	require.Error(t, err)
	require.NotEqual(t, io.EOF, err)
	require.True(t, c.closed.Load())
}

func TestServerDeadlineDoesNotRetryNativeCommand(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c := &testConnector{send: func(string) (string, error) { close(entered); <-release; return acceptedResultXML, nil }}
	cfg := DefaultConfig()
	cfg.CommandTimeout = 50 * time.Millisecond
	s, _ := setup(t, c, cfg)
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client(t, s).SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="neworder"/>`})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Equal(t, "COMMAND_OUTCOME_UNKNOWN", errorReason(err))
	require.Equal(t, "faulted", s.Snapshot().State)
	require.EqualValues(t, 1, c.calls.Load())
}

func TestHungNativeCallHasBoundedShutdown(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c := &testConnector{send: func(string) (string, error) { close(entered); <-release; return acceptedResultXML, nil }}
	s, _ := setup(t, c, DefaultConfig())
	defer close(release)
	requestDone := make(chan error, 1)
	go func() { _, err := s.Send(context.Background(), `<command id="neworder"/>`); requestDone <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.Close(ctx), context.DeadlineExceeded)
	require.Equal(t, "COMMAND_OUTCOME_UNKNOWN", errorReason(<-requestDone))
}

func TestCallbackByteLimit(t *testing.T) {
	c := &testConnector{}
	cfg := DefaultConfig()
	cfg.MaxMessageBytes = 20
	cfg.MaxEventBytes = 20
	s, _ := setup(t, c, cfg)
	for i := 0; i < 3; i++ {
		c.receive(`<orders/>`, nil)
	}
	require.Equal(t, "event_bytes_exceeded", s.Snapshot().Reason)
}

func TestMalformedCallbackTerminatesStream(t *testing.T) {
	c := &testConnector{}
	s, _ := setup(t, c, DefaultConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client(t, s).FetchResponseData(ctx, &pb.DataRequest{})
	require.NoError(t, err)
	_, err = stream.Header()
	require.NoError(t, err)
	c.receive(`<orders`, nil)
	_, err = stream.Recv()
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.Equal(t, "invalid_callback_xml", s.Snapshot().Reason)
}

func TestCancelledQueueEntryCannotDispatchBeforeWaiterWakes(t *testing.T) {
	c := &testConnector{send: func(string) (string, error) { t.Error("canceled command dispatched"); return "", nil }}
	s, _ := setup(t, c, DefaultConfig())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := &job{phase: "queued", done: make(chan struct{}), canceled: ctx.Done()}
	s.commands <- j
	select {
	case <-j.done:
	case <-time.After(time.Second):
		t.Fatal("executor stuck")
	}
	require.Equal(t, "COMMAND_NOT_DISPATCHED", errorReason(j.err))
	require.Zero(t, c.calls.Load())
}

func TestOnlyOwnerConnectionCanSendCommandsOrSubscribe(t *testing.T) {
	c := &testConnector{send: func(string) (string, error) { return acceptedResultXML, nil }}
	s, _ := setup(t, c, DefaultConfig())
	listener := bufconn.Listen(1 << 20)
	server := NewGRPCServer(s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	dial := func() *grpc.ClientConn {
		conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	owner, other := dial(), dial()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request := &pb.SendCommandRequest{Message: `<command id="server_status"/>`}
	_, err := pb.NewConnectServiceClient(owner).SendCommand(ctx, request)
	require.NoError(t, err)
	_, err = pb.NewConnectServiceClient(other).SendCommand(ctx, request)
	require.Equal(t, "CLIENT_ALREADY_ATTACHED", errorReason(err))
	stream, err := pb.NewConnectServiceClient(other).FetchResponseData(ctx, &pb.DataRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, "CLIENT_ALREADY_ATTACHED", errorReason(err))
	require.NoError(t, other.Close())
	_, err = pb.NewConnectServiceClient(owner).SendCommand(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 2, c.calls.Load())
	require.NoError(t, owner.Close())
	require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.state == stateReady && s.owner == nil }, time.Second, time.Millisecond)
	require.False(t, s.Snapshot().ClientAttached)
	_, err = pb.NewConnectServiceClient(dial()).SendCommand(ctx, request)
	require.NoError(t, err, "a new owner can connect after the old connection closes")
	require.True(t, c.closed.Load(), "old native session must be closed before handover")
	require.EqualValues(t, 3, c.calls.Load())
}

func TestOwnerResetWaitsForCleanupAndDiscardsOldEvents(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var closes atomic.Int32
	c := &testConnector{send: func(string) (string, error) { return acceptedResultXML, nil }}
	c.onClose = func() error {
		if closes.Add(1) == 1 {
			close(entered)
			<-resume
		}
		return nil
	}
	s, _ := setup(t, c, DefaultConfig())
	server := NewServer(s)
	owner := &connectionState{}
	ctx := context.WithValue(context.Background(), connectionKey{}, owner)
	_, err := server.SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="server_status"/>`})
	require.NoError(t, err)
	s.receive(`<old/>`, nil)
	server.HandleConn(ctx, &stats.ConnEnd{})
	<-entered
	s.receive(`<late/>`, nil)
	other := context.WithValue(context.Background(), connectionKey{}, &connectionState{})
	_, err = server.SendCommand(other, &pb.SendCommandRequest{Message: `<command id="server_status"/>`})
	require.Equal(t, "SESSION_NOT_READY", errorReason(err))
	close(resume)
	require.Eventually(t, func() bool { return s.Snapshot().State == stateReady }, time.Second, time.Millisecond)
	require.Zero(t, s.Snapshot().EventQueue)
	require.Zero(t, s.Snapshot().EventBytes)
	require.EqualValues(t, 2, c.starts.Load())
	_, err = server.SendCommand(other, &pb.SendCommandRequest{Message: `<command id="server_status"/>`})
	require.NoError(t, err)
	// A repeated notification from the previous transport cannot release its successor.
	server.HandleConn(ctx, &stats.ConnEnd{})
	require.True(t, s.Snapshot().ClientAttached)
	require.Equal(t, stateReady, s.Snapshot().State)
}

func TestOwnerResetFailureRemainsUnavailable(t *testing.T) {
	c := &testConnector{send: func(string) (string, error) { return acceptedResultXML, nil }}
	c.onStart = func() error {
		if c.starts.Load() > 1 {
			return errors.New("initialization failed")
		}
		return nil
	}
	s, _ := setup(t, c, DefaultConfig())
	server := NewServer(s)
	ctx := context.WithValue(context.Background(), connectionKey{}, &connectionState{})
	_, err := server.SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="server_status"/>`})
	require.NoError(t, err)
	server.HandleConn(ctx, &stats.ConnEnd{})
	require.Eventually(t, func() bool { return s.Snapshot().Reason == "reset_initialize_failed" }, time.Second, time.Millisecond)
	other := context.WithValue(context.Background(), connectionKey{}, &connectionState{})
	_, err = server.SendCommand(other, &pb.SendCommandRequest{Message: `<command id="server_status"/>`})
	require.Equal(t, "SESSION_NOT_READY", errorReason(err))
}
