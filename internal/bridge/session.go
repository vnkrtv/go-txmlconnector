package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	stateResetting = "resetting"
	stateStarting  = "starting"
	stateReady     = "ready"
	stateFaulted   = "faulted"
	stateStopping  = "stopping"
	stateStopped   = "stopped"

	phaseQueued    = "queued"
	phaseRunning   = "running"
	phaseCompleted = "completed"
)

// Connector is owned by one executor. Receive must copy/transfer ownership of
// its message and must not block on the executor. Close joins native callbacks.
type Connector interface {
	Start(receive func(string, error)) error
	Send(string) (string, error)
	Close() error
}

type Config struct {
	CommandQueue    int
	EventQueue      int
	MaxMessageBytes int
	MaxEventBytes   int
	CommandTimeout  time.Duration
}

func DefaultConfig() Config {
	return Config{CommandQueue: 64, EventQueue: 1024, MaxMessageBytes: 4 << 20,
		MaxEventBytes: 32 << 20, CommandTimeout: 15 * time.Second}
}

type job struct {
	mu       sync.Mutex
	phase    string
	message  string
	result   string
	err      error
	done     chan struct{}
	canceled <-chan struct{}
}

type Session struct {
	connector     Connector
	reset         chan struct{}
	resetQueued   bool
	activeRPCs    int
	nativeStarted bool
	cfg           Config
	metrics       *Metrics
	commands      chan *job
	events        chan string
	stop          chan struct{}
	done          chan struct{}
	fault         chan struct{}
	mu            sync.Mutex
	state         string
	reason        string
	eventBytes    int
	subscribed    bool
	owner         *connectionState
	closeErr      error
	startupErr    error
	started       chan struct{}
	stopOnce      sync.Once
	sequence      atomic.Uint64
}

func NewSession(connector Connector, cfg Config, metrics *Metrics) (*Session, error) {
	const op = "bridge.NewSession"
	if cfg.CommandQueue < 1 || cfg.EventQueue < 1 || cfg.MaxMessageBytes < 1 || cfg.MaxEventBytes < cfg.MaxMessageBytes || cfg.CommandTimeout <= 0 {
		return nil, fmt.Errorf("%s: invalid queue, size or timeout configuration", op)
	}
	return &Session{connector: connector, cfg: cfg, metrics: metrics, state: stateStarting,
		reset: make(chan struct{}, 1), commands: make(chan *job, cfg.CommandQueue), events: make(chan string, cfg.EventQueue),
		stop: make(chan struct{}), done: make(chan struct{}), fault: make(chan struct{}), started: make(chan struct{})}, nil
}

// Run must be called exactly once. It also owns initialization and cleanup.
func (s *Session) Run() {
	defer close(s.done)
	go s.logFault()
	if err := s.connector.Start(s.receive); err != nil {
		s.startupErr = err
		s.fail("initialize_failed")
		close(s.started)
		return
	}
	s.nativeStarted = true
	defer func() {
		var err error
		if s.nativeStarted {
			err = s.connector.Close()
		}
		s.mu.Lock()
		s.closeErr = err
		s.state = stateStopped
		s.metrics.ready.Set(0)
		s.mu.Unlock()
		slog.InfoContext(context.Background(), "connector stopped", slog.Bool("cleanup_failed", err != nil))
	}()
	s.mu.Lock()
	if s.state == stateStarting {
		s.state = stateReady
		s.metrics.ready.Set(1)
	}
	s.mu.Unlock()
	close(s.started)
	slog.InfoContext(context.Background(), "connector initialized")
	for {
		select {
		case <-s.stop:
			return
		case <-s.reset:
			s.resetOwner()
		case j := <-s.commands:
			s.execute(j)
		}
	}
}

// scheduleResetLocked runs only after the old transport and all its RPCs end.
func (s *Session) scheduleResetLocked() {
	if s.state == stateResetting && s.activeRPCs == 0 && !s.resetQueued {
		s.resetQueued = true
		s.reset <- struct{}{}
	}
}

// resetOwner runs on the same executor as commands. UnInitialize ends the old
// broker session and joins callbacks before its buffered events are discarded.
func (s *Session) resetOwner() {
	s.mu.Lock()
	reset := s.state == stateResetting
	s.mu.Unlock()
	if !reset {
		return
	}
	slog.InfoContext(context.Background(), "client disconnected; resetting connector")
	err := s.connector.Close()
	s.nativeStarted = false
	if err != nil {
		s.fail("reset_close_failed")
		return
	}
	s.mu.Lock()
	for len(s.events) > 0 {
		<-s.events
	}
	s.eventBytes = 0
	s.metrics.brokerConnected.Set(-1)
	s.mu.Unlock()
	if err = s.connector.Start(s.receive); err != nil {
		s.fail("reset_initialize_failed")
		return
	}
	s.nativeStarted = true
	s.mu.Lock()
	if s.state == stateResetting {
		s.owner = nil
		s.resetQueued = false
		s.state = stateReady
		s.metrics.ready.Set(1)
	}
	s.mu.Unlock()
	slog.InfoContext(context.Background(), "connector ready for next client")
}

func (s *Session) execute(j *job) {
	j.mu.Lock()
	if j.phase != phaseQueued {
		j.mu.Unlock()
		return
	}
	select {
	case <-j.canceled:
		j.phase, j.err = phaseCompleted, rpcError(codes.Canceled, "COMMAND_NOT_DISPATCHED", "command canceled in queue; command not dispatched")
		close(j.done)
		j.mu.Unlock()
		return
	default:
	}
	s.mu.Lock()
	ready := s.state == stateReady
	s.mu.Unlock()
	if !ready {
		j.phase, j.err = phaseCompleted, rpcError(codes.FailedPrecondition, "SESSION_NOT_READY", "session is not ready; command not dispatched")
		close(j.done)
		j.mu.Unlock()
		return
	}
	j.phase = phaseRunning
	j.mu.Unlock()
	message, err := s.connector.Send(j.message)
	faultReason := ""
	if err != nil {
		faultReason = "native_command_failed"
		err = rpcError(codes.Internal, "COMMAND_OUTCOME_UNKNOWN", err.Error()+"; outcome unknown; reconcile before restarting")
	} else {
		kind, text, parseErr := responseKind(message)
		if parseErr != nil || len(message) > s.cfg.MaxMessageBytes {
			faultReason = "invalid_response"
			err = rpcError(codes.Internal, "COMMAND_OUTCOME_UNKNOWN", "invalid DLL response; outcome unknown")
		} else if kind == "connector_error" {
			err = rpcError(codes.Internal, "CONNECTOR_ERROR", text)
		}
	}
	j.mu.Lock()
	j.phase, j.result, j.err = phaseCompleted, message, err
	if faultReason != "" {
		s.fail(faultReason)
	}
	close(j.done)
	j.mu.Unlock()
}

func (s *Session) Send(ctx context.Context, message string) (response string, err error) {
	const op = "bridge.Session.Send"
	name := "invalid"
	started := time.Now()
	id := s.sequence.Add(1)
	defer func() {
		outcome := status.Code(err).String()
		reason := ""
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				reason = info.Reason
			}
		}
		if err == nil {
			outcome, _, _ = responseKind(response)
		}
		s.metrics.commands.WithLabelValues(name, outcome).Inc()
		s.metrics.duration.WithLabelValues(name).Observe(time.Since(started).Seconds())
		slog.InfoContext(ctx, "command finished", slog.Uint64("request_id", id), slog.String("command", name),
			slog.String("outcome", outcome), slog.String("reason", reason), slog.Duration("duration", time.Since(started)))
	}()
	if len(message) > s.cfg.MaxMessageBytes {
		return "", rpcError(codes.InvalidArgument, "INVALID_COMMAND", op+": command exceeds byte limit")
	}
	name, err = commandName(message)
	if err != nil {
		name = "invalid"
		return "", rpcError(codes.InvalidArgument, "INVALID_COMMAND", err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CommandTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return "", status.FromContextError(ctx.Err()).Err()
	}
	s.mu.Lock()
	ready := s.state == stateReady
	s.mu.Unlock()
	if !ready {
		return "", rpcError(codes.FailedPrecondition, "SESSION_NOT_READY", op+": session is not ready; command not dispatched")
	}
	j := &job{phase: phaseQueued, message: message, done: make(chan struct{}), canceled: ctx.Done()}
	select {
	case s.commands <- j:
	default:
		return "", rpcError(codes.ResourceExhausted, "COMMAND_QUEUE_FULL", op+": command queue is full; command not dispatched")
	}
	select {
	case <-j.done:
		return j.result, j.err
	case <-ctx.Done():
		return s.abandon(j, status.Code(status.FromContextError(ctx.Err()).Err()), "command context ended")
	case <-s.fault:
		return s.abandon(j, codes.FailedPrecondition, "session faulted")
	case <-s.stop:
		return s.abandon(j, codes.Unavailable, "session stopping")
	}
}

func (s *Session) abandon(j *job, code codes.Code, message string) (string, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.phase == phaseCompleted {
		return j.result, j.err
	}
	if j.phase == phaseQueued {
		j.phase = "canceled"
		return "", rpcError(code, "COMMAND_NOT_DISPATCHED", message+"; command not dispatched")
	}
	s.fail("command_outcome_unknown")
	return "", rpcError(code, "COMMAND_OUTCOME_UNKNOWN", message+" after dispatch; outcome unknown; reconcile before restarting")
}

func (s *Session) receive(message string, err error) {
	if err != nil {
		s.fail("callback_read_failed")
		return
	}
	s.metrics.events.Inc()
	s.mu.Lock()
	if s.state == stateResetting || s.state == stateFaulted || s.state == stateStopping || s.state == stateStopped {
		s.mu.Unlock()
		return
	}
	if len(message) == 0 || len(message) > s.cfg.MaxMessageBytes {
		s.mu.Unlock()
		s.fail("invalid_callback_size")
		return
	}
	if s.eventBytes+len(message) > s.cfg.MaxEventBytes {
		s.mu.Unlock()
		s.fail("event_bytes_exceeded")
		return
	}
	select {
	case s.events <- message:
		s.eventBytes += len(message)
		s.mu.Unlock()
	default:
		s.mu.Unlock()
		s.fail("event_queue_full")
	}
}

func (s *Session) fail(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateFaulted || s.state == stateStopping || s.state == stateStopped {
		return
	}
	s.state, s.reason = stateFaulted, reason
	s.metrics.ready.Set(0)
	s.metrics.faults.WithLabelValues(reason).Inc()
	close(s.fault)
}

// Logging can block on stdout. Never do it on the DLL callback thread.
func (s *Session) logFault() {
	select {
	case <-s.fault:
		slog.ErrorContext(context.Background(), "connector session faulted", slog.String("reason", s.Snapshot().Reason))
	case <-s.done:
	}
}

type Snapshot struct {
	State          string `json:"state"`
	Reason         string `json:"reason,omitempty"`
	CommandQueue   int    `json:"command_queue"`
	EventQueue     int    `json:"event_queue"`
	EventBytes     int    `json:"event_bytes"`
	ClientAttached bool   `json:"client_attached"`
}

func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{State: s.state, Reason: s.reason, CommandQueue: len(s.commands), EventQueue: len(s.events), EventBytes: s.eventBytes, ClientAttached: s.owner != nil && !s.owner.closed.Load()}
}

func (s *Session) WaitStarted(ctx context.Context) error {
	const op = "bridge.Session.WaitStarted"
	select {
	case <-s.started:
		if s.startupErr != nil {
			return fmt.Errorf("%s: %w", op, s.startupErr)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}

func (s *Session) Close(ctx context.Context) error {
	const op = "bridge.Session.Close"
	s.stopOnce.Do(func() {
		s.mu.Lock()
		if s.state != stateStopped {
			s.state = stateStopping
		}
		s.metrics.ready.Set(0)
		s.mu.Unlock()
		close(s.stop)
	})
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		s.state = stateStopped
		if s.closeErr != nil {
			return fmt.Errorf("%s: %w", op, s.closeErr)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}

func rpcError(code codes.Code, reason, message string) error {
	s := status.New(code, message)
	withDetails, err := s.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "txmlconnector"})
	if err != nil {
		return s.Err()
	}
	return withDetails.Err()
}
