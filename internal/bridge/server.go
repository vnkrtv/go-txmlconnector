package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	pb "go-txmlconnector/proto"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedConnectServiceServer
	session *Session
}

type connectionKey struct{}
type connectionState struct{ closed atomic.Bool }

// NewGRPCServer installs transport identity tracking as part of the contract:
// the first RPC claims the DLL for this HTTP/2 connection, not merely its stream.
func NewGRPCServer(session *Session) *grpc.Server {
	server := NewServer(session)
	g := grpc.NewServer(grpc.StatsHandler(server), grpc.MaxRecvMsgSize(session.cfg.MaxMessageBytes+1024), grpc.MaxSendMsgSize(session.cfg.MaxMessageBytes+1024))
	pb.RegisterConnectServiceServer(g, server)
	return g
}

func (s *Server) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, connectionKey{}, &connectionState{})
}
func (s *Server) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (s *Server) HandleRPC(context.Context, stats.RPCStats)                       {}
func (s *Server) HandleConn(ctx context.Context, event stats.ConnStats) {
	if _, ended := event.(*stats.ConnEnd); !ended {
		return
	}
	connection, ok := ctx.Value(connectionKey{}).(*connectionState)
	if !ok {
		return
	}
	connection.closed.Store(true)
	s.session.mu.Lock()
	if s.session.owner == connection && s.session.state == stateReady {
		s.session.state = stateResetting
		s.session.metrics.ready.Set(0)
	}
	s.session.scheduleResetLocked()
	s.session.mu.Unlock()
}

func (s *Server) claim(ctx context.Context) error {
	const op = "bridge.Server.claim"
	connection, ok := ctx.Value(connectionKey{}).(*connectionState)
	if !ok {
		return status.Error(codes.Internal, op+": missing transport identity")
	}
	s.session.mu.Lock()
	defer s.session.mu.Unlock()
	if connection.closed.Load() || s.session.state != stateReady {
		return rpcError(codes.FailedPrecondition, "SESSION_NOT_READY", op+": session is not ready")
	}
	if s.session.owner != nil && s.session.owner != connection {
		return rpcError(codes.AlreadyExists, "CLIENT_ALREADY_ATTACHED", op+": DLL session already belongs to another connection")
	}
	s.session.owner = connection
	s.session.activeRPCs++
	return nil
}

// release keeps a disconnected owner reserved until its RPC handlers exit.
func (s *Server) release() {
	s.session.mu.Lock()
	defer s.session.mu.Unlock()
	s.session.activeRPCs--
	s.session.scheduleResetLocked()
}

func NewServer(session *Session) *Server { return &Server{session: session} }

func (s *Server) SendCommand(ctx context.Context, request *pb.SendCommandRequest) (*pb.SendCommandResponse, error) {
	const op = "bridge.Server.SendCommand"
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, op+": missing request")
	}
	if err := s.claim(ctx); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer s.release()
	message, err := s.session.Send(ctx, request.Message)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return &pb.SendCommandResponse{Message: message}, nil
}

func (s *Server) FetchResponseData(_ *pb.DataRequest, stream pb.ConnectService_FetchResponseDataServer) error {
	const op = "bridge.Server.FetchResponseData"
	if err := s.claim(stream.Context()); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	defer s.release()
	session := s.session
	session.mu.Lock()
	if session.subscribed {
		session.mu.Unlock()
		return rpcError(codes.AlreadyExists, "STREAM_ALREADY_ATTACHED", op+": only one subscriber is supported")
	}
	if session.state != stateReady {
		session.mu.Unlock()
		return rpcError(codes.FailedPrecondition, "SESSION_NOT_READY", op+": session is not ready")
	}
	session.subscribed = true
	session.metrics.stream.Set(1)
	session.mu.Unlock()
	defer func() {
		session.mu.Lock()
		session.subscribed = false
		session.metrics.stream.Set(0)
		session.mu.Unlock()
	}()
	if err := stream.SendHeader(metadata.Pairs("txml-stream", "attached")); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	lastConnected := ""
	for {
		select {
		case <-session.stop:
			return rpcError(codes.Unavailable, "SESSION_STOPPING", op+": session stopping")
		case <-session.fault:
			return rpcError(codes.FailedPrecondition, "SESSION_FAULTED", op+": session faulted; reconcile before restarting")
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		case message := <-session.events:
			session.mu.Lock()
			session.eventBytes -= len(message)
			ready := session.state == stateReady
			session.mu.Unlock()
			if !ready {
				return rpcError(codes.FailedPrecondition, "SESSION_FAULTED", op+": session no longer ready")
			}
			root, err := parseXML(message)
			if err != nil {
				session.fail("invalid_callback_xml")
				return rpcError(codes.DataLoss, "INVALID_CALLBACK", op+": invalid callback XML")
			}
			if root.Name.Local == xmlError {
				session.metrics.callbackErrors.Inc()
			}
			if err := stream.Send(&pb.DataResponse{Message: message}); err != nil {
				return fmt.Errorf("%s: %w", op, err)
			}
			session.metrics.delivered.Inc()
			if root.Name.Local == "server_status" {
				connected := "unknown"
				for _, attr := range root.Attr {
					if attr.Name.Local == "connected" {
						switch attr.Value {
						case xmlTrue, xmlFalse, xmlError:
							connected = attr.Value
						}
					}
				}
				value := float64(-1)
				switch connected {
				case xmlTrue:
					value = 1
				case xmlFalse, xmlError:
					value = 0
				}
				session.metrics.brokerConnected.Set(value)
				if connected != lastConnected {
					slog.InfoContext(stream.Context(), "broker status changed", slog.String("connected", connected))
					lastConnected = connected
				}
			}
		}
	}
}

// Handler exposes process liveness, bridge readiness and a private registry.
func Handler(session *Session, registry *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		snapshot := session.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		if snapshot.State != stateReady {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(snapshot)
	})
	registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "txml_client_attached", Help: "Whether the owning gRPC transport is still connected."}, func() float64 {
			if session.Snapshot().ClientAttached {
				return 1
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "txml_command_queue_depth", Help: "Pending command slots, including canceled jobs not yet drained."}, func() float64 { return float64(len(session.commands)) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "txml_event_queue_depth", Help: "Buffered callback messages."}, func() float64 { return float64(session.Snapshot().EventQueue) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "txml_event_queue_bytes", Help: "Bytes held by buffered callback messages."}, func() float64 { return float64(session.Snapshot().EventBytes) }),
	)
	return mux
}
