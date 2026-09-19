package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"go-txmlconnector/internal/bridge"
	"go-txmlconnector/internal/config"
	"go-txmlconnector/internal/native"
)

var version = "development"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		slog.ErrorContext(ctx, "connector process stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	const op = "main.run"
	cfg, err := config.Parse(args)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	var level slog.Level
	if err = level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	if err = os.MkdirAll(cfg.NativeLogPath, 0700); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	lib, err := native.Load(cfg.DLLPath, cfg.Bridge.MaxMessageBytes)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	session, err := bridge.NewSession(native.NewAdapter(lib, cfg.NativeLogPath, cfg.NativeLogLevel), cfg.Bridge, bridge.NewMetrics(registry))
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	defer grpcListener.Close()
	httpListener, err := net.Listen("tcp", cfg.HTTPAddress)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	defer httpListener.Close()
	grpcServer := bridge.NewGRPCServer(session)
	httpServer := &http.Server{Handler: bridge.Handler(session, registry), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	errorsCh := make(chan error, 2)
	go session.Run()
	startupCtx, startupCancel := context.WithTimeout(ctx, cfg.ShutdownTimeout)
	err = session.WaitStarted(startupCtx)
	startupCancel()
	if err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cleanupCancel()
		_ = session.Close(cleanupCtx)
		return fmt.Errorf("%s: %w", op, err)
	}
	go func() { errorsCh <- grpcServer.Serve(grpcListener) }()
	go func() { errorsCh <- httpServer.Serve(httpListener) }()
	slog.InfoContext(ctx, "connector process started", slog.String("version", version), slog.String("grpc_address", cfg.GRPCAddress), slog.String("http_address", cfg.HTTPAddress))
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errorsCh:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	// Close the session first: this wakes RPCs and does not wait for clients to
	// close streams. Force-stop gRPC if flow control prevents graceful shutdown.
	closeCh := make(chan error, 1)
	go func() { closeCh <- session.Close(shutdownCtx) }()
	grpcDone := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(grpcDone) }()
	select {
	case <-grpcDone:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	_ = httpServer.Shutdown(shutdownCtx)
	var closeErr error
	select {
	case closeErr = <-closeCh:
	case <-shutdownCtx.Done():
		closeErr = shutdownCtx.Err()
	}
	if err := errors.Join(serveErr, closeErr); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}
