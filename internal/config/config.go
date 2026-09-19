package config

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"go-txmlconnector/internal/bridge"
)

type Config struct {
	DLLPath         string
	NativeLogPath   string
	NativeLogLevel  int
	LogLevel        string
	GRPCAddress     string
	HTTPAddress     string
	ShutdownTimeout time.Duration
	Bridge          bridge.Config
}

func Parse(args []string) (Config, error) {
	const op = "config.Parse"
	c := Config{Bridge: bridge.DefaultConfig()}
	f := flag.NewFlagSet("go-txmlconnector", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.StringVar(&c.DLLPath, "dll", "txmlconnector64-6.43.2.24.0.dll", "Path to pinned DLL")
	f.StringVar(&c.NativeLogPath, "native-log-dir", "logs", "Private native DLL log directory")
	f.IntVar(&c.NativeLogLevel, "native-log-level", 1, "DLL log level (1..3)")
	f.StringVar(&c.LogLevel, "log-level", "info", "JSON application log level")
	f.StringVar(&c.GRPCAddress, "grpc-address", "127.0.0.1:50051", "gRPC listen address")
	f.StringVar(&c.HTTPAddress, "http-address", "127.0.0.1:9090", "Metrics and health address")
	f.DurationVar(&c.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "Maximum time to stop the native process")
	f.DurationVar(&c.Bridge.CommandTimeout, "command-timeout", c.Bridge.CommandTimeout, "Server command deadline")
	f.IntVar(&c.Bridge.CommandQueue, "command-queue", c.Bridge.CommandQueue, "Maximum queued commands")
	f.IntVar(&c.Bridge.EventQueue, "event-queue", c.Bridge.EventQueue, "Maximum buffered callbacks")
	f.IntVar(&c.Bridge.MaxMessageBytes, "max-message-bytes", c.Bridge.MaxMessageBytes, "Maximum XML message bytes")
	f.IntVar(&c.Bridge.MaxEventBytes, "max-event-bytes", c.Bridge.MaxEventBytes, "Maximum buffered callback bytes")
	if err := f.Parse(args); err != nil {
		return c, fmt.Errorf("%s: %w", op, err)
	}
	if f.NArg() != 0 {
		return c, fmt.Errorf("%s: unexpected positional arguments", op)
	}
	if c.NativeLogLevel < 1 || c.NativeLogLevel > 3 || c.ShutdownTimeout <= 0 || c.Bridge.CommandTimeout <= 0 || c.Bridge.CommandQueue < 1 || c.Bridge.EventQueue < 1 || c.Bridge.MaxMessageBytes < 1 || c.Bridge.MaxMessageBytes > 64<<20 || c.Bridge.MaxEventBytes < c.Bridge.MaxMessageBytes {
		return c, fmt.Errorf("%s: invalid limit or timeout", op)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return c, fmt.Errorf("%s: invalid log level", op)
	}
	if c.DLLPath == "" || c.NativeLogPath == "" || strings.ContainsRune(c.DLLPath+c.NativeLogPath, 0) {
		return c, fmt.Errorf("%s: invalid native path", op)
	}
	var err error
	c.DLLPath, err = filepath.Abs(c.DLLPath)
	if err != nil {
		return c, fmt.Errorf("%s: %w", op, err)
	}
	c.NativeLogPath, err = filepath.Abs(c.NativeLogPath)
	if err != nil {
		return c, fmt.Errorf("%s: %w", op, err)
	}
	return c, nil
}
