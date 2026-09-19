// Package native owns DLL memory and serializes all DLL API calls. Callbacks must
// never acquire callMu: a DLL call can wait for the callback to return.
package native

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Library is the native ABI boundary. The error returned by a native call is
// GetLastError, not its success indicator. Read is a memory copy, not a DLL call.
type Library interface {
	Initialize(string, int) (uintptr, error)
	SetCallback(func(uintptr) uintptr) (bool, error)
	SendCommand(string) (uintptr, error)
	Read(uintptr) (string, error)
	FreeMemory(uintptr) bool
	UnInitialize() (uintptr, error)
}

type Adapter struct {
	lib      Library
	logPath  string
	logLevel int
	callMu   sync.Mutex
	freeMu   sync.Mutex
	pending  []uintptr
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	receive  func(string, error)
}

func NewAdapter(lib Library, logPath string, logLevel int) *Adapter {
	return &Adapter{lib: lib, logPath: logPath, logLevel: logLevel,
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
}

// Start/Send/Close are owned by the session executor; Close follows successful Start.
func (a *Adapter) Start(receive func(string, error)) error {
	const op = "native.Adapter.Start"
	if strings.ContainsRune(a.logPath, 0) {
		return fmt.Errorf("%s: log path contains NUL", op)
	}
	a.callMu.Lock()
	defer a.callMu.Unlock()
	a.stop = make(chan struct{})
	a.done = make(chan struct{})
	a.wake = make(chan struct{}, 1)
	a.receive = receive
	p, _ := a.lib.Initialize(a.logPath, a.logLevel)
	if p != 0 {
		message, err := a.readAndFree(p)
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		return fmt.Errorf("%s: Initialize: %s", op, message)
	}
	ok, _ := a.lib.SetCallback(a.callback)
	if !ok {
		p, _ = a.lib.UnInitialize()
		if p != 0 {
			_, _ = a.readAndFree(p)
		}
		_ = a.drain()
		return fmt.Errorf("%s: SetCallback returned false", op)
	}
	go a.cleaner()
	return nil
}

func (a *Adapter) Send(command string) (string, error) {
	const op = "native.Adapter.Send"
	if strings.ContainsRune(command, 0) {
		return "", fmt.Errorf("%s: command contains NUL", op)
	}
	a.callMu.Lock()
	defer a.callMu.Unlock()
	// Exactly one call. A stale GetLastError must not discard or repeat a command.
	p, _ := a.lib.SendCommand(command)
	if p == 0 {
		return "", fmt.Errorf("%s: %w", op, ErrNoResponse)
	}
	message, err := a.readAndFree(p)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}
	return message, nil
}

func (a *Adapter) Close() error {
	const op = "native.Adapter.Close"
	close(a.stop)
	<-a.done
	a.callMu.Lock()
	defer a.callMu.Unlock()
	// UnInitialize joins the DLL callback thread. Only then can the last batch
	// of retired callback pointers be freed without racing another callback.
	p, _ := a.lib.UnInitialize()
	var shutdownErr error
	if p != 0 {
		message, err := a.readAndFree(p)
		if err != nil {
			shutdownErr = err
		} else {
			shutdownErr = fmt.Errorf("UnInitialize: %s", message)
		}
	}
	if err := errors.Join(shutdownErr, a.drain()); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (a *Adapter) readAndFree(p uintptr) (message string, err error) {
	const op = "native.Adapter.readAndFree"
	defer func() {
		if !a.lib.FreeMemory(p) {
			err = errors.Join(err, fmt.Errorf("%s: %w", op, ErrMemory))
		}
	}()
	message, err = a.lib.Read(p)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}
	return message, nil
}

func (a *Adapter) callback(p uintptr) (result uintptr) {
	// Never include panic values or native payloads in logs.
	defer func() {
		if recover() != nil {
			a.receive("", errors.New("native.Adapter.callback: panic"))
			result = 0
		}
	}()
	if p == 0 {
		a.receive("", errors.New("native.Adapter.callback: null pointer"))
		return 0
	}
	defer func() {
		a.freeMu.Lock()
		a.pending = append(a.pending, p)
		a.freeMu.Unlock()
		select {
		case a.wake <- struct{}{}:
		default:
		}
	}()
	message, err := a.lib.Read(p)
	a.receive(message, err)
	return 1
}

func (a *Adapter) cleaner() {
	defer close(a.done)
	for {
		select {
		case <-a.stop:
			return
		case <-a.wake:
			a.callMu.Lock()
			err := a.drain()
			a.callMu.Unlock()
			if err != nil {
				a.receive("", err)
			}
		}
	}
}

func (a *Adapter) drain() error {
	const op = "native.Adapter.drain"
	a.freeMu.Lock()
	pending := a.pending
	a.pending = nil
	a.freeMu.Unlock()
	var failed bool
	for _, p := range pending {
		if !a.lib.FreeMemory(p) {
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("%s: %w", op, ErrMemory)
	}
	return nil
}
