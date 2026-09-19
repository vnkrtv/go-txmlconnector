package native

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// A stateful ABI simulator, including callbacks that run inside SendCommand and
// UnInitialize. It fails on overlapping DLL calls and double-free, not just on
// deviations from an expected sequence of method calls.
type testLibrary struct {
	mu              sync.Mutex
	active          atomic.Int32
	overlap         atomic.Bool
	next            uintptr
	memory          map[uintptr]string
	freed           map[uintptr]int
	callback        func(uintptr) uintptr
	response        string
	initializeError string
	callbackOK      bool
	readError       bool
	freeOK          bool
	emit            bool
	null            bool
	calls           atomic.Int32
	uninitialized   atomic.Bool
}

func newTestLibrary() *testLibrary {
	return &testLibrary{memory: make(map[uintptr]string), freed: make(map[uintptr]int), callbackOK: true, freeOK: true, response: `<result success="true" transactionid="42"/>`}
}

func (l *testLibrary) enter() func() {
	if l.active.Add(1) != 1 {
		l.overlap.Store(true)
	}
	return func() { l.active.Add(-1) }
}

func (l *testLibrary) allocate(message string) uintptr {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.next++
	l.memory[l.next] = message
	return l.next
}

func (l *testLibrary) Initialize(string, int) (uintptr, error) {
	defer l.enter()()
	if l.initializeError != "" {
		return l.allocate(l.initializeError), syscall.Errno(122)
	}
	return 0, syscall.Errno(122)
}
func (l *testLibrary) SetCallback(callback func(uintptr) uintptr) (bool, error) {
	defer l.enter()()
	l.callback = callback
	return l.callbackOK, syscall.Errno(122)
}
func (l *testLibrary) SendCommand(string) (uintptr, error) {
	defer l.enter()()
	l.calls.Add(1)
	if l.emit {
		l.callback(l.allocate(`<orders/>`))
	}
	if l.null {
		return 0, syscall.Errno(122)
	}
	return l.allocate(l.response), syscall.Errno(122)
}
func (l *testLibrary) Read(p uintptr) (string, error) {
	if l.readError {
		return "", errors.New("testLibrary.Read: invalid data")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.memory[p], nil
}
func (l *testLibrary) FreeMemory(p uintptr) bool {
	defer l.enter()()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.freed[p]++
	delete(l.memory, p)
	return l.freeOK
}
func (l *testLibrary) UnInitialize() (uintptr, error) {
	defer l.enter()()
	if l.emit {
		l.callback(l.allocate(`<server_status connected="false"/>`))
	}
	l.uninitialized.Store(true)
	return 0, syscall.Errno(122)
}

func TestStaleLastErrorDoesNotDiscardResponseOrRetry(t *testing.T) {
	l := newTestLibrary()
	l.emit = true
	a := NewAdapter(l, "logs", 1)
	var received atomic.Int32
	require.NoError(t, a.Start(func(message string, err error) { require.NoError(t, err); received.Add(1) }))
	response, err := a.Send(`<command id="neworder"/>`)
	require.NoError(t, err)
	require.Equal(t, l.response, response)
	require.NoError(t, a.Close())
	require.EqualValues(t, 1, l.calls.Load())
	require.EqualValues(t, 2, received.Load())
	require.False(t, l.overlap.Load(), "FreeMemory must never overlap SendCommand/UnInitialize")
	require.Empty(t, l.memory)
	for _, count := range l.freed {
		require.Equal(t, 1, count)
	}
}

func TestNullResponseIsAnExplicitError(t *testing.T) {
	l := newTestLibrary()
	l.null = true
	a := NewAdapter(l, "logs", 1)
	require.NoError(t, a.Start(func(string, error) {}))
	_, err := a.Send(`<command id="neworder"/>`)
	require.ErrorIs(t, err, ErrNoResponse)
	require.NoError(t, a.Close())
	require.EqualValues(t, 1, l.calls.Load())
}

func TestReadFailureStillFreesResponse(t *testing.T) {
	l := newTestLibrary()
	l.readError = true
	a := NewAdapter(l, "logs", 1)
	require.NoError(t, a.Start(func(string, error) {}))
	_, err := a.Send(`<command id="neworder"/>`)
	require.ErrorContains(t, err, "invalid data")
	require.NoError(t, a.Close())
	require.Empty(t, l.memory)
}

func TestInitializationFailurePreservesTextAndFreesMemory(t *testing.T) {
	l := newTestLibrary()
	l.initializeError = "cannot open log directory"
	a := NewAdapter(l, "logs", 1)
	require.ErrorContains(t, a.Start(func(string, error) {}), l.initializeError)
	require.Empty(t, l.memory)
	require.False(t, l.uninitialized.Load(), "UnInitialize only follows successful Initialize")
}

func TestFailedCallbackRegistrationUninitializes(t *testing.T) {
	l := newTestLibrary()
	l.callbackOK = false
	a := NewAdapter(l, "logs", 1)
	require.ErrorContains(t, a.Start(func(string, error) {}), "SetCallback")
	require.True(t, l.uninitialized.Load())
}

func TestFreeFailureIsReported(t *testing.T) {
	l := newTestLibrary()
	l.freeOK = false
	a := NewAdapter(l, "logs", 1)
	require.NoError(t, a.Start(func(string, error) {}))
	_, err := a.Send(`<command id="neworder"/>`)
	require.ErrorIs(t, err, ErrMemory)
	require.NoError(t, a.Close())
}

func TestAdapterCanRestartWithoutLeakingCallbackMemory(t *testing.T) {
	lib := newTestLibrary()
	lib.emit = true
	adapter := NewAdapter(lib, t.TempDir(), 1)
	for range 3 {
		var events atomic.Int32
		require.NoError(t, adapter.Start(func(_ string, err error) {
			require.NoError(t, err)
			events.Add(1)
		}))
		_, err := adapter.Send(`<command id="server_status"/>`)
		require.NoError(t, err)
		require.NoError(t, adapter.Close())
		require.EqualValues(t, 2, events.Load())
	}
	require.False(t, lib.overlap.Load())
	require.Empty(t, lib.memory)
	for _, count := range lib.freed {
		require.Equal(t, 1, count)
	}
}
