//go:build windows && amd64

package native

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Opt-in native smoke test. It never connects to a broker or submits an order.
func TestDLLSmoke(t *testing.T) {
	path := os.Getenv("TXML_SMOKE_DLL")
	if path == "" {
		t.Skip("set TXML_SMOKE_DLL to an absolute Windows DLL path")
	}
	lib, err := Load(path, 4<<20)
	require.NoError(t, err)
	a := NewAdapter(lib, t.TempDir(), 1)
	events := make(chan string, 16)
	require.NoError(t, a.Start(func(message string, err error) {
		if err != nil {
			t.Errorf("callback: %v", err)
		}
		select {
		case events <- message:
		default:
			t.Error("unexpected callback flood")
		}
	}))
	defer func() { require.NoError(t, a.Close()) }()
	response, err := a.Send(`<command id="get_connector_version"/>`)
	require.NoError(t, err)
	// This DLL acknowledges synchronously and returns the version in callback.
	require.Contains(t, response, `success="true"`)
	select {
	case event := <-events:
		require.Contains(t, event, "connector_version")
	case <-time.After(5 * time.Second):
		t.Fatal("version callback did not arrive")
	}
	response, err = a.Send(`<command id="__bridge_smoke_invalid_command__"/>`)
	require.NoError(t, err, "DLL error XML must remain a valid native response")
	require.True(t, strings.Contains(response, "error") || strings.Contains(response, `success="false"`), "expected an error response, got %q", response)
}
