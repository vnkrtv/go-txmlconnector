package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigLimitsAndPaths(t *testing.T) {
	cfg, err := Parse(nil)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(cfg.DLLPath))
	require.True(t, filepath.IsAbs(cfg.NativeLogPath))
	for _, args := range [][]string{
		{"-command-queue=0"}, {"-event-queue=0"}, {"-command-timeout=0s"},
		{"-shutdown-timeout=-1s"}, {"-max-event-bytes=1"}, {"-max-message-bytes=67108865"},
		{"-native-log-level=0"}, {"-log-level=trace"}, {"-dll="}, {"-native-log-dir=a\x00b"}, {"unexpected"},
	} {
		_, err := Parse(args)
		require.Error(t, err, args)
	}
}
