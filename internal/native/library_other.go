//go:build !windows || !amd64

package native

import (
	"fmt"
)

// Load deliberately has no simulated production fallback on unsupported systems.
func Load(string, int) (Library, error) {
	return nil, fmt.Errorf("%s: %w", "native.Load", ErrUnsupported)
}
