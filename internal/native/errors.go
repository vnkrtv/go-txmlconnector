package native

import (
	"errors"
)

var (
	ErrNoResponse  = errors.New("DLL returned a null response; command outcome is unknown")
	ErrMemory      = errors.New("DLL memory release failed")
	ErrUnsupported = errors.New("TXmlConnector requires Windows amd64 (or Wine)")
)
