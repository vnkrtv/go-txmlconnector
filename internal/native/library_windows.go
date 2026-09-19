//go:build windows && amd64

package native

import (
	"fmt"
	"runtime"
	"strings"
	"syscall"
	"unsafe"
)

type DLL struct {
	initialize, callback, send, free, uninitialize *syscall.Proc
	maxBytes                                       int
	callbackPtr                                    uintptr
}

// Load keeps the DLL loaded for the process lifetime, including callback thunks.
func Load(path string, maxBytes int) (*DLL, error) {
	const op = "native.Load"
	dll, err := syscall.LoadDLL(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	d := &DLL{maxBytes: maxBytes}
	for name, target := range map[string]**syscall.Proc{
		"Initialize": &d.initialize, "SetCallback": &d.callback,
		"SendCommand": &d.send, "FreeMemory": &d.free, "UnInitialize": &d.uninitialize,
	} {
		*target, err = dll.FindProc(name)
		if err != nil {
			_ = dll.Release()
			return nil, fmt.Errorf("%s: %w", op, err)
		}
	}
	return d, nil
}

func (d *DLL) Initialize(path string, level int) (uintptr, error) {
	const op = "native.DLL.Initialize"
	p, err := syscall.BytePtrFromString(path)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	r, _, lastErr := d.initialize.Call(uintptr(unsafe.Pointer(p)), uintptr(level))
	runtime.KeepAlive(p)
	return r, lastErr // ABI result; the adapter interprets it, including stale lastErr.
}

func (d *DLL) SetCallback(callback func(uintptr) uintptr) (bool, error) {
	if d.callbackPtr == 0 {
		d.callbackPtr = syscall.NewCallback(callback)
	}
	r, _, lastErr := d.callback.Call(d.callbackPtr)
	return r != 0, lastErr
}

func (d *DLL) SendCommand(command string) (uintptr, error) {
	const op = "native.DLL.SendCommand"
	p, err := syscall.BytePtrFromString(command)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	r, _, lastErr := d.send.Call(uintptr(unsafe.Pointer(p)))
	runtime.KeepAlive(p)
	return r, lastErr
}

func (d *DLL) Read(p uintptr) (string, error) {
	const op = "native.DLL.Read"
	if p == 0 {
		return "", fmt.Errorf("%s: null pointer", op)
	}
	// Native pointer validity is an ABI assumption. A corrupt DLL pointer can
	// terminate the process; recover is not a substitute for process isolation.
	for n := 0; n <= d.maxBytes; n++ {
		if *(*byte)(unsafe.Pointer(p + uintptr(n))) == 0 {
			return strings.Clone(unsafe.String((*byte)(unsafe.Pointer(p)), n)), nil
		}
	}
	return "", fmt.Errorf("%s: response exceeds byte limit", op)
}

func (d *DLL) FreeMemory(p uintptr) bool      { r, _, _ := d.free.Call(p); return r != 0 }
func (d *DLL) UnInitialize() (uintptr, error) { r, _, e := d.uninitialize.Call(); return r, e }
