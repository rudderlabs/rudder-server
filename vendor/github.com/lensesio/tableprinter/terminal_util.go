// +build linux,cgo darwin,cgo

package tableprinter

import (
	"syscall"
	"unsafe"
)

type winsize struct {
	Row    uint16
	Col    uint16
	Xpixel uint16
	Ypixel uint16
}

func getTerminalWidth() uint {
	ws := &winsize{}
	retCode, _, _ := syscall.Syscall(syscall.SYS_IOCTL, uintptr(syscall.Stdin), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(ws)))

	if int(retCode) == -1 {
		return maxWidth
	}

	return uint(ws.Col)
}
