// +build !linux,!darwin !cgo

package tableprinter

func getTerminalWidth() uint {
	return maxWidth
}
