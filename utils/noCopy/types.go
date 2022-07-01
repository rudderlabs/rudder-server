package noCopy

type NoCopy struct{}

// Lock ...
func (*NoCopy) Lock() {}

// Unlock ...
func (*NoCopy) Unlock() {}
