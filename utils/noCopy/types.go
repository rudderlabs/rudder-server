package noCopy

type CopyRestrictor struct{}

// Lock ...
func (*CopyRestrictor) Lock() {}

// Unlock ...
func (*CopyRestrictor) Unlock() {}
