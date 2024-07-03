package flusher

type NOPCronRunner struct{}

func (c *NOPCronRunner) Run()  {}
func (c *NOPCronRunner) Stop() {}
