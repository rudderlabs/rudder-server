package multitenant

type Counter struct {
	ch                    chan func()
	workspaceDestCountMap map[string]map[string]int
}

func NewChannelCounter() *Counter {
	counter := &Counter{make(chan func(), 256), make(map[string]map[string]int)}
	go func(counter *Counter) {
		for f := range counter.ch {
			f()
		}
	}(counter)
	return counter
}

func (c *Counter) Add(workspaceID, dest string, num int) {
	c.ch <- func() {
		_, ok := c.workspaceDestCountMap[workspaceID]
		if !ok {
			c.workspaceDestCountMap[workspaceID] = make(map[string]int)
		}
		c.workspaceDestCountMap[workspaceID][dest] += num
	}
}

func (c *Counter) Sub(workspaceID, dest string, num int) {
	c.ch <- func() {
		_, ok := c.workspaceDestCountMap[workspaceID]
		if !ok {
			c.workspaceDestCountMap[workspaceID] = make(map[string]int)
		}
		c.workspaceDestCountMap[workspaceID][dest] -= num
	}
}

func (c *Counter) Read() map[string]map[string]int {
	ret := make(chan map[string]map[string]int)
	c.ch <- func() {
		counts := make(map[string]map[string]int)
		for workspace := range c.workspaceDestCountMap {
			counts[workspace] = make(map[string]int)
			for dest := range c.workspaceDestCountMap[workspace] {
				counts[workspace][dest] = c.workspaceDestCountMap[workspace][dest]
			}
		}
		ret <- counts
		close(ret)
	}
	return <-ret
}
