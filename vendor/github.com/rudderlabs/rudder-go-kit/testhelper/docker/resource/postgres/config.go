package postgres

type Opt func(*Config)

func WithTag(tag string) Opt {
	return func(c *Config) {
		c.Tag = tag
	}
}

func WithOptions(options ...string) Opt {
	return func(c *Config) {
		c.Options = options
	}
}

func WithShmSize(shmSize int64) Opt {
	return func(c *Config) {
		c.ShmSize = shmSize
	}
}

type Config struct {
	Tag     string
	Options []string
	ShmSize int64
}
