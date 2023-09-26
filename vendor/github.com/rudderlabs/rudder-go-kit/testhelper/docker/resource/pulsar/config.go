package pulsar

type Opt func(*Config)

func WithTag(tag string) Opt {
	return func(c *Config) {
		c.Tag = tag
	}
}

type Config struct {
	Tag string
}
