package aquifer

import (
	"log"
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

type RateConfig struct {
	RPS           float64 `yaml:"rps"`
	MaxConcurrent int     `yaml:"max_concurrent"`
}

type Config struct {
	Defaults  RateConfig            `yaml:"defaults"`
	Upstreams map[string]RateConfig `yaml:"upstreams"`
}

func LoadConfig(path string) *Config {
	cfg := &Config{
		Defaults: RateConfig{RPS: 2.0, MaxConcurrent: 1},
	}
	if path == "" {
		return cfg
	}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("config: cannot read %s, using defaults: %v", path, err)
		return cfg
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		log.Printf("config: parse error in %s, using defaults: %v", path, err)
		return cfg
	}
	if cfg.Defaults.RPS <= 0 {
		cfg.Defaults.RPS = 2.0
	}
	if cfg.Defaults.MaxConcurrent <= 0 {
		cfg.Defaults.MaxConcurrent = 1
	}
	return cfg
}

func (c *Config) ForURL(rawURL string) RateConfig {
	u, err := url.Parse(rawURL)
	if err != nil {
		return c.Defaults
	}
	if rc, ok := c.Upstreams[u.Host]; ok {
		return rc
	}
	return c.Defaults
}
