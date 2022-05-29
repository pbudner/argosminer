package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/pbudner/argosminer/storage"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type Component struct {
	Name     string                 `yaml:"name"`
	Disabled bool                   `yaml:"disabled"`
	Config   map[string]interface{} `yaml:",inline"`
	Connects []Component            `yaml:"connects,omitempty"`
}

type Config struct {
	Listener         string           `yaml:"listener"`
	BaseURL          string           `yaml:"base-url"`
	Logger           zap.Config       `yaml:"logger"`
	Database         storage.Config   `yaml:"database"`
	Pipeline         []Component      `yaml:"pipeline"`
	IgnoreActivities []IgnoreActivity `yaml:"ignore-activities"`
}

type IgnoreActivity struct {
	Name   string `yaml:"name"`
	Remove bool   `yaml:"remove,omitempty"`
}

func DefaultConfig() *Config {
	config := &Config{
		Logger:   zap.NewDevelopmentConfig(),
		BaseURL:  "",
		Listener: "localhost:4711",
		Database: storage.Config{
			Path:       "./data/",
			SyncWrites: false,
		},
	}

	config.Logger.Level = zap.NewAtomicLevelAt(zap.InfoLevel) // set default level to info

	return config
}

// NewConfigFromFile returns a new decoded Config struct from file
func NewConfigFromStr(confContent []byte) (*Config, error) {
	confContent = []byte(os.ExpandEnv(string(confContent))) // expand config with environment variables

	defaultConfig := DefaultConfig()
	config := DefaultConfig()

	// unmarshal yaml
	err := yaml.Unmarshal(confContent, config)
	if err != nil {
		return nil, err
	}

	if config.BaseURL == "" {
		config.BaseURL = defaultConfig.BaseURL
	}

	config.BaseURL = strings.TrimRight(config.BaseURL, "/") // remove tailing slashes
	return config, nil
}

// NewConfigFromFile returns a new decoded Config struct from file
func NewConfigFromFile(path string) (*Config, error) {
	// check that config exists
	err := validateConfigPath(path)
	if err != nil {
		return nil, err
	}

	// read config
	confContent, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return NewConfigFromStr(confContent)
}

// validateConfigPath just makes sure, that the path provided is a file,
// that can be read
func validateConfigPath(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if s.IsDir() {
		return fmt.Errorf("'%s' is a directory, not a normal file", path)
	}
	return nil
}
