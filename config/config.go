package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/pbudner/argosminer/pipeline/sources"
	"github.com/pbudner/argosminer/pipeline/transforms"
	"github.com/pbudner/argosminer/storage"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type Source struct {
	Enabled bool `yaml:"enabled"`
	// FileConfig  *sources.FileSourceConfig   `yaml:"file-config,omitempty"`
	KafkaConfig *sources.KafkaConfig           `yaml:"kafka-config,omitempty"`
	CsvParsers  []*transforms.CsvParserConfig  `yaml:"csv-parsers,omitempty"`
	JsonParsers []*transforms.JsonParserConfig `yaml:"json-parsers,omitempty"`
}

type Config struct {
	Listener         string           `yaml:"listener"`
	BaseURL          string           `yaml:"base-url"`
	Logger           zap.Config       `yaml:"logger"`
	Database         storage.Config   `yaml:"db"`
	Sources          []Source         `yaml:"sources"`
	IgnoreActivities []IgnoreActivity `yaml:"ignore-activities"`
}

type IgnoreActivity struct {
	Name   string `yaml:"name"`
	Remove bool   `yaml:"remove,omitempty"`
}

func DefaultConfig() *Config {
	config := &Config{
		Logger:   zap.NewDevelopmentConfig(),
		BaseURL:  "/",
		Listener: "localhost:4711",
		Database: storage.Config{
			Path:       "./data/",
			SyncWrites: false,
		},
	}

	config.Logger.Level = zap.NewAtomicLevelAt(zap.InfoLevel) // set default level to info

	return config
}

// NewConfig returns a new decoded Config struct
func NewConfig(path string) (*Config, error) {
	defaultConfig := DefaultConfig()
	config := DefaultConfig()

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

	// expand config with environment variables
	confContent = []byte(os.ExpandEnv(string(confContent)))

	// unmarshal yaml
	err = yaml.Unmarshal(confContent, config)
	if err != nil {
		return nil, err
	}

	if config.BaseURL == "" {
		config.BaseURL = defaultConfig.BaseURL
	}

	config.BaseURL = strings.TrimRight(config.BaseURL, "/") // remove tailing slashes
	return config, nil
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
