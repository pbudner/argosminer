package config

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/sources"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Source struct {
	Enabled     bool                       `yaml:"enabled"`
	FileConfig  *sources.FileSourceConfig  `yaml:"file-config,omitempty"`
	KafkaConfig *sources.KafkaSourceConfig `yaml:"kafka-config,omitempty"`
	CsvParser   *parsers.CsvParserConfig   `yaml:"csv-parser,omitempty"`
	JsonParser  *parsers.JsonParserConfig  `yaml:"json-parser,omitempty"`
}

type Config struct {
	Listener string    `yaml:"listener"`
	LogLevel log.Level `yaml:"log_level"`
	DataPath string    `yaml:"data_path"`
	Sources  []Source  `yaml:"sources"`
}

// NewConfig returns a new decoded Config struct
func NewConfig(path string) (*Config, error) {
	config := &Config{}

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
