package config

import (
	"fmt"
	"os"

	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/sources"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type Source struct {
	Enabled     bool                       `yaml:"enabled"`
	FileConfig  *sources.FileSourceConfig  `yaml:"file-config,omitempty"`
	KafkaConfig *sources.KafkaSourceConfig `yaml:"kafka-config,omitempty"`
	CsvParser   *parsers.CsvParserConfig   `yaml:"csv-parser,omitempty"`
	JsonParser  *parsers.JsonParserConfig  `yaml:"json-parser,omitempty"`
}

type Config struct {
	LogLevel log.Level `yaml:"log_level"`
	Sources  []Source  `yaml:"sources"`
}

// NewConfig returns a new decoded Config struct
func NewConfig() (*Config, error) {
	// Create config structure
	config := &Config{}

	// Open config file
	file, err := os.Open("config.yaml")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Init new YAML decode
	d := yaml.NewDecoder(file)

	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		return nil, err
	}

	return config, nil
}

// ValidateConfigPath just makes sure, that the path provided is a file,
// that can be read
func ValidateConfigPath(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if s.IsDir() {
		return fmt.Errorf("'%s' is a directory, not a normal file", path)
	}
	return nil
}
