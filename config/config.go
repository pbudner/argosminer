package config

import (
	"fmt"
	"os"

	"github.com/pbudner/argosminer/parsers/csv"
	"github.com/pbudner/argosminer/parsers/json"
	"github.com/pbudner/argosminer/sources/file"
	"github.com/pbudner/argosminer/sources/kafka"
	"gopkg.in/yaml.v2"
)

type Source struct {
	Enabled     bool                    `yaml:"enabled"`
	FileConfig  file.FileSourceConfig   `yaml:"file-config"`
	KafkaConfig kafka.KafkaSourceConfig `yaml:"kafka-config"`
	CsvParser   csv.CsvParserConfig     `yaml:"csv-parser"`
	JsonParser  json.JsonParserConfig   `yaml:"json-parser"`
}

type Config struct {
	Sources []Source `yaml:"sources"`
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
