package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type Source struct {
	Enabled    bool       `yaml:"enabled"`
	FileConfig FileSource `yaml:"file-config"`
	CsvParser  CsvParser  `yaml:"csv-parser"`
}

type FileSource struct {
	Path     string `yaml:"path"`
	ReadFrom string `yaml:"read-from"`
}

type CsvParser struct {
	Delimiter             string `yaml:"delimiter"`
	ActivityColumn        uint   `yaml:"activity-column"`
	ProcessInstanceColumn uint   `yaml:"process-instance-column"`
	TimestampColumn       uint   `yaml:"timestamp-column"`
	TimestampFormat       string `yaml:"timestamp-format"`      // https://golang.org/src/time/format.go
	TimestampTzIanakey    string `yaml:"timestamp-tz-iana-key"` // https://golang.org/src/time/format.go
	IgnoreWhen            []struct {
		Column    uint   `yaml:"column"`
		Condition string `yaml:"condition"`
		Value     string `yaml:"value"`
	} `yaml:"ignore-when"`
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
