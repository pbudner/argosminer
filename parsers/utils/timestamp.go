package utils

import (
	"time"
)

type TimestampParser struct {
	tz     *time.Location
	format string `yaml:"timestamp-format"` // https://golang.org/src/time/format.go
}

func NewTimestampParser(format string, tzIanaKey string) *TimestampParser {
	var tz *time.Location = nil
	if tzIanaKey != "" {
		tz, _ = time.LoadLocation(tzIanaKey)
	}

	return &TimestampParser{
		format: format,
		tz:     tz,
	}
}

func (p TimestampParser) Parse(input string) (time.Time, error) {
	var (
		timestamp time.Time
		err       error
	)
	if p.tz != nil {
		timestamp, err = time.ParseInLocation(p.format, input, p.tz)
		if err != nil {
			return timestamp, err
		}
	} else {
		timestamp, err = time.Parse(p.format, input)
		if err != nil {
			return timestamp, err
		}
	}

	return timestamp.UTC(), nil
}
