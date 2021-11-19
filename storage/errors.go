package storage

import "errors"

var (
	// ErrNoResults is returned when no results have been found (e.g., in a seek)
	ErrNoResults = errors.New("could not find any results")
)
