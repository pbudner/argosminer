package parsers

import (
	"io"

	"github.com/pbudner/argosminer-collector/pkg/algorithms"
)

type Parser interface {
	Parse(reader io.Reader, receivers []algorithms.StreamingAlgorithm) error
	Close()
}
