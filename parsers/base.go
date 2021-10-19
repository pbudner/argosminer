package parsers

import "github.com/pbudner/argosminer/events"

type Parser interface {
	Parse(input []byte) (*events.Event, error)
	Close()
}
