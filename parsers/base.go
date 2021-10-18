package parsers

import "github.com/pbudner/argosminer/events"

type Parser interface {
	Parse(input string) (*events.Event, error)
	Close()
}
