package sources

import (
	"context"
	"sync"

	"github.com/pbudner/argosminer/processors"
)

type Source interface {
	AddProcessors([]processors.StreamingProcessor)
	Run(context.Context, *sync.WaitGroup)
	Close()
}
