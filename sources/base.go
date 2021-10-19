package sources

import (
	"context"
	"sync"

	"github.com/pbudner/argosminer/algorithms"
)

type Source interface {
	AddReceivers([]algorithms.StreamingAlgorithm)
	Run(context.Context, *sync.WaitGroup)
	Close()
}
