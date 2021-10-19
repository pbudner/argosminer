package sources

import (
	"context"
	"sync"

	"github.com/pbudner/argosminer/receivers"
)

type Source interface {
	AddReceivers([]receivers.StreamingReceiver)
	Run(context.Context, *sync.WaitGroup)
	Close()
}
