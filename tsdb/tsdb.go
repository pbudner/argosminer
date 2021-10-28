package tsdb

import (
	"sync"

	"github.com/pbudner/argosminer/storage"
)

type Tsdb struct {
	sync.Mutex
	storage storage.Storage
	buffer  []*storage.KeyValue
}

type TsdbOptions struct {
	maxEntries uint64
}
