package stores

import (
	"sync"
	"time"

	"github.com/pbudner/argosminer/stores/backends"
)

type SbarStore struct {
	sync.Mutex
	store backends.StoreBackend
}

const seperatorCode = 0x00
const activityCode = 0x01
const dfRelationCode = 0x02
const startActivityCode = 0x03
const caseCode = 0x04

// TODO: Trace DFRelations and Activities over time
func NewSbarStore(storeGenerator backends.StoreBackendGenerator) *SbarStore {
	return &SbarStore{
		store: storeGenerator("sbar"),
	}
}

func (kv *SbarStore) RecordDirectlyFollowsRelation(from []byte, to []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.store.Increment(encodeDfRelation(from, to))
	return err
}

func (kv *SbarStore) RecordActivityForCase(activity []byte, caseId []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	return kv.store.Set(encodeCase(caseId), activity)
}

func (kv *SbarStore) GetLastActivityForCase(caseId []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.store.Get(encodeCase(caseId))
}

func (kv *SbarStore) RecordActivity(key []byte) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.store.Increment(encodeActivity(key))
	return err
}

func (kv *SbarStore) RecordStartActivity(key []byte) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.store.Increment(encodeStartActivity(key))
	return err
}

func encodeActivity(key []byte) []byte {
	return append([]byte{activityCode}, key...)
}

// TODO: evaluate whether this is correct
func encodeDfRelation(from []byte, to []byte) []byte {
	result := make([]byte, len(from)+len(to)+2)
	result[0] = dfRelationCode
	result[len(from)+1] = seperatorCode
	for i, b := range from {
		result[1+i] = b
	}
	for i, b := range from {
		result[len(from)+2+i] = b
	}
	return result
}

func encodeStartActivity(key []byte) []byte {
	return append([]byte{startActivityCode}, key...)
}

func encodeCase(key []byte) []byte {
	return append([]byte{caseCode}, key...)
}
