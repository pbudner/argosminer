package stores

import (
	"math/rand"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pbudner/argosminer/stores/backends"
	"github.com/pbudner/argosminer/stores/ulid"
	"github.com/pbudner/argosminer/stores/utils"
)

type SbarStore struct {
	sync.Mutex
	store         backends.StoreBackend
	ulidGenerator ulid.MonotonicULIDGenerator
}

const seperatorCode = 0x00
const activityCode = 0x01
const dfRelationCode = 0x02
const startActivityCode = 0x03
const caseCode = 0x04
const timestampedCode = 0x05

func NewSbarStore(storeGenerator backends.StoreBackendGenerator) *SbarStore {
	return &SbarStore{
		store:         storeGenerator("sbar"),
		ulidGenerator: *ulid.NewMonotonicULIDGenerator(rand.New(rand.NewSource(4711))),
	}
}

func (kv *SbarStore) RecordDirectlyFollowsRelation(from []byte, to []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.store.Increment(encodeDfRelation(from, to, false))
	if err != nil {
		return err
	}

	ulid, err := kv.ulidGenerator.New(timestamp)
	if err != nil {
		return err
	}

	binID, err := ulid.MarshalBinary()
	if err != nil {
		panic(err)
	}

	kv.store.Set(append(encodeDfRelation(from, to, true), binID...), utils.Uint64ToBytes(counter))
	return err
}

func (kv *SbarStore) RecordActivityForCase(activity []byte, caseId []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	return kv.store.Set(encodeCase(caseId, false), activity)
}

func (kv *SbarStore) GetLastActivityForCase(caseId []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()
	v, err := kv.store.Get(encodeCase(caseId, false))
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return v, nil
}

func (kv *SbarStore) RecordActivity(key []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.store.Increment(encodeActivity(key, false))
	if err != nil {
		return err
	}

	ulid, err := kv.ulidGenerator.New(timestamp)
	if err != nil {
		return err
	}

	binID, err := ulid.MarshalBinary()
	if err != nil {
		panic(err)
	}

	kv.store.Set(append(encodeActivity(key, true), binID...), utils.Uint64ToBytes(counter))
	return err
}

func (kv *SbarStore) GetActivities() ([]string, error) {
	kv.Lock()
	defer kv.Unlock()
	activities, err := kv.store.Find([]byte{activityCode})
	if err != nil {
		return nil, err
	}
	result := make([]string, len(activities))
	for i, a := range activities {
		result[i] = string(a.Key[1:])
	}
	return result, nil
}

func (kv *SbarStore) GetDfRelations() ([]string, error) {
	kv.Lock()
	defer kv.Unlock()
	activities, err := kv.store.Find([]byte{dfRelationCode})
	if err != nil {
		return nil, err
	}
	result := make([]string, len(activities))
	for i, a := range activities {
		result[i] = string(a.Key[1:])
	}
	return result, nil
}

func (kv *SbarStore) RecordStartActivity(key []byte) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.store.Increment(encodeStartActivity(key, false))
	return err
}

func (kv *SbarStore) Close() {
	kv.store.Close()
}

func encodeActivity(key []byte, timestamped bool) []byte {
	if timestamped {
		return append([]byte{timestampedCode, activityCode}, key...)
	}

	return append([]byte{activityCode}, key...)
}

func encodeDfRelation(from []byte, to []byte, timestamped bool) []byte {
	result := make([]byte, len(from)+len(to)+2)
	result[0] = dfRelationCode
	result[len(from)] = seperatorCode
	for i, b := range from {
		result[1+i] = b
	}
	for i, b := range to {
		result[len(from)+1+i] = b
	}

	if timestamped {
		result = append([]byte{timestampedCode}, result...)
	}

	return result
}

func encodeStartActivity(key []byte, timestamped bool) []byte {
	if timestamped {
		return append([]byte{timestampedCode, startActivityCode}, key...)
	}

	return append([]byte{startActivityCode}, key...)
}

func encodeCase(key []byte, timestamped bool) []byte {
	if timestamped {
		return append([]byte{timestampedCode, caseCode}, key...)
	}

	return append([]byte{caseCode}, key...)
}
