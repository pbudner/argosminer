package stores

import (
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
)

type SbarStore struct {
	sync.Mutex
	storage storage.Storage
}

const seperatorCode = 0x00
const activityCode = 0x01
const dfRelationCode = 0x02
const startActivityCode = 0x03
const caseCode = 0x04
const timestampedCode = 0x05

func NewSbarStore(storageGenerator storage.StorageGenerator) *SbarStore {
	return &SbarStore{
		storage: storageGenerator("sbar"),
	}
}

func (kv *SbarStore) RecordDirectlyFollowsRelation(from []byte, to []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.storage.Increment(encodeDfRelation(from, to, false))
	if err != nil {
		return err
	}

	k, err := key.New(encodeDfRelation(from, to, true), timestamp)
	if err != nil {
		return err
	}

	kv.storage.Set(k, storage.Uint64ToBytes(counter))
	return err
}

func (kv *SbarStore) RecordActivityForCase(activity []byte, caseId []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	return kv.storage.Set(encodeCase(caseId, false), activity)
}

func (kv *SbarStore) GetLastActivityForCase(caseId []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()
	v, err := kv.storage.Get(encodeCase(caseId, false))
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return v, nil
}

func (kv *SbarStore) RecordActivity(activity []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.storage.Increment(encodeActivity(activity, false))
	if err != nil {
		return err
	}

	k, err := key.New(encodeActivity(activity, true), timestamp)
	if err != nil {
		return err
	}

	kv.storage.Set(k, storage.Uint64ToBytes(counter))
	return err
}

func (kv *SbarStore) GetActivities() (map[string]uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	activities, err := kv.storage.Find([]byte{activityCode})
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64)
	for _, a := range activities {
		result[string(a.Key[1:])] = storage.BytesToUint64(a.Value)
	}
	return result, nil
}

func (kv *SbarStore) GetDfRelations() (map[string]uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	activities, err := kv.storage.Find([]byte{dfRelationCode})
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64)
	for _, a := range activities {
		result[string(a.Key[1:])] = storage.BytesToUint64(a.Value)
	}
	return result, nil
}

func (kv *SbarStore) CountActivities() (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.storage.CountPrefix([]byte{activityCode})
	if err != nil {
		return 0, err
	}

	return counter, nil
}

func (kv *SbarStore) CountDfRelations() (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.storage.CountPrefix([]byte{dfRelationCode})
	if err != nil {
		return 0, err
	}

	return counter, nil
}

func (kv *SbarStore) CountStartActivities() (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.storage.CountPrefix([]byte{startActivityCode})
	if err != nil {
		return 0, err
	}

	return counter, nil
}

func (kv *SbarStore) RecordStartActivity(key []byte) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.storage.Increment(encodeStartActivity(key, false))
	return err
}

func (kv *SbarStore) Close() {
	kv.storage.Close()
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
