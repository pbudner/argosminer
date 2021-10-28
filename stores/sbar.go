package stores

import (
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/pbudner/argosminer/storage"
)

type SbarStore struct {
	sync.Mutex
	storage      storage.Storage
	counterCache map[uint64]uint64
	caseCache    map[uint64][]byte
}

const seperatorCode = 0x00
const activityCode = 0x01
const dfRelationCode = 0x02
const startActivityCode = 0x03
const caseCode = 0x04
const timestampedCode = 0x05

func NewSbarStore(storageGenerator storage.StorageGenerator) *SbarStore {
	return &SbarStore{
		storage:      storageGenerator("sbar"),
		counterCache: make(map[uint64]uint64),
		caseCache:    make(map[uint64][]byte),
	}
}

func (kv *SbarStore) incr(key []byte) (uint64, error) {
	k := xxhash.Sum64(key)
	_, ok := kv.counterCache[k]
	if !ok {
		kv.counterCache[k] = 1
		return 1, nil
	} else {
		kv.counterCache[k]++
		return kv.counterCache[k], nil
	}
}

func (kv *SbarStore) RecordDirectlyFollowsRelation(from []byte, to []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.incr(encodeDfRelation(from, to, false))
	if err != nil {
		return err
	}

	/*k, err := key.New(encodeDfRelation(from, to, true), timestamp)
	if err != nil {
		return err
	}*/

	// kv.storage.Set(k, storage.Uint64ToBytes(counter))
	return nil
}

func (kv *SbarStore) RecordActivityForCase(activity []byte, caseId []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	//return kv.storage.Set(encodeCase(caseId, false), activity)
	kv.caseCache[xxhash.Sum64(caseId)] = activity
	return nil
}

func (kv *SbarStore) GetLastActivityForCase(caseId []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()
	v, ok := kv.caseCache[xxhash.Sum64(caseId)]
	if !ok {
		return nil, nil
	}

	return v, nil
	/*v, err := kv.storage.Get(encodeCase(caseId, false))
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return v, nil*/
}

func (kv *SbarStore) RecordActivity(activity []byte, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.incr(encodeActivity(activity, false))
	if err != nil {
		return err
	}

	/*
		k, err := key.New(encodeActivity(activity, true), timestamp)
		if err != nil {
			return err
		}

		kv.storage.Set(k, storage.Uint64ToBytes(counter))*/
	return nil
}

func (kv *SbarStore) GetActivities() (map[string]uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	/*activities, err := kv.storage.Find([]byte{activityCode})
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64)
	for _, a := range activities {
		result[string(a.Key[1:])] = storage.BytesToUint64(a.Value)
	}
	return result, nil*/
	return nil, nil
}

func (kv *SbarStore) GetDfRelations() (map[string]uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	/*activities, err := kv.storage.Find([]byte{dfRelationCode})
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64)
	for _, a := range activities {
		result[string(a.Key[1:])] = storage.BytesToUint64(a.Value)
	}
	return result, nil*/
	return nil, nil
}

func (kv *SbarStore) CountActivities() (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	/*counter, err := kv.storage.CountPrefix([]byte{activityCode})
	if err != nil {
		return 0, err
	}

	return counter, nil*/
	return 0, nil
}

func (kv *SbarStore) CountDfRelations() (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	/*counter, err := kv.storage.CountPrefix([]byte{dfRelationCode})
	if err != nil {
		return 0, err
	}

	return counter, nil*/
	return 0, nil
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
	//_, err := kv.storage.Increment(encodeStartActivity(key, false))
	return nil
}

func (kv *SbarStore) Close() {
	kv.Lock()
	defer kv.Unlock()
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
