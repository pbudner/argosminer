package stores

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const sbarPrefix = 0x01

var (
	metaCode             = []byte{sbarPrefix, 0x00}
	caseCode             = []byte{sbarPrefix, 0x01}
	activityCode         = []byte{sbarPrefix, 0x02}
	dfRelationCode       = []byte{sbarPrefix, 0x03}
	activityCounterKey   = append(metaCode, []byte("activity_counter")...)
	dfRelationCounterKey = append(metaCode, []byte("dfRelation_counter")...)
	startEventCounterKey = append(metaCode, []byte("startEvent_counter")...)
	flushAfter           = 10000 * time.Millisecond
	flushAfterEntries    = 100000
)

type DirectlyFollowsRelation struct {
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
	Count uint64 `json:"count,omitempty"`
}

type Activity struct {
	Name  string `json:"name,omitempty"`
	Count uint64 `json:"count,omitempty"`
}

type SbarStore struct {
	sync.Mutex
	storage                storage.Storage
	activityCounterCache   map[string]uint64
	dfRelationCounterCache map[string]uint64
	startEventCounterCache map[string]uint64
	caseCache              map[string]string
	activityBuffer         []storage.KeyValue
	dfRelationBuffer       []storage.KeyValue
	flushTicker            *time.Ticker
	doneChannel            chan bool
}

var activityBufferMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_stores_sbar",
	Name:      "buffered_activities_total",
	Help:      "Count of buffered activties in memory.",
}, []string{})

var dfRelationBufferMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_stores_sbar",
	Name:      "buffered_dfrelations_total",
	Help:      "Count of buffered activties in memory.",
}, []string{})

func init() {
	prometheus.MustRegister(activityBufferMetric, dfRelationBufferMetric)
}

func NewSbarStore(store storage.Storage) (*SbarStore, error) {
	result := SbarStore{
		storage:          store,
		activityBuffer:   make([]storage.KeyValue, 0),
		dfRelationBuffer: make([]storage.KeyValue, 0),
		caseCache:        make(map[string]string),
		doneChannel:      make(chan bool),
	}
	if err := result.init(); err != nil {
		return nil, err
	}
	return &result, nil
}

func (kv *SbarStore) init() error {
	kv.Lock()
	defer kv.Unlock()

	// load activity counter key from disk
	v, err := kv.storage.Get(activityCounterKey)
	if err != nil && err != badger.ErrKeyNotFound {
		log.Error(err)
	} else if err == badger.ErrKeyNotFound {
		log.Info("Initialize empty activity cache.")
		kv.activityCounterCache = make(map[string]uint64)
	} else {
		err = msgpack.Unmarshal(v, &kv.activityCounterCache)
		if err != nil {
			return err
		}
	}

	// load dfRelation counter key from disk
	v, err = kv.storage.Get(dfRelationCounterKey)
	if err != nil && err != badger.ErrKeyNotFound {
		log.Error(err)
	} else if err == badger.ErrKeyNotFound {
		log.Info("Initialize empty directly-follows relation cache.")
		kv.dfRelationCounterCache = make(map[string]uint64)
	} else {
		err = msgpack.Unmarshal(v, &kv.dfRelationCounterCache)
		if err != nil {
			return err
		}
	}

	// load startEvent counter key from disk
	v, err = kv.storage.Get(startEventCounterKey)
	if err != nil && err != badger.ErrKeyNotFound {
		log.Error(err)
	} else if err == badger.ErrKeyNotFound {
		kv.startEventCounterCache = make(map[string]uint64)
	} else {
		err = msgpack.Unmarshal(v, &kv.startEventCounterCache)
		if err != nil {
			return err
		}
	}

	kv.flushTicker = time.NewTicker(flushAfter)
	go func() {
		for {
			select {
			case <-kv.doneChannel:
				return
			case <-kv.flushTicker.C:
				kv.Lock()
				kv.flush()
				kv.Unlock()
			}
		}
	}()
	return nil
}

func (kv *SbarStore) RecordActivityForCase(activity string, caseId string, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	kv.caseCache[caseId] = activity
	return nil
}

func (kv *SbarStore) GetLastActivityForCase(caseId string) (string, error) {
	kv.Lock()
	defer kv.Unlock()
	// first, try to get from cache
	v, ok := kv.caseCache[caseId]
	if ok {
		return v, nil
	}
	// otherwise, try to get from disk
	b, err := kv.storage.Get(prefixString(caseCode, caseId))
	if err != nil && err != badger.ErrKeyNotFound {
		return "", err
	}
	if err == badger.ErrKeyNotFound {
		return "", nil
	}
	return string(b), nil
}

func (kv *SbarStore) RecordDirectlyFollowsRelation(from string, to string, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	dfRelation := fmt.Sprintf("%s-->%s", from, to)
	counter, err := kv.incr(kv.dfRelationCounterCache, dfRelation)
	if err != nil {
		return err
	}

	k, err := key.New(prefixString(dfRelationCode, dfRelation), timestamp)
	if err != nil {
		return err
	}

	kv.dfRelationBuffer = append(kv.dfRelationBuffer, storage.KeyValue{Key: k, Value: storage.Uint64ToBytes(counter)})
	dfRelationBufferMetric.WithLabelValues().Inc()
	return nil
}

func (kv *SbarStore) RecordActivity(activity string, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	counter, err := kv.incr(kv.activityCounterCache, activity)
	if err != nil {
		return err
	}

	k, err := key.New(prefixString(activityCode, activity), timestamp)
	if err != nil {
		return err
	}

	kv.activityBuffer = append(kv.activityBuffer, storage.KeyValue{Key: k, Value: storage.Uint64ToBytes(counter)})
	activityBufferMetric.WithLabelValues().Inc()
	if len(kv.activityBuffer) >= flushAfterEntries {
		kv.flush()
	}
	return nil
}

func (kv *SbarStore) GetActivities() []Activity {
	kv.Lock()
	defer kv.Unlock()
	result := make([]Activity, 0)
	for k, v := range kv.activityCounterCache {
		result = append(result, Activity{
			Name:  k,
			Count: v,
		})
	}
	return result
}

func (kv *SbarStore) GetDfRelations() []DirectlyFollowsRelation {
	kv.Lock()
	defer kv.Unlock()
	result := make([]DirectlyFollowsRelation, 0)
	for k, v := range kv.dfRelationCounterCache {
		splittedRelation := strings.Split(k, "-->") // this is not good, but
		result = append(result, DirectlyFollowsRelation{
			From:  splittedRelation[0],
			To:    strings.Join(splittedRelation[1:], "-->"),
			Count: v,
		})
	}
	return result
}

func (kv *SbarStore) CountActivities() int {
	kv.Lock()
	defer kv.Unlock()
	return len(kv.activityCounterCache)
}

func (kv *SbarStore) CountDfRelations() int {
	kv.Lock()
	defer kv.Unlock()
	return len(kv.dfRelationCounterCache)
}

func (kv *SbarStore) CountStartActivities() int {
	kv.Lock()
	defer kv.Unlock()
	return len(kv.startEventCounterCache)
}

func (kv *SbarStore) RecordStartActivity(key string) error {
	kv.Lock()
	defer kv.Unlock()
	_, err := kv.incr(kv.startEventCounterCache, key)
	if err != nil {
		return err
	}
	return nil
}

func (kv *SbarStore) Close() {
	kv.Lock()
	defer kv.Unlock()
	close(kv.doneChannel)
	kv.flushTicker.Stop()
	kv.flush()
}

func (kv *SbarStore) flush() error {
	b, err := msgpack.Marshal(&kv.activityCounterCache)
	if err != nil {
		return err
	}
	err = kv.storage.Set(activityCounterKey, b)
	if err != nil {
		return err
	}
	b, err = msgpack.Marshal(&kv.dfRelationCounterCache)
	if err != nil {
		return err
	}
	err = kv.storage.Set(dfRelationCounterKey, b)
	if err != nil {
		return err
	}
	b, err = msgpack.Marshal(&kv.startEventCounterCache)
	if err != nil {
		return err
	}
	err = kv.storage.Set(startEventCounterKey, b)
	if err != nil {
		return err
	}
	caseBuffer := make([]storage.KeyValue, len(kv.caseCache))
	i := 0
	for k, v := range kv.caseCache {
		caseBuffer[i] = storage.KeyValue{Key: prefixString(caseCode, k), Value: []byte(v)}
		i++
	}
	flushedItems := kv.flushBuffer(&caseBuffer)
	log.Debugf("Flushed %d last activities for a case", flushedItems)
	kv.caseCache = make(map[string]string)
	flushedItems = kv.flushBuffer(&kv.activityBuffer)
	log.Debugf("Flushed %d activties", flushedItems)
	flushedItems = kv.flushBuffer(&kv.dfRelationBuffer)
	log.Debugf("Flushed %d directly-follows relations", flushedItems)
	activityBufferMetric.Reset()
	dfRelationBufferMetric.Reset()
	return nil
}

func (kv *SbarStore) flushBuffer(items *[]storage.KeyValue) int {
	count := len(*items)
	kv.storage.SetBatch(*items)
	*items = make([]storage.KeyValue, 0)
	return count
}

func (kv *SbarStore) incr(cache map[string]uint64, key string) (uint64, error) {
	_, ok := cache[key]
	if !ok {
		cache[key] = 1
		return 1, nil
	} else {
		cache[key]++
		return cache[key], nil
	}
}

func prefixString(prefix []byte, str string) []byte {
	return append(prefix, []byte(str)...)
}
