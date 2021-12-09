package stores

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	ulid "github.com/oklog/ulid/v2"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const sbarPrefix = 0x03

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
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Weight uint64 `json:"weight,omitempty"`
}

type Activity struct {
	Name   string `json:"name,omitempty"`
	Weight uint64 `json:"weight,omitempty"`
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
	dfRelation := encodeDfRelation(from, to)
	counter := kv.incr(kv.dfRelationCounterCache, dfRelation)
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
	counter := kv.incr(kv.activityCounterCache, activity)
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
			Name:   k,
			Weight: v,
		})
	}
	return result
}

func (kv *SbarStore) GetDfRelations() []DirectlyFollowsRelation {
	kv.Lock()
	defer kv.Unlock()
	result := make([]DirectlyFollowsRelation, 0)
	for k, v := range kv.dfRelationCounterCache {
		splittedRelation := strings.Split(k, "-->") // this is not good, but ok for now
		result = append(result, DirectlyFollowsRelation{
			From:   splittedRelation[0],
			To:     strings.Join(splittedRelation[1:], "-->"),
			Weight: v,
		})
	}
	return result
}

func (kv *SbarStore) GetDfRelationsWithinTimewindow(dfRelations [][]string, start time.Time, end time.Time) ([]DirectlyFollowsRelation, error) {
	kv.Lock()
	defer kv.Unlock()

	result := make([]DirectlyFollowsRelation, 0)

	for _, relation := range dfRelations {
		encodedRelation := encodeDfRelation(relation[0], relation[1])

		getCountForRelation := func(encodedRelation string, timestamp time.Time) (uint64, error) {
			k, err := key.New(prefixString(dfRelationCode, encodedRelation), timestamp)
			if err != nil {
				return 0, err
			}

			log.Debugf("%s: Seek for date %s", encodedRelation, timestamp.Format(time.RFC3339))
			keyValue, err := kv.storage.Seek(k[:14]) // 8 for name name hash + 6 for timestamp

			if err == storage.ErrNoResults {
				log.Debugf("Could not find a directly-follows relation for %s", encodedRelation)
				return 0, nil
			}

			if err != nil {
				return 0, err
			}

			return storage.BytesToUint64(keyValue.Value), nil
		}

		startValue, err := getCountForRelation(encodedRelation, start)
		if err != nil {
			return nil, err
		}
		endValue, err := getCountForRelation(encodedRelation, end)
		if err != nil {
			return nil, err
		}

		diffValue := endValue - startValue
		if diffValue > 0 {
			result = append(result, DirectlyFollowsRelation{From: relation[0], To: relation[1], Weight: diffValue})
		}
	}
	return result, nil
}

func (kv *SbarStore) DailyCountOfActivities(activities []string) (map[string]map[string]uint64, error) {
	kv.Lock()
	defer kv.Unlock()

	result := make(map[string]map[string]uint64)

	for _, activity := range activities {
		k, err := key.New(prefixString(activityCode, activity), time.Now())
		if err != nil {
			return nil, err
		}

		binCounter := make(map[string]uint64)
		currentUlid := ulid.ULID{}
		prevFormattedTime := ""
		kv.storage.Iterate(k[0:8], func(key []byte, valueFunc func() ([]byte, error)) (bool, error) {
			currentUlid.UnmarshalBinary(key[8:])
			eventTime := time.UnixMilli(int64(currentUlid.Time()))
			formattedTime := eventTime.Format("2006/01/02")
			if prevFormattedTime == "" {
				prevFormattedTime = formattedTime
				return true, nil
			}

			if formattedTime != prevFormattedTime {
				v, err := valueFunc()
				if err != nil {
					return false, nil
				}
				value := storage.BytesToUint64(v)
				binCounter[prevFormattedTime] = value - 1
				prevFormattedTime = formattedTime
			}
			return true, nil
		})

		keys := make([]string, 0)
		for k := range binCounter {
			keys = append(keys, k)
		}

		sort.Strings(keys)
		binRate := make(map[string]uint64)
		for i, k := range keys {
			if i == 0 {
				binRate[k] = binCounter[k]
			} else {
				if binCounter[k] < binCounter[keys[i-1]] {
					log.Warnf("Previous counter in DailyCountOfActivities for activity %s is larger than current. This should not happen.", activity)
				} else {
					binRate[k] = binCounter[k] - binCounter[keys[i-1]]
				}
			}
		}

		result[activity] = binRate
	}
	return result, nil
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

func (kv *SbarStore) RecordStartActivity(key string) {
	kv.Lock()
	defer kv.Unlock()
	kv.incr(kv.startEventCounterCache, key)
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

func (kv *SbarStore) incr(cache map[string]uint64, key string) uint64 {
	_, ok := cache[key]
	if !ok {
		cache[key] = 1
		return 1
	} else {
		cache[key]++
		return cache[key]
	}
}

func prefixString(prefix []byte, str string) []byte {
	return append(prefix, []byte(str)...)
}

func encodeDfRelation(from string, to string) string {
	return fmt.Sprintf("%s-->%s", from, to)
}
