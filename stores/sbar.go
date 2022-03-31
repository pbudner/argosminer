package stores

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	ulid "github.com/oklog/ulid/v2"
	"github.com/pbudner/argosminer/encoding"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const sbarPrefix = 0x03

var (
	sbarStoreSingletonOnce sync.Once
	sbarStoreSingleton     *SbarStore
	metaCode               = []byte{sbarPrefix, 0x00}
	caseCode               = []byte{sbarPrefix, 0x01}
	activityOverTimeCode   = []byte{sbarPrefix, 0x02}
	dfRelationOverTimeCode = []byte{sbarPrefix, 0x03}
	activityCounterKey     = append(metaCode, []byte("activity_counter")...)
	dfRelationCounterKey   = append(metaCode, []byte("dfRelation_counter")...)
	startEventCounterKey   = append(metaCode, []byte("startEvent_counter")...)
	flushAfter             = 10000 * time.Millisecond
	flushAfterEntries      = 100000
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
	sync.RWMutex
	activityCounterCache   map[string]uint64
	dfRelationCounterCache map[string]uint64
	startEventCounterCache map[string]uint64
	caseCache              storage.CachedByteStorage
	activityBuffer         []storage.KeyValue[[]byte, []byte]
	dfRelationBuffer       []storage.KeyValue[[]byte, []byte]
	flushTicker            *time.Ticker
	doneChannel            chan bool
	log                    *zap.SugaredLogger
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

func GetSbarStore() *SbarStore {
	sbarStoreSingletonOnce.Do(func() {
		sbarStoreSingleton = &SbarStore{
			activityBuffer:   make([]storage.KeyValue[[]byte, []byte], 0),
			dfRelationBuffer: make([]storage.KeyValue[[]byte, []byte], 0),
			caseCache: *storage.NewCachedByteStorage(storage.DefaultStorage, storage.CachedByteStorageConfig{
				StoragePrefix: caseCode,
				TTL:           1 * time.Minute,
				MaxItems:      1000,
			}), //make(map[string]string),
			doneChannel: make(chan bool),
			log:         zap.L().Sugar().With("service", "sbar-store"),
		}
		if err := sbarStoreSingleton.init(); err != nil {
			panic(err)
		}
	})

	return sbarStoreSingleton
}

func (kv *SbarStore) init() error {
	kv.Lock()
	defer kv.Unlock()

	// load activity counter key from disk
	v, err := storage.DefaultStorage.Get(activityCounterKey)
	if err != nil && err != storage.ErrKeyNotFound {
		kv.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		kv.log.Info("Initialize empty activity cache.")
		kv.activityCounterCache = make(map[string]uint64)
	} else {
		err = encoding.Gob.Unmarshal(v, &kv.activityCounterCache)
		if err != nil {
			return err
		}
	}

	// load dfRelation counter key from disk
	v, err = storage.DefaultStorage.Get(dfRelationCounterKey)
	if err != nil && err != storage.ErrKeyNotFound {
		kv.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		kv.log.Info("Initialize empty directly-follows relation cache.")
		kv.dfRelationCounterCache = make(map[string]uint64)
	} else {
		err = encoding.Gob.Unmarshal(v, &kv.dfRelationCounterCache)
		if err != nil {
			return err
		}
	}

	// load startEvent counter key from disk
	v, err = storage.DefaultStorage.Get(startEventCounterKey)
	if err != nil && err != storage.ErrKeyNotFound {
		kv.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		kv.startEventCounterCache = make(map[string]uint64)
	} else {
		err = encoding.Gob.Unmarshal(v, &kv.startEventCounterCache)
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
	kv.caseCache.Set([]byte(caseId), []byte(activity))
	return nil
}

func (kv *SbarStore) GetLastActivityForCase(caseId string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	// first, try to get from cache
	v, ok := kv.caseCache.Get([]byte(caseId))
	if ok {
		return string(v), nil
	}

	return "", nil
	/*
		// otherwise, try to get from disk
		b, err := storage.DefaultStorage.Get(prefixString(caseCode, caseId))
		if err != nil && err != storage.ErrKeyNotFound {
			return "", err
		}
		if err == storage.ErrKeyNotFound {
			return "", nil
		}
		return string(b), nil*/
}

func (kv *SbarStore) RecordDirectlyFollowsRelation(from string, to string, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	dfRelation := encodeDfRelation(from, to)
	counter := kv.incr(kv.dfRelationCounterCache, dfRelation)
	k, err := key.New(prefixString(dfRelationOverTimeCode, dfRelation), timestamp)
	if err != nil {
		return err
	}

	kv.dfRelationBuffer = append(kv.dfRelationBuffer, storage.KeyValue[[]byte, []byte]{Key: k, Value: storage.Uint64ToBytes(counter)})
	dfRelationBufferMetric.WithLabelValues().Inc()
	return nil
}

func (kv *SbarStore) RecordActivity(activity string, timestamp time.Time) error {
	kv.Lock()
	defer kv.Unlock()
	counter := kv.incr(kv.activityCounterCache, activity)
	k, err := key.New(prefixString(activityOverTimeCode, activity), timestamp)
	if err != nil {
		return err
	}

	kv.activityBuffer = append(kv.activityBuffer, storage.KeyValue[[]byte, []byte]{Key: k, Value: storage.Uint64ToBytes(counter)})
	activityBufferMetric.WithLabelValues().Inc()
	if len(kv.activityBuffer) >= flushAfterEntries {
		kv.flush()
	}
	return nil
}

func (kv *SbarStore) GetActivities() []Activity {
	kv.RLock()
	defer kv.RUnlock()
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
	kv.RLock()
	defer kv.RUnlock()
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
	kv.RLock()
	defer kv.RUnlock()

	result := make([]DirectlyFollowsRelation, 0)

	for _, relation := range dfRelations {
		encodedRelation := encodeDfRelation(relation[0], relation[1])

		getCountForRelation := func(encodedRelation string, timestamp time.Time) (uint64, error) {
			k, err := key.New(prefixString(dfRelationOverTimeCode, encodedRelation), timestamp)
			if err != nil {
				return 0, err
			}

			kv.log.Debugf("%s: Seek for date %s", encodedRelation, timestamp.Format(time.RFC3339))
			keyValue, err := storage.DefaultStorage.Seek(k[:14]) // 8 for name name hash + 6 for timestamp

			if err == storage.ErrNoResults {
				kv.log.Debugf("Could not find a directly-follows relation for %s", encodedRelation)
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

func (kv *SbarStore) BinnedCountOfActivities(activities []string, binFormat string) (map[string]map[string]uint64, error) {
	kv.RLock()
	defer kv.RUnlock()

	result := make(map[string]map[string]uint64)

	datesInitialized := false
	var smallestDate, largestDate time.Time

	for _, activity := range activities {
		k, err := key.New(prefixString(activityOverTimeCode, activity), time.Now())
		if err != nil {
			return nil, err
		}

		binCounter := make(map[string]uint64)
		currentUlid := ulid.ULID{}
		var lastSeenKey key.Key
		lastSeenFormattedTime := ""

		// iterate over all events
		err = storage.DefaultStorage.Iterate(k[0:8], func(key []byte, retrieveValue func() ([]byte, error)) (bool, error) {
			lastSeenKey = key
			currentUlid.UnmarshalBinary(key[8:])
			eventTime := time.UnixMilli(int64(currentUlid.Time()))
			if !datesInitialized {
				datesInitialized = true
				smallestDate = eventTime
				largestDate = eventTime
			}
			if smallestDate.UnixMilli() > eventTime.UnixMilli() {
				smallestDate = eventTime
			}
			if largestDate.UnixMilli() < eventTime.UnixMilli() {
				largestDate = eventTime
			}
			currentTime := eventTime.Format(binFormat)
			if lastSeenFormattedTime == "" {
				lastSeenFormattedTime = currentTime
			}

			if currentTime != lastSeenFormattedTime {
				v, err := retrieveValue()
				if err != nil {
					return false, err
				}
				value := storage.BytesToUint64(v)
				binCounter[lastSeenFormattedTime] = value - 1
				lastSeenFormattedTime = currentTime
			}

			return true, nil
		})

		if err != nil {
			kv.log.Error("An unexpected error occurred during iterating through storage:", err)
		}

		if len(lastSeenKey) > 8 {
			currentUlid.UnmarshalBinary(lastSeenKey[8:])
			lastEventTime := time.UnixMilli(int64(currentUlid.Time())).Format(binFormat)
			lastValue, err := storage.DefaultStorage.Get(lastSeenKey)
			if err != nil {
				kv.log.Error("An unexpected error occurred during iterating through storage:", err)
			}
			value := storage.BytesToUint64(lastValue)
			binCounter[lastEventTime] = value
		}

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
					kv.log.Warnf("Previous counter in DailyCountOfActivities for activity %s is larger than current. This should not happen.", activity)
				} else {
					binRate[k] = binCounter[k] - binCounter[keys[i-1]]
				}
			}
		}

		result[activity] = binRate
	}

	// for all activities: insert a 0 if a date has no value
	for activity := range result {
		currentDate := smallestDate
		largestTimeFormatted := largestDate.Add(24 * time.Hour).Format(binFormat)
		for currentDate.UnixMilli() < largestDate.Add(24*time.Hour).UnixMilli() {
			formattedTime := currentDate.Format(binFormat)
			_, exists := result[activity][formattedTime]
			if !exists {
				if formattedTime != largestTimeFormatted {
					result[activity][formattedTime] = 0
				}
			}
			currentDate = currentDate.Add(24 * time.Hour)
		}
	}

	return result, nil
}

func (kv *SbarStore) CountActivities() int {
	kv.RLock()
	defer kv.RUnlock()
	return len(kv.activityCounterCache)
}

func (kv *SbarStore) CountDfRelations() int {
	kv.RLock()
	defer kv.RUnlock()
	return len(kv.dfRelationCounterCache)
}

func (kv *SbarStore) CountStartActivities() int {
	kv.RLock()
	defer kv.RUnlock()
	return len(kv.startEventCounterCache)
}

func (kv *SbarStore) RecordStartActivity(key string) {
	kv.Lock()
	defer kv.Unlock()
	kv.incr(kv.startEventCounterCache, key)
}

func (kv *SbarStore) Close() {
	kv.log.Info("Shutting down stores.SbarStore")
	defer kv.log.Info("Closed stores.SbarStore")
	kv.Lock()
	defer kv.Unlock()
	close(kv.doneChannel)
	kv.flushTicker.Stop()
	kv.caseCache.Close()
	kv.flush()
}

func (kv *SbarStore) flush() error {
	b, err := encoding.Gob.Marshal(&kv.activityCounterCache)
	if err != nil {
		return err
	}
	err = storage.DefaultStorage.Set(activityCounterKey, b)
	if err != nil {
		return err
	}
	b, err = encoding.Gob.Marshal(&kv.dfRelationCounterCache)
	if err != nil {
		return err
	}
	err = storage.DefaultStorage.Set(dfRelationCounterKey, b)
	if err != nil {
		return err
	}
	b, err = encoding.Gob.Marshal(&kv.startEventCounterCache)
	if err != nil {
		return err
	}
	err = storage.DefaultStorage.Set(startEventCounterKey, b)
	if err != nil {
		return err
	}
	/*caseBuffer := make([]storage.KeyValue[[]byte, []byte], len(kv.caseCache))
	i := 0
	for k, v := range kv.caseCache {
		caseBuffer[i] = storage.KeyValue[[]byte, []byte]{Key: prefixString(caseCode, k), Value: []byte(v)}
		i++
	}
	flushedItems := kv.flushBuffer(&caseBuffer)
	kv.log.Debugf("Flushed %d last activities for a case", flushedItems)
	kv.caseCache = make(map[string]string)*/
	flushedItems := kv.flushBuffer(&kv.activityBuffer)
	kv.log.Debugf("Flushed %d activties", flushedItems)
	flushedItems = kv.flushBuffer(&kv.dfRelationBuffer)
	kv.log.Debugf("Flushed %d directly-follows relations", flushedItems)
	activityBufferMetric.Reset()
	dfRelationBufferMetric.Reset()
	return nil
}

func (kv *SbarStore) flushBuffer(items *[]storage.KeyValue[[]byte, []byte]) int {
	count := len(*items)
	storage.DefaultStorage.SetBatch(*items)
	*items = make([]storage.KeyValue[[]byte, []byte], 0)
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
