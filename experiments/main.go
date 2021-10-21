package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/dustin/go-humanize"
	ulid "github.com/oklog/ulid/v2"
)

// based on this blog post https://barkeywolf.consulting/posts/badger-event-store/

const eventDbPath = "/Volumes/PascalsSSD/badgerdb_tmp"

type MonotonicULIDsource struct {
	sync.Mutex            // mutex to allow clean concurrent access
	entropy    *rand.Rand // the entropy source
	lastMs     uint64     // the last millisecond timestamp it encountered
	lastULID   ulid.ULID  // the last ULID it generated using "github.com/oklog/ulid"
}

func NewMonotonicULIDsource(entropy *rand.Rand) *MonotonicULIDsource {

	// get an initial ULID to kick the monotonic generation off with
	inital, err := ulid.New(ulid.Now(), entropy)
	if err != nil {
		panic(err)
	}

	return &MonotonicULIDsource{
		entropy:  entropy,
		lastMs:   0,
		lastULID: inital,
	}
}

func incrementBytes(in []byte) []byte {
	const (
		minByte byte = 0
		maxByte byte = 255
	)

	// copy the byte slice
	out := make([]byte, len(in))
	copy(out, in)

	// iterate over the byte slice from right to left
	// most significant byte == first byte (big-endian)
	leastSigByteIdx := len(out) - 1
	mostSigByteIdex := 0
	for i := leastSigByteIdx; i >= mostSigByteIdex; i-- {

		// If its value is 255, rollover back to 0 and try the next byte.
		if out[i] == maxByte {
			out[i] = minByte
			continue
		}
		// Else: increment.
		out[i]++
		return out
	}
	// panic if the increments are exhausted
	panic("errOverflow")
}

type FakeEvent struct {
	Number    int // we also add a number to keep track of the ground-truth creation order
	Timestamp time.Time
	ID        ulid.ULID
}

func (u *MonotonicULIDsource) New(t time.Time) (ulid.ULID, error) {
	u.Lock()
	defer u.Unlock()

	ms := ulid.Timestamp(t)
	var err error
	if ms > u.lastMs {
		u.lastMs = ms
		u.lastULID, err = ulid.New(ms, u.entropy)
		return u.lastULID, err
	}

	// if the ms are the same, increment the entropy part of the last ULID
	// and use it as the entropy part of the new ULID.
	incrEntropy := incrementBytes(u.lastULID.Entropy())
	var dup ulid.ULID
	dup.SetTime(ms)
	if err := dup.SetEntropy(incrEntropy); err != nil {
		return dup, err
	}
	u.lastULID = dup
	u.lastMs = ms
	return dup, nil
}

func bytesToTime(k []byte) time.Time {
	unix := uint64(k[5]) | uint64(k[4])<<8 |
		uint64(k[3])<<16 | uint64(k[2])<<24 |
		uint64(k[1])<<32 | uint64(k[0])<<40
	return time.Unix(0, int64(unix)*int64(time.Millisecond))
}

func timeToBytes(ts time.Time) []byte {
	t := ulid.Timestamp(ts)
	r := make([]byte, 6)
	r[0] = byte(t >> 40)
	r[1] = byte(t >> 32)
	r[2] = byte(t >> 24)
	r[3] = byte(t >> 16)
	r[4] = byte(t >> 8)
	r[5] = byte(t)
	return r
}

func main() {
	opts := badger.DefaultOptions(eventDbPath).WithLoggingLevel(badger.ERROR)
	eventDB, _ := badger.Open(opts)
	defer eventDB.Close()

	// validate badger iteration order is equal to original creation order
	// var retrieved uint64
	fmt.Println("Starting tests..")
	start := time.Now()
	if err := eventDB.View(func(txn *badger.Txn) error {
		// fetch first
		// create a Badger iterator with the default settings
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		// have the iterator walk the LMB tree
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			fmt.Printf("First item: %s\n", bytesToTime(k[:6]))
			break
			/*v, err := item.ValueCopy(nil)
			if err != nil {
				panic(err)
			}

			// deserialize the fake events and store them
			var des FakeEvent
			err = json.Unmarshal(v, &des)
			if err != nil {
				panic(err)
			}*/

			//retrieved = append(retrieved, des)
		}
		it.Close()

		// fetch last value
		opts.Reverse = true
		it = txn.NewIterator(opts)
		// have the iterator walk the LMB tree
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			fmt.Printf("Last Item: %s\n", bytesToTime(k[:6]))
			break
		}
		it.Close()

		/*opts.Reverse = false
		it = txn.NewIterator(opts)
		// have the iterator walk the LMB tree
		for it.Rewind(); it.Valid(); it.Next() {
			retrieved++
		}
		it.Close()
		fmt.Printf("Number of total events: %s\n", humanize.Comma(int64(retrieved)))*/

		opts.Reverse = false
		it = txn.NewIterator(opts)

		from := time.Date(2021, 10, 21, 0, 0, 0, 0, time.Local)
		fromEncoded := timeToBytes(from)
		to := time.Date(2021, 10, 21, 0, 50, 0, 0, time.Local)
		toEncoded := timeToBytes(to)
		fmt.Printf("[%s,%s]\n", bytesToTime(fromEncoded), bytesToTime(toEncoded))
		// have the iterator walk the LMB tree
		found := false
		var counter int64 = 0
		it.Rewind()
		var searched int64 = 0
		// seek was fucking important here... it finds our starting key..
		for it.Seek(fromEncoded); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()[:6]
			if !found {
				if bytes.Compare(key, fromEncoded) < 0 {
					searched++
					continue
				}
				if bytes.Compare(key, toEncoded) == 1 {
					break
				}
				found = true
				fmt.Printf("Found inclusive start %s after %s iterations\n", bytesToTime(key), humanize.Comma(searched))
			}

			if bytes.Compare(key, toEncoded) > 0 {
				fmt.Printf("Found exclusive end %s\n", bytesToTime(key))
				break
			}
			counter++
		}

		fmt.Printf("Found %s items in the specified range\n", humanize.Comma(counter))
		it.Close()

		return nil
	}); err != nil {
		panic(err)
	}

	elapsed := time.Since(start)
	fmt.Printf("All operations took %s\n", elapsed)
}

func bla() {
	// reproducible entropy source
	entropy := rand.New(rand.NewSource(time.Unix(1000000, 0).UnixNano()))

	// sub-ms safe ULID generator
	ulidSource := NewMonotonicULIDsource(entropy)

	now := time.Now()
	id, _ := ulidSource.New(now)
	timestamp := id[:6]
	unix := uint64(timestamp[5]) | uint64(timestamp[4])<<8 |
		uint64(timestamp[3])<<16 | uint64(timestamp[2])<<24 |
		uint64(timestamp[1])<<32 | uint64(timestamp[0])<<40
	date := time.Unix(0, int64(unix)*int64(time.Millisecond))
	fmt.Println(date)
}

func write() {
	// reproducible entropy source
	entropy := rand.New(rand.NewSource(time.Unix(1000000, 0).UnixNano()))

	// sub-ms safe ULID generator
	ulidSource := NewMonotonicULIDsource(entropy)

	// generate fake events that contain a ground-truth sorting order and a ULID
	var events []ulid.ULID
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < int(1e7); i++ {
		id, _ := ulidSource.New(start)
		events = append(events, id)
		start = start.Add(time.Minute * 1)
	}

	entropy.Shuffle(len(events), func(i, j int) {
		events[i], events[j] = events[j], events[i]
	})

	opts := badger.DefaultOptions(eventDbPath)
	db, _ := badger.Open(opts)
	defer db.Close()

	// open a database transaction
	txn := db.NewTransaction(true)

	for _, e := range events {

		// serialize the event payload (to JSON for simplicity)
		/*eSerial, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}*/

		// serialize the ULID to its binary form
		binID, err := e.MarshalBinary()
		if err != nil {
			panic(err)
		}

		// add the insert operation to the transaction
		// and open a new transaction if this one is full
		if err := txn.Set(binID, []byte{}); err == badger.ErrTxnTooBig {
			if err := txn.Commit(); err != nil {
				panic(err)
			}
			txn = db.NewTransaction(true)
			if err := txn.Set(binID, []byte{}); err != nil {
				panic(err)
			}
		}
	}

	// flush the transaction
	if err := txn.Commit(); err != nil {
		panic(err)
	}
}
