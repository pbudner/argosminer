package key

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/cespare/xxhash/v2"
)

const size = 8 + 16

// Key represents a lexicographically sortable key
type Key []byte

func (a Key) Compare(b Key) int {
	return bytes.Compare(a, b)
}

func (a Key) Equal(b Key) bool {
	return bytes.Equal(a, b)
}

// New generates a new lexicographically sortable key
func New(name []byte, timestamp time.Time) (Key, error) {
	ulid, err := newMonotonicULIDGenerator().New(timestamp)
	if err != nil {
		return nil, err
	}

	out := make([]byte, size)
	binary.BigEndian.PutUint64(out[0:8], xxhash.Sum64(name))
	err = ulid.MarshalBinaryTo(out[8:])
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Returns the hashed name part of the key
func HashOf(k Key) uint64 {
	return binary.BigEndian.Uint64(k[0:8])
}

// Clone clones a key
func Clone(k Key) Key {
	b := make(Key, size)
	copy(b, k)
	return b[:len(k)]
}

// PrefixOf a common prefix between two keys (common leading bytes) which is
// then used as a prefix for Badger to narrow down SSTables to traverse.
func PrefixOf(seek, until Key) []byte {
	var prefix []byte

	// Calculate the minimum length
	length := len(seek)
	if len(until) < length {
		length = len(until)
	}

	// Iterate through the bytes and append common ones
	for i := 0; i < length; i++ {
		if seek[i] != until[i] {
			break
		}
		prefix = append(prefix, seek[i])
	}
	return prefix
}
