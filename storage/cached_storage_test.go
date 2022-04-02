package storage

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MyByte []byte

func (b MyByte) Marshal() []byte {
	return b
}

func (b MyByte) Unmarshal(a []byte) Serializable {
	return MyByte(a)
}

func TestTTLCache(t *testing.T) {
	dir, err := ioutil.TempDir("", "db-test")
	require.NoError(t, err)
	defer removeDir(dir)

	storage := NewDiskStorage(Config{Path: dir})
	defer storage.Close()

	cache := NewCachedByteStorage[MyByte](storage, CachedStorageConfig{
		StoragePrefix: []byte("test"),
		TTL:           2 * time.Second,
		MaxItems:      2,
	})

	cache.Set([]byte("k1"), MyByte("v1"))
	cache.Set([]byte("k2"), MyByte("v2"))

	v, _ := cache.Get([]byte("k1"))
	require.Equal(t, MyByte("v1"), v)

	v, _ = cache.Get([]byte("k2"))
	require.Equal(t, MyByte("v2"), v)

	time.Sleep(3 * time.Second)

	v, _ = cache.Get([]byte("k2"))
	require.Equal(t, MyByte("v2"), v)

	// close cache, and open it again
	cache.Close()
	cache = NewCachedByteStorage[MyByte](storage, CachedStorageConfig{
		StoragePrefix: []byte("test"),
		TTL:           2 * time.Second,
		MaxItems:      2,
	})

	v, _ = cache.Get([]byte("k1"))
	require.Equal(t, MyByte("v1"), v)

	cache.Set([]byte("k3"), MyByte("v3"))
	cache.Set([]byte("k4"), MyByte("v4"))
	cache.Set([]byte("k5"), MyByte("v5"))
	cache.Set([]byte("k6"), MyByte("v6"))
	v, _ = cache.Get([]byte("k2"))
	require.Equal(t, MyByte("v2"), v)
	v, _ = cache.Get([]byte("k6"))
	require.Equal(t, MyByte("v6"), v)
	v, _ = cache.Get([]byte("k2"))
	require.Equal(t, MyByte("v2"), v)
	time.Sleep(500 * time.Millisecond) // ToDo: there is a chance, when an element is evicted, but not yet written to disk. Unlikely with higher number of max items, thoguh
	v, _ = cache.Get([]byte("k5"))
	require.Equal(t, MyByte("v5"), v)
}
