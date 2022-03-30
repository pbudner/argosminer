package storage

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTTLCache(t *testing.T) {
	dir, err := ioutil.TempDir("", "db-test")
	require.NoError(t, err)
	defer removeDir(dir)

	storage := NewDiskStorage(Config{Path: dir})
	defer storage.Close()

	cache := NewCachedByteStorage(storage, CachedByteStorageConfig{
		StoragePrefix: []byte("test"),
		TTL:           2 * time.Second,
		MaxItems:      2,
	})

	cache.Set([]byte("k1"), []byte("v1"))
	cache.Set([]byte("k2"), []byte("v2"))

	v, _ := cache.Get([]byte("k1"))
	require.Equal(t, v, []byte("v1"))

	v, _ = cache.Get([]byte("k2"))
	require.Equal(t, v, []byte("v2"))

	time.Sleep(3 * time.Second)

	v, _ = cache.Get([]byte("k2"))
	require.Equal(t, v, []byte("v2"))

	// close cache, and open it again
	cache.Close()
	cache = NewCachedByteStorage(storage, CachedByteStorageConfig{
		StoragePrefix: []byte("test"),
		TTL:           2 * time.Second,
		MaxItems:      2,
	})

	v, _ = cache.Get([]byte("k1"))
	require.Equal(t, v, []byte("v1"))

	cache.Set([]byte("k3"), []byte("v3"))
	cache.Set([]byte("k4"), []byte("v4"))
	cache.Set([]byte("k5"), []byte("v5"))
	cache.Set([]byte("k6"), []byte("v6"))
	v, _ = cache.Get([]byte("k2"))
	require.Equal(t, v, []byte("v2"))
	v, _ = cache.Get([]byte("k6"))
	require.Equal(t, v, []byte("v6"))
	v, _ = cache.Get([]byte("k4"))
	require.Equal(t, v, []byte("v4"))
}
