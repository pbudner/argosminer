package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pbudner/argosminer/storage/key"
	"github.com/stretchr/testify/require"
)

func TestDiskStorageGetAndSet(t *testing.T) {
	dir, err := ioutil.TempDir("", "db-test")
	require.NoError(t, err)
	defer removeDir(dir)

	storage := NewDiskStorage(Config{Path: dir})
	defer storage.Close()

	// fill with test data
	storage.Set([]byte("k1"), []byte("v1"))
	storage.Set([]byte("k2"), []byte("v2"))
	v1, err := storage.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v1)
	v2, err := storage.Get([]byte("k2"))
	require.Equal(t, []byte("v2"), v2)
	require.NoError(t, err)
}

func TestDiskStorageSeek(t *testing.T) {
	dir, err := ioutil.TempDir("", "db-test")
	require.NoError(t, err)
	defer removeDir(dir)

	storage := NewDiskStorage(Config{Path: dir})
	defer storage.Close()

	// fill with test data
	k1, err := key.New([]byte("my_key"), time.Date(2021, 11, 11, 10, 1, 2, 3, time.UTC))
	require.NoError(t, err)
	k2, err := key.New([]byte("my_key"), time.Date(2021, 11, 11, 11, 1, 2, 3, time.UTC))
	require.NoError(t, err)
	k3, err := key.New([]byte("my_key"), time.Date(2021, 11, 11, 12, 1, 2, 3, time.UTC))
	require.NoError(t, err)
	storage.Set(k1, []byte("v1"))
	storage.Set(k2, []byte("v2"))
	storage.Set(k3, []byte("v3"))

	// provoke an error
	seekKey, err := key.New([]byte("my_key"), time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	kv, err := storage.Seek(seekKey[:14])
	require.Equal(t, err, ErrNoResults)

	// return k1
	seekKey, err = key.New([]byte("my_key"), time.Date(2021, 11, 11, 11, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	kv, err = storage.Seek(seekKey[:14])
	require.NoError(t, err)
	require.Equal(t, "v1", string(kv.Value))

	// return k2
	seekKey, err = key.New([]byte("my_key"), time.Date(2021, 11, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	kv, err = storage.Seek(seekKey[:14])
	require.NoError(t, err)
	require.Equal(t, "v2", string(kv.Value))

	// return k3
	seekKey, err = key.New([]byte("my_key"), time.Date(2021, 11, 11, 13, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	kv, err = storage.Seek(seekKey[:14])
	require.NoError(t, err)
	require.Equal(t, "v3", string(kv.Value))

	// return k3
	seekKey, err = key.New([]byte("my_key"), time.Date(2022, 11, 11, 11, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	kv, err = storage.Seek(seekKey[:14])
	require.NoError(t, err)
	require.Equal(t, "v3", string(kv.Value))
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Error while removing dir: %v\n", err)
	}
}
