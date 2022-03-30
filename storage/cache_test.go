package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

func TestTTLCache(t *testing.T) {
	loader := ttlcache.LoaderFunc[string, string](
		func(c *ttlcache.Cache[string, string], key string) *ttlcache.Item[string, string] {
			item := c.Set(key, "value from file", ttlcache.DefaultTTL)
			return item
		},
	)

	cache := ttlcache.New(
		ttlcache.WithTTL[string, string](2*time.Second),
		ttlcache.WithCapacity[string, string](10000),
		ttlcache.WithLoader[string, string](loader),
	)

	envictedItemKey := ""
	envictedItemValue := ""

	cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, string]) {
		envictedItemKey = item.Key()
		envictedItemValue = item.Value()
	})

	go cache.Start()

	require.Equal(t, envictedItemKey, "")
	cache.Set("myValue", "das ist mein Wert", ttlcache.DefaultTTL)
	cache.Set("myValue", "das ist mein Wert2", ttlcache.DefaultTTL)
	require.Equal(t, cache.Get("myValue").Value(), "das ist mein Wert2")
	time.Sleep(3 * time.Second)
	require.Equal(t, cache.Get("myValue").Value(), "value from file")
	require.Equal(t, envictedItemKey, "myValue")
	require.Equal(t, envictedItemValue, "das ist mein Wert2")
	cache.Stop()
}
