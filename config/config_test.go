package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	rawConfig := `---
listener: localhost:4711
logger:
  level: info
database:
  path: /Volumes/PascalsSSD/ArgosMiner/badgerv2_nopointer_nosync
ignore-activities:
  - name: 'krankenAngeboteWiederherstellen::Start'
  - name: '.*::Start'
    remove: true
pipeline:
  - name: sources.file
    path: 'data/eventlog.csv'
    read-from: start
    connects:
      - name: transforms.csv_parser
        delimiter: ';'
        case-id-column: 1
        activity-column: 2
        timestamp-column: 0
        timestamp-format: '02/01/2006 15:04:05.000000'
        ignore-when:
          - column: 2
            condition: '=='
            value: 'IgnoreMe'
        connects:
          - name: transforms.event_buffer
            max-events: 100000
            max-age: 1m
            flush-interval: 1s
            ignore-events-older-than: 5m`
	cfg, err := NewConfigFromStr([]byte(rawConfig))
	require.NoError(t, err)
	require.Equal(t, "localhost:4711", cfg.Listener)
	require.Equal(t, "", cfg.BaseURL)
	require.Equal(t, "/Volumes/PascalsSSD/ArgosMiner/badgerv2_nopointer_nosync", cfg.Database.Path)
	require.Equal(t, 1, len(cfg.Pipeline))
	require.Equal(t, "sources.file", cfg.Pipeline[0].Name)
	require.Equal(t, false, cfg.Pipeline[0].Disabled)
	require.Equal(t, 1, len(cfg.Pipeline[0].Connects))
	require.Equal(t, "path", cfg.Pipeline[0].Config["connects"])
}
