package sources

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/processors"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/radovskyb/watcher"
	"go.uber.org/zap"
)

type FileSourceConfig struct {
	Path     string `yaml:"path"`
	ReadFrom string `yaml:"read-from"`
}

type fileSource struct {
	Path             string
	ReadFrom         string
	Watcher          *watcher.Watcher
	Parsers          []parsers.Parser
	Receivers        []processors.StreamingProcessor
	lastFilePosition int64
	log              *zap.SugaredLogger
	kvStore          *stores.KvStore
}

var receivedFileEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_file",
	Name:      "received_events",
	Help:      "Total number of received events.",
}, []string{"path"})

var receivedFileEventsWithError = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_file",
	Name:      "received_events_error",
	Help:      "Total number of received events that produced an error.",
}, []string{"path"})

var lastReceivedFileEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_sources_file",
	Name:      "last_received_event",
	Help:      "Last received event for this source.",
}, []string{"path"})

func init() {
	prometheus.MustRegister(receivedFileEvents, receivedFileEventsWithError, lastReceivedFileEvent)
}

func NewFileSource(path, readFrom string, parsers []parsers.Parser, receivers []processors.StreamingProcessor, kvStore *stores.KvStore) fileSource {
	fs := fileSource{
		Path:             path,
		ReadFrom:         strings.ToLower(readFrom),
		Parsers:          parsers,
		Receivers:        receivers,
		lastFilePosition: 0,
		log:              zap.L().Sugar().With("service", "file-source"),
		kvStore:          kvStore,
	}

	lastFilePositionBytes, err := fs.kvStore.Get([]byte(fmt.Sprintf("file-source-position-%s", fs.Path)))
	if err == nil {
		fs.lastFilePosition = int64(storage.BytesToUint64(lastFilePositionBytes))
		fs.log.Infow("continuing reading file source", "position", fs.lastFilePosition)
	}

	return fs
}

func (fs *fileSource) Close() {
	fs.kvStore.Set([]byte(fmt.Sprintf("file-source-position-%s", fs.Path)), storage.Uint64ToBytes(uint64(fs.lastFilePosition)))
	fs.Watcher.Close()
	for _, parser := range fs.Parsers {
		parser.Close()
	}
}

func (fs *fileSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	fs.log.Debug("Initializing file watcher..")
	defer wg.Done()
	fs.Watcher = watcher.New()
	fs.Watcher.FilterOps(watcher.Write, watcher.Rename, watcher.Create)

	go func() {
		fs.readFile(ctx)
		for {
			select {
			case <-ctx.Done():
				fs.Close()
				return
			case event := <-fs.Watcher.Event:
				if event.Path == fs.Path {
					fs.readFile(ctx)
				}
			case err := <-fs.Watcher.Error:
				fs.log.Error(err)
			case <-fs.Watcher.Closed:
				return
			}
		}
	}()

	if err := fs.Watcher.Add(filepath.Dir(fs.Path)); err != nil {
		fs.log.Error(err)
	}

	if err := fs.Watcher.Start(time.Millisecond * 1000); err != nil {
		fs.log.Error(err)
	}

	fs.log.Info("Closed file source")
}

func (fs *fileSource) readFile(ctx context.Context) {
	f, err := os.Open(fs.Path)
	if err != nil {
		fs.log.Fatal(err)
	}

	if fs.lastFilePosition == 0 {
		if fs.ReadFrom == "beginning" || fs.ReadFrom == "start" {
			fs.lastFilePosition = 0
		} else {
			pos, err := f.Seek(0, 2)
			if err != nil {
				fs.log.Fatal(err)
			}

			fs.lastFilePosition = pos
		}
	}
	_, err = f.Seek(fs.lastFilePosition, 0) // 0 beginning, 1 current, 2 end
	if err != nil {
		fs.log.Error(err)
	}

	defer f.Close()
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			fs.log.Info("Aborting readFile operation..")

			newPosition, err := f.Seek(0, 1)
			if err != nil {
				fs.log.Error(err)
			}

			fs.lastFilePosition = newPosition
			return
		default:
			lastReceivedFileEvent.WithLabelValues(fs.Path).SetToCurrentTime()
			receivedFileEvents.WithLabelValues(fs.Path).Inc()
			line := scanner.Text()
			line = strings.ReplaceAll(line, "\"", "")

			var event *events.Event
			var parseErr error
			// event, parseErr = fs.Parser.Parse([]byte(line))
			for _, parser := range fs.Parsers {
				event, parseErr = parser.Parse([]byte(line))
				if parseErr == nil && event != nil {
					break
				}
			}

			if parseErr != nil {
				fs.log.Error(err)
				receivedFileEventsWithError.WithLabelValues(fs.Path).Inc()
				continue
			}

			if event != nil {
				for _, receiver := range fs.Receivers {
					err := receiver.Append(event)
					if err != nil {
						fs.log.Error(err)
						receivedFileEventsWithError.WithLabelValues(fs.Path).Inc()
					}
				}
			}
		}
	}

	newPosition, err := f.Seek(0, 1)
	if err != nil {
		fs.log.Error(err)
	}

	fs.lastFilePosition = newPosition
}
