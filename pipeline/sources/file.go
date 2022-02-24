package sources

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pbudner/argosminer/pipeline"
	_ "github.com/pbudner/argosminer/pipeline/transforms"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/radovskyb/watcher"
	"go.uber.org/zap"
)

type FileConfig struct {
	Path     string `yaml:"path"`
	ReadFrom string `yaml:"read-from"`
}

type file struct {
	pipeline.Consumer
	pipeline.Publisher
	Path             string
	ReadFrom         string
	Watcher          *watcher.Watcher
	lastFilePosition int64
	log              *zap.SugaredLogger
}

var receivedFileEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_file",
	Name:      "received_events",
	Help:      "Total number of received events.",
}, []string{"path"})

var lastReceivedFileEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_sources_file",
	Name:      "last_received_event",
	Help:      "Last received event for this source.",
}, []string{"path"})

func init() {
	prometheus.MustRegister(receivedFileEvents, lastReceivedFileEvent)
	pipeline.RegisterComponent("sources.file", FileConfig{}, func(config interface{}) pipeline.Component {
		return NewFile(config.(FileConfig))
	})
}

func NewFile(cfg FileConfig) *file {
	fs := file{
		Path:             cfg.Path,
		ReadFrom:         strings.ToLower(cfg.ReadFrom),
		lastFilePosition: 0,
		log:              zap.L().Sugar().With("service", "file-source"),
		// TODO: kvStore:          kvStore,
	}

	lastFilePositionBytes, err := stores.GetKvStore().Get([]byte(fmt.Sprintf("file-source-position-%s", fs.Path)))
	if err == nil {
		fs.lastFilePosition = int64(storage.BytesToUint64(lastFilePositionBytes))
		fs.log.Infow("continuing reading file source", "position", fs.lastFilePosition)
	}

	return &fs
}

func (fs *file) Link(parent chan interface{}) {
	panic("A source component must not be linked to a parent pipeline component")
}

func (fs *file) Close() {
	// TODO: fs.kvStore.Set([]byte(fmt.Sprintf("file-source-position-%s", fs.Path)), storage.Uint64ToBytes(uint64(fs.lastFilePosition)))
	fs.Watcher.Close()
}

func (fs *file) Run(wg *sync.WaitGroup, ctx context.Context) {
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

func (fs *file) readFile(ctx context.Context) {
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
			line := scanner.Bytes()
			line = bytes.ReplaceAll(line, []byte("\""), []byte(""))
			fs.Publish(line, false) // we only want to send an input to one working parser
		}
	}

	newPosition, err := f.Seek(0, 1)
	if err != nil {
		fs.log.Error(err)
	}

	fs.lastFilePosition = newPosition
}
