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

	"github.com/pbudner/argosminer/algorithms"
	"github.com/pbudner/argosminer/parsers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/radovskyb/watcher"
	log "github.com/sirupsen/logrus"
)

type FileSourceConfig struct {
	Path     string `yaml:"path"`
	ReadFrom string `yaml:"read-from"`
}

type fileSource struct {
	Path             string
	ReadFrom         string
	Watcher          *watcher.Watcher
	Parser           parsers.Parser
	Receivers        []algorithms.StreamingAlgorithm
	lastFilePosition int64
}

var receivedFileEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_source_file",
	Name:      "received_events",
	Help:      "Total number of received events.",
})

var receivedFileEventsWithError = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_source_file",
	Name:      "received_events_error",
	Help:      "Total number of received events that produced an error.",
})

func init() {
	prometheus.MustRegister(receivedFileEvents)
	prometheus.MustRegister(receivedFileEventsWithError)
}

func NewFileSource(path, readFrom string, parser parsers.Parser, receivers []algorithms.StreamingAlgorithm) fileSource {
	fs := fileSource{
		Path:             path,
		ReadFrom:         strings.ToLower(readFrom),
		Parser:           parser,
		Receivers:        receivers,
		lastFilePosition: 0,
	}

	return fs
}

func (fs *fileSource) Close() {
	err := os.WriteFile("test.txt", []byte(fmt.Sprintf("Position: %d", fs.lastFilePosition)), 0644)
	if err != nil {
		log.Error(err)
	}
	fs.Watcher.Close()
	fs.Parser.Close()
}

func (fs *fileSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Debug("Initializing file watcher..")
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
				log.Error(err)
			case <-fs.Watcher.Closed:
				return
			}
		}
	}()

	if err := fs.Watcher.Add(filepath.Dir(fs.Path)); err != nil {
		log.Error(err)
	}

	if err := fs.Watcher.Start(time.Millisecond * 1000); err != nil {
		log.Error(err)
	}

	log.Info("Closed file source")
}

func (fs *fileSource) readFile(ctx context.Context) {
	f, err := os.Open(fs.Path)
	if err != nil {
		log.Fatal(err)
	}

	if fs.lastFilePosition == 0 {
		if fs.ReadFrom == "beginning" || fs.ReadFrom == "start" {
			fs.lastFilePosition = 0
		} else {
			pos, err := f.Seek(0, 2)
			if err != nil {
				log.Fatal(err)
			}

			fs.lastFilePosition = pos
		}
	}
	_, err = f.Seek(fs.lastFilePosition, 0) // 0 beginning, 1 current, 2 end
	if err != nil {
		log.Error(err)
	}

	defer f.Close()
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			log.Info("Aborting readFile operation..")

			newPosition, err := f.Seek(0, 1)
			if err != nil {
				log.Error(err)
			}

			fs.lastFilePosition = newPosition
			return
		default:
			receivedFileEvents.Inc()
			line := scanner.Text()
			line = strings.ReplaceAll(line, "\"", "")
			event, err := fs.Parser.Parse(line)
			if err != nil {
				log.Error(err)
				receivedFileEventsWithError.Inc()
				continue
			}

			if event != nil {
				for _, receiver := range fs.Receivers {
					err := receiver.Append(*event)
					if err != nil {
						log.Error(err)
						receivedFileEventsWithError.Inc()
					}
				}
			}
		}
	}

	newPosition, err := f.Seek(0, 1)
	if err != nil {
		log.Error(err)
	}

	fs.lastFilePosition = newPosition
}
