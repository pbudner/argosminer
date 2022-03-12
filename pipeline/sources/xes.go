package sources

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/labstack/gommon/log"
	"github.com/pbudner/argosminer/pipeline"
	"github.com/pbudner/argosminer/pipeline/transforms"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/radovskyb/watcher"
	xmlparser "github.com/tamerh/xml-stream-parser"
	"go.uber.org/zap"
)

type XesConfig struct {
	Path      string `yaml:"path"`
	CaseId    string `yaml:"case-id-key"`
	Activity  string `yaml:"activity-key"`
	Timestamp string `yaml:"timestamp-key"`
}

type xes struct {
	pipeline.Consumer
	pipeline.Publisher
	Config           XesConfig
	Watcher          *watcher.Watcher
	lastFilePosition int64
	log              *zap.SugaredLogger
	timestampParser  *transforms.TimestampParser
}

var receivedXesEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_xes",
	Name:      "received_events",
	Help:      "Total number of received events.",
}, []string{"path"})

var lastReceivedXesEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_sources_xes",
	Name:      "last_received_event",
	Help:      "Last received event.",
}, []string{"path"})

func init() {
	prometheus.MustRegister(receivedXesEvents, lastReceivedXesEvent)
	pipeline.RegisterComponent("sources.xes", XesConfig{}, func(config interface{}) pipeline.Component {
		return NewXes(config.(XesConfig))
	})
}

func NewXes(cfg XesConfig) *xes {
	fs := xes{
		Config:           cfg,
		lastFilePosition: 0,
		log:              zap.L().Sugar().With("service", "xes-source"),
		timestampParser:  transforms.NewTimestampParser("2006-01-02T15:04:05.000Z", "UTC"),
	}

	lastFilePositionBytes, err := stores.GetKvStore().Get([]byte(fmt.Sprintf("xes-source-position-%s", fs.Config.Path)))
	if err == nil {
		fs.lastFilePosition = int64(storage.BytesToUint64(lastFilePositionBytes))
		fs.log.Infow("continuing reading xes source", "position", fs.lastFilePosition)
	}

	return &fs
}

func (fs *xes) Link(parent <-chan interface{}) {
	panic("A source component must not be linked to a parent pipeline component")
}

func (fs *xes) Close() {
	// TODO: fs.kvStore.Set([]byte(fmt.Sprintf("file-source-position-%s", fs.Path)), storage.Uint64ToBytes(uint64(fs.lastFilePosition)))
	fs.Watcher.Close()
}

func (fs *xes) Run(wg *sync.WaitGroup, ctx context.Context) {
	fs.log.Info("Starting pipeline.sources.file")
	defer wg.Done()
	defer fs.log.Info("Shutting down pipeline.sources.file")
	time.Sleep(1 * time.Second)
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
				if event.Path == fs.Config.Path {
					fs.readFile(ctx)
				}
			case err := <-fs.Watcher.Error:
				fs.log.Error(err)
			case <-fs.Watcher.Closed:
				return
			}
		}
	}()

	if err := fs.Watcher.Add(filepath.Dir(fs.Config.Path)); err != nil {
		fs.log.Error(err)
	}

	if err := fs.Watcher.Start(time.Millisecond * 1000); err != nil {
		fs.log.Error(err)
	}
}

func (fs *xes) readFile(ctx context.Context) {
	f, err := os.Open(fs.Config.Path)
	if err != nil {
		fs.log.Error(err)
		return
	}

	if fs.lastFilePosition == 0 {
		fs.lastFilePosition = 0
	}

	_, err = f.Seek(fs.lastFilePosition, 0) // 0 beginning, 1 current, 2 end
	if err != nil {
		fs.log.Error(err)
	}

	defer f.Close()
	scanner := bufio.NewReaderSize(f, 8*1024*8)
	parser := xmlparser.NewXMLParser(scanner, "trace")

	for traceXml := range parser.Stream() {
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
			// 1st. Identify Case ID
			var caseId string
			for _, stringXml := range traceXml.Childs["string"] {
				if stringXml.Attrs["key"] == fs.Config.CaseId {
					caseId = stringXml.Attrs["value"]
				}
			}

			// 2nd. Iterate through events
			for _, eventXml := range traceXml.Childs["event"] {
				var activity string
				var rawTimestamp string
				for _, stringXml := range eventXml.Childs["string"] {
					if stringXml.Attrs["key"] == fs.Config.Activity {
						activity = stringXml.Attrs["value"]
					}
				}
				for _, dateXml := range eventXml.Childs["date"] {
					if dateXml.Attrs["key"] == fs.Config.Timestamp {
						rawTimestamp = dateXml.Attrs["value"]
					}
				}

				timestamp, err := fs.timestampParser.Parse(rawTimestamp)
				if err != nil {
					log.Error(err)
				}

				lastReceivedXesEvent.WithLabelValues(fs.Config.Path).SetToCurrentTime()
				receivedXesEvents.WithLabelValues(fs.Config.Path).Inc()
				fs.Publish(pipeline.Event{
					CaseId:       caseId,
					ActivityName: activity,
					Timestamp:    timestamp,
				})
			}
		}
	}

	newPosition, err := f.Seek(0, 1)
	if err != nil {
		fs.log.Error(err)
	}

	fs.lastFilePosition = newPosition
}
