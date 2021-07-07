package sources

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbudner/argosminer-collector/pkg/algorithms"
	"github.com/pbudner/argosminer-collector/pkg/parsers"
	"github.com/radovskyb/watcher"
	log "github.com/sirupsen/logrus"
)

type fileSource struct {
	Path             string
	ReadFrom         string
	Watcher          *watcher.Watcher
	Parser           parsers.Parser
	Receivers        []algorithms.StreamingAlgorithm
	lastFilePosition int64
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
	fs.Watcher.Close()
	fs.Parser.Close()
}

func (fs *fileSource) Run() {
	fs.initWatcher()
}

func (fs *fileSource) readFile() {
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

	err = fs.Parser.Parse(f, fs.Receivers)
	if err != nil {
		log.Error(err)
		return
	}

	newPosition, err := f.Seek(0, 1)
	if err != nil {
		log.Error(err)
	}

	fs.lastFilePosition = newPosition
}

func (fs *fileSource) initWatcher() {
	log.Debug("Initializing the file watcher..")
	fs.Watcher = watcher.New()
	fs.Watcher.FilterOps(watcher.Write, watcher.Rename, watcher.Create)

	go func() {
		for {
			select {
			case event := <-fs.Watcher.Event:
				if event.Path == fs.Path {
					fs.readFile()
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

	// starting a first file scan
	fs.readFile()

	if err := fs.Watcher.Start(time.Millisecond * 1000); err != nil {
		log.Error(err)
	}

	log.Debug("Closing file watcher.")
}
