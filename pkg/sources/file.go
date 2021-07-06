package sources

import (
	"bufio"
	"os"
	"path/filepath"
	"time"

	"github.com/pbudner/argosminer-collector/pkg/parsers"
	"github.com/radovskyb/watcher"
	log "github.com/sirupsen/logrus"
)

type fileSource struct {
	Path     string
	ReadFrom string
	Watcher  *watcher.Watcher
	Parser   parsers.Parser
}

func NewFileSource(path, readFrom string, parser parsers.Parser) fileSource {
	fs := fileSource{
		Path:     path,
		ReadFrom: readFrom,
		Parser:   parser,
	}
	return fs
}

func (fs fileSource) Close() {
	fs.Watcher.Close()
}

func (fs fileSource) Run() {
	fs.initWatcher()
}

func (fs fileSource) readFile() {
	f, err := os.Open(fs.Path)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fs.Parser.Parse(line)
	}
}

func (fs fileSource) initWatcher() {
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
				log.Fatalln(err)
			case <-fs.Watcher.Closed:
				return
			}
		}
	}()

	if err := fs.Watcher.Add(filepath.Dir(fs.Path)); err != nil {
		log.Fatalln(err)
	}

	if err := fs.Watcher.Start(time.Millisecond * 1000); err != nil {
		log.Fatalln(err)
	}
}
