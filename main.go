package main

import (
	"context"
	"embed"
	"flag"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	prom "github.com/labstack/echo-contrib/prometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/pbudner/argosminer/api"
	"github.com/pbudner/argosminer/config"
	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/processors"
	"github.com/pbudner/argosminer/sources"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.uber.org/zap"
)

//go:embed ui/dist
var embededFiles embed.FS

var (
	GitCommit string = "dev"
	Version   string = "-live"
)

var (
	processStartedGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "process_started",
		Help: "Starttime of the process as a unix timestamp.",
	})
)

func init() {
	// register global prometheus metrics
	prometheus.MustRegister(processStartedGauge, collectors.NewBuildInfoCollector())
	processStartedGauge.SetToCurrentTime()
}

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	log := logger.Sugar()

	var configPath string
	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.Parse()

	var (
		cfg *config.Config
		err error
	)

	if configPath == "" {
		cfg = config.DefaultConfig()
	} else {
		cfg, err = config.NewConfig(configPath)
		if err != nil {
			log.Fatalw("unexpected error during unmarshalling provided log", "error", err, "path", configPath)
		}
	}

	logger, _ = cfg.Logger.Build()
	log = logger.Sugar()
	undo := zap.ReplaceGlobals(logger)
	defer undo()

	log.Infow("Starting ArgosMiner", "version", Version, "commit", GitCommit)

	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// ensure data path exists
	err = os.MkdirAll(cfg.DataPath, os.ModePerm)
	if err != nil {
		log.Panicw("Could not ensure data path exists", "path", cfg.DataPath, "error", err)
	}

	// check data path exists
	if _, err := os.Stat(cfg.DataPath); os.IsNotExist(err) {
		log.Panicw("Could not open database as data path does not exist", "path", cfg.DataPath, "error", err)
	}

	// open storage
	store := storage.NewDiskStorage(cfg.DataPath)

	// initialize stores
	eventStore := stores.NewEventStore(store)
	kvStore := stores.NewKvStore(store)
	sbarStore, err := stores.NewSbarStore(store)
	if err != nil {
		log.Fatal(err)
	}

	eventSampler := utils.NewEventSampler(eventStore)

	receiverList := []processors.StreamingProcessor{
		processors.NewEventProcessor(eventStore),
		processors.NewDfgStreamingAlgorithm(sbarStore),
	}
	for _, source := range cfg.Sources {
		if !source.Enabled {
			continue
		}

		// file Source
		if source.FileConfig != nil {
			log.Debug("starting a file source...")
			wg.Add(1)
			parserSlice := make([]parsers.Parser, 0)
			for _, parser := range source.CsvParser {
				parserSlice = append(parserSlice, parsers.NewCsvParser(*parser))
			}
			for _, parser := range source.JsonParser {
				parserSlice = append(parserSlice, parsers.NewJsonParser(*parser))
			}
			fs := sources.NewFileSource(source.FileConfig.Path, source.FileConfig.ReadFrom, parserSlice, receiverList, kvStore)
			go fs.Run(ctx, wg)
		}

		// kafka Source
		if source.KafkaConfig != nil {
			log.Debug("starting kafka source...")
			wg.Add(1)
			parserSlice := make([]parsers.Parser, 0)
			for _, parser := range source.CsvParser {
				parserSlice = append(parserSlice, parsers.NewCsvParser(*parser))
			}
			for _, parser := range source.JsonParser {
				parserSlice = append(parserSlice, parsers.NewJsonParser(*parser))
			}

			fs := sources.NewKafkaSource(*source.KafkaConfig, parserSlice)
			for _, receiver := range receiverList {
				fs.AddReceiver(receiver)
			}

			go fs.Run(ctx, wg)
		}
	}

	e := echo.New()
	e.Use(
		middleware.Recover(),   // Recover from all panics to always have your server up
		middleware.RequestID(), // Generate a request id on the HTTP response headers for identification
		middleware.CORS(),
	)

	e.HidePort = true
	e.HideBanner = true

	// Prometheus HTTP handler
	p := prom.NewPrometheus("echo", nil)
	p.Use(e)

	e.Use(middleware.StaticWithConfig(middleware.StaticConfig{
		Root:       "/",
		Browse:     false,
		HTML5:      true,
		Filesystem: getFileSystem(),
	}))

	g := e.Group("/api")
	api.RegisterApiHandlers(g, Version, GitCommit, sbarStore, eventStore, eventSampler)

	// start the server
	go func() {
		log.Infof("Started listener on http://%s", cfg.Listener)
		if err := e.Start(cfg.Listener); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	// wait here before closing all workers
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-termChan // Blocks here until interrupted
	log.Info("SIGTERM received. Shutdown initiated")

	ctxTimeout, cancelFunc2 := context.WithTimeout(ctx, time.Duration(time.Second*15))
	if err := e.Shutdown(ctxTimeout); err != nil {
		log.Error(err)
	}

	cancelFunc2()
	cancelFunc()

	// block here until are workers are done
	wg.Wait()
	eventSampler.Close()
	sbarStore.Close()
	eventStore.Close()
	kvStore.Close()
	store.Close()
	log.Info("all workers finished.. Shutting down!")
}

func getFileSystem() http.FileSystem {
	fsys, err := fs.Sub(embededFiles, "ui/dist")
	if err != nil {
		panic(err)
	}

	return http.FS(fsys)
}
