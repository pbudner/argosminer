package main

import (
	"bytes"
	"context"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
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
	err = os.MkdirAll(cfg.Database.Path, os.ModePerm)
	if err != nil {
		log.Panicw("Could not ensure data path exists", "path", cfg.Database.Path, "error", err)
	}

	// check data path exists
	if _, err := os.Stat(cfg.Database.Path); os.IsNotExist(err) {
		log.Panicw("Could not open database as data path does not exist", "path", cfg.Database.Path, "error", err)
	}

	// open storage
	store := storage.NewDiskStorage(cfg.Database)

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
			for _, parser := range source.CsvParsers {
				parserSlice = append(parserSlice, parsers.NewCsvParser(*parser))
			}
			for _, parser := range source.JsonParsers {
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
			for _, parser := range source.CsvParsers {
				parserSlice = append(parserSlice, parsers.NewCsvParser(*parser))
			}
			for _, parser := range source.JsonParsers {
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

	e.HTTPErrorHandler = func(err error, c echo.Context) {
		log.Info("IM IN")
		code := http.StatusInternalServerError
		if he, ok := err.(*echo.HTTPError); ok {
			code = he.Code
		}
		log.Info(err)
		c.JSON(code, api.JSON{
			"status_code": code,
			"message":     "Sorry, we could not find the requested URL.",
		})
	}

	// Prometheus HTTP handler
	p := prom.NewPrometheus("echo", nil)
	p.Use(e)

	// parse BaseURL
	baseURL, err := url.Parse(cfg.BaseURL)
	if err != nil {
		log.Fatal(err)
	}

	baseGroup := e.Group(baseURL.RequestURI())
	handleIndexFunc := func(c echo.Context) error {
		response, err := parseTemplate("ui/dist/index.html", cfg)
		if err != nil {
			log.Error(err)
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"message": "Could not parse index.html template.",
			})
		}
		return c.HTML(http.StatusOK, response)
	}

	fsys, err := fs.Sub(embededFiles, "ui/dist")
	if err != nil {
		panic(err)
	}

	assetHandler := func(root http.FileSystem) echo.HandlerFunc {
		return func(c echo.Context) error {
			fileServer := http.FileServer(root)
			path := strings.TrimPrefix(path.Clean(c.Request().URL.Path), baseURL.RequestURI())
			_, err := root.Open(path) // Do not allow path traversals.
			if os.IsNotExist(err) {
				return handleIndexFunc(c)
			}

			fsPath, err := url.Parse(path)
			if err != nil {
				return c.JSON(http.StatusInternalServerError, api.JSON{
					"message": "Could not serve static asset.",
				})
			}

			c.Request().URL = fsPath
			fileServer.ServeHTTP(c.Response(), c.Request())
			return nil
		}
	}

	baseGroup.GET("/*", assetHandler(http.FS(fsys)))
	baseGroup.GET("/index.html", handleIndexFunc)
	baseGroup.GET("/", handleIndexFunc)
	baseGroup.GET("/argos_config.js", func(c echo.Context) error {
		return c.Blob(http.StatusOK, echo.MIMEApplicationJavaScriptCharsetUTF8, []byte(fmt.Sprintf("var baseURL = '%s';", baseURL.RequestURI())))
	})

	g := baseGroup.Group("/api")
	api.RegisterApiHandlers(g, cfg, Version, GitCommit, sbarStore, eventStore, eventSampler)

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

func parseTemplate(templateFileName string, data interface{}) (string, error) {
	templateName := filepath.Base(templateFileName)
	t, err := template.New(templateName).Funcs(template.FuncMap{
		"replaceNewline": func(s string) template.HTML {
			return template.HTML(strings.Replace(template.HTMLEscapeString(s), "\n", "<br>", -1))
		},
	}).ParseFS(embededFiles, templateFileName)
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if err = t.Execute(buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
