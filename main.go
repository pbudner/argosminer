package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	prom "github.com/labstack/echo-contrib/prometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
	"github.com/pbudner/argosminer/api"
	"github.com/pbudner/argosminer/config"
	"github.com/pbudner/argosminer/pipeline"
	_ "github.com/pbudner/argosminer/pipeline/sinks"
	_ "github.com/pbudner/argosminer/pipeline/sources"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/ui"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.uber.org/zap"
)

var (
	GitCommit = "live"
	Version   = ""
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
		cfg             *config.Config
		err             error
		wg              = &sync.WaitGroup{}
		ctx, cancelFunc = context.WithCancel(context.Background())
	)

	if _, err := os.Stat("./config.yaml"); configPath == "" && err == nil {
		configPath = "config.yaml"
	}

	if configPath == "" {
		cfg = config.DefaultConfig()
	} else {
		cfg, err = config.NewConfigFromFile(configPath)
		if err != nil {
			log.Fatalw("unexpected error during unmarshalling provided log", "error", err, "path", configPath)
		}
	}

	logger, _ = cfg.Logger.Build()
	log = logger.Sugar()
	undo := zap.ReplaceGlobals(logger)
	defer undo()

	log.Infow("Starting ArgosMiner", "version", Version, "commit", GitCommit)

	// ensure data path exists
	err = os.MkdirAll(cfg.Database.Path, os.ModePerm)
	if err != nil {
		log.Panicw("Could not ensure data path exists", "path", cfg.Database.Path, "error", err)
	}

	// check data path exists
	if _, err := os.Stat(cfg.Database.Path); os.IsNotExist(err) {
		log.Panicw("Could not open database as data path does not exist", "path", cfg.Database.Path, "error", err)
	}

	// open default storage
	storage.DefaultStorage = storage.NewDiskStorage(cfg.Database)

	// instantiate all configured pipeline components
	instantiateComponents(nil, cfg.Pipeline, wg, ctx)

	// now, configure webserver
	e := echo.New()
	e.Use(
		middleware.Recover(),   // Recover from all panics to always have your server up
		middleware.RequestID(), // Generate a request id on the HTTP response headers for identification
		middleware.CORS(),
	)

	e.HidePort = true
	e.HideBanner = true

	e.HTTPErrorHandler = func(err error, c echo.Context) {
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

	// parse BaseURL
	baseURL, err := url.Parse(cfg.BaseURL)
	if err != nil {
		log.Fatal(err)
	}

	baseUrlPath := strings.TrimRight(baseURL.RequestURI(), "/")
	baseGroup := e.Group(baseUrlPath)

	// Prometheus HTTP handler
	p := prom.NewPrometheus("echo", nil)
	if baseUrlPath == "/" {
		p.MetricsPath = "/metrics"
	} else {
		p.MetricsPath = fmt.Sprintf("%s/metrics", baseUrlPath)
	}
	p.Use(e)

	baseGroup.GET("/*", ui.GetStaticHandler(baseURL, cfg))
	baseGroup.GET("/index.html", ui.GetIndexHandler(cfg))
	baseGroup.GET("/", ui.GetIndexHandler(cfg))

	g := baseGroup.Group("/api")
	api.RegisterApiHandlers(g, cfg, Version, GitCommit)

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
	log.Info("SIGTERM received, initiating shutdown now")
	log.Info("[1/3] Shutting down webserver..")
	ctxTimeout, cancelFunc2 := context.WithTimeout(context.Background(), time.Duration(time.Second*15))
	if err := e.Shutdown(ctxTimeout); err != nil {
		log.Error(err)
	}
	log.Info("[2/3] Closing all pipeline components - this might take some time as we are straighten things up (i.e., flushing buffers, canceling jobs, ...)")
	cancelFunc()  // this closes the context for all pipeline components
	wg.Wait()     // block here until are workers are done
	cancelFunc2() // this stops the server if the graceful shutdown was not successful
	log.Info("[3/3] Finally, closing stores")
	stores.GetEventSampler().Close()
	stores.GetSbarStore().Close()
	stores.GetEventStore().Close()
	stores.GetKvStore().Close()
	storage.DefaultStorage.Close()
	log.Info("Graceful shutdown completed")
}

func instantiateComponents(parent pipeline.Component, components []config.Component, wg *sync.WaitGroup, ctx context.Context) {
	for _, component := range components {
		if component.Disabled {
			log.Infof("Ignoring component %s as it is disabled in the config", component.Name)
			continue
		}
		instantiation, err := pipeline.InstantiateComponent(component.Name, component.Config)
		if err != nil {
			log.Errorf("An unexpected error occured during instantiating component '%s': %s", component.Name, err)
			return
		}
		if parent != nil {
			instantiation.Link(parent.Subscribe())
		}

		childContext, cancelChilds := context.WithCancel(context.Background())
		instantiateComponents(instantiation, component.Connects, wg, childContext)

		wg.Add(1)
		go func() {
			instantiation.Run(wg, ctx)
			cancelChilds() // when parent finishes, close the child components
		}()
	}
}
