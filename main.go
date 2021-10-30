package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

//go:embed ui/dist
var embededFiles embed.FS

var processStartedGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "process_started",
	Help: "Starttime of the process as a unix timestamp.",
})

func init() {
	// configure logger
	log.SetLevel(log.InfoLevel)

	// register global prometheus metrics
	prometheus.MustRegister(processStartedGauge)
	processStartedGauge.SetToCurrentTime()
}

func main() {
	cfg, err := config.NewConfig()

	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	store := storage.NewDiskStorageGenerator()
	eventStore := stores.NewEventStore(store)
	defer eventStore.Close()
	kvStore := stores.NewKvStore(store)
	defer kvStore.Close()
	sbarStore, err := stores.NewSbarStore(store)
	if err != nil {
		log.Fatal(err)
	}
	defer sbarStore.Close()
	receiverList := []processors.StreamingProcessor{
		processors.NewEventProcessor(eventStore),
		processors.NewDfgStreamingAlgorithm(sbarStore),
	}
	for _, source := range cfg.Sources {
		if !source.Enabled {
			continue
		}

		// file Source
		// not functional right now
		/*if source.FileConfig != nil {
			log.Debugf("Starting a file source...")
			wg.Add(1)
			var parser parsers.Parser
			if source.CsvParser != nil {
				// parser = parsers.NewCsvParser(*source.CsvParser)
			}
			fs := sources.NewFileSource(source.FileConfig.Path, source.FileConfig.ReadFrom, parser, receiverList)
			go fs.Run(ctx, wg)
		}*/

		// kafka Source
		if source.KafkaConfig != nil {
			log.Debugf("Starting kafka source...")
			wg.Add(1)
			var parser parsers.Parser
			// not functional right now
			/*if source.CsvParser != nil {
				parser = parsers.NewCsvParser(*source.CsvParser)
			}*/

			if source.JsonParser != nil {
				parser = parsers.NewJsonParser(*source.JsonParser)
			}

			fs := sources.NewKafkaSource(*source.KafkaConfig, parser)
			for _, receiver := range receiverList {
				fs.AddReceiver(receiver)
			}

			go fs.Run(ctx, wg)
		}
	}

	e := echo.New()
	e.Use(
		middleware.Recover(),   // Recover from all panics to always have your server up
		middleware.Logger(),    // Log everything to stdout
		middleware.RequestID(), // Generate a request id on the HTTP response headers for identification
	)

	e.HidePort = true
	e.HideBanner = true

	// Prometheus HTTP handler
	p := prom.NewPrometheus("echo", nil)
	p.Use(e)

	// error handling
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		log.Error(err)
		code := http.StatusInternalServerError
		if he, ok := err.(*echo.HTTPError); ok {
			code = he.Code
		}
		c.JSON(code, api.JSON{
			"status_code": code,
			"message":     err.Error(),
		})
		// errorPage := fmt.Sprintf("%d.html", code)
		/*if err := c.File(errorPage); err != nil {
			c.Logger().Error(err)
		}*/
		// c.Logger().Error(err)
	}

	useOS := len(os.Args) > 1 && os.Args[1] == "live"
	assetHandler := http.FileServer(getFileSystem(useOS))
	e.GET("/", echo.WrapHandler(assetHandler))

	g := e.Group("/api")
	g.GET("/", func(c echo.Context) error {
		return c.JSON(http.StatusOK, api.JSON{
			"message": "Hello, world! Welcome to ArgosMiner!",
			"version": "0.1",
		})
	})

	g.GET("/events/last/:count", func(c echo.Context) error {
		counter := 10
		i, err := strconv.Atoi(c.Param("count"))
		if err == nil {
			if i < 0 {
				counter = 10
			} else if i > 50 {
				counter = 50
			} else {
				counter = i
			}
		}

		events, err := eventStore.GetLast(counter)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

		return c.JSON(http.StatusOK, api.JSON{
			"events": events,
			"count":  len(events),
		})
	})

	g.GET("/events/frequency", func(c echo.Context) error {
		counter, err := eventStore.GetBinCount()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

		dailyCounter := make(map[string]uint64)
		for k, v := range counter {
			trimmedKey := fmt.Sprintf("%s-%s-%s", k[0:4], k[4:6], k[6:8])
			_, ok := dailyCounter[trimmedKey]
			if !ok {
				dailyCounter[trimmedKey] = v
			} else {
				dailyCounter[trimmedKey] += v
			}
		}

		return c.JSON(http.StatusOK, api.JSON{
			"frequency": dailyCounter,
		})
	})

	g.GET("/events/statistics", func(c echo.Context) error {
		counter := eventStore.GetCount()
		activityCount := sbarStore.CountActivities()
		dfRelationCount := sbarStore.CountDfRelations()
		return c.JSON(http.StatusOK, api.JSON{
			"event_count":       counter,
			"activity_count":    activityCount,
			"df_relation_count": dfRelationCount,
		})
	})

	g.GET("/events/activities", func(c echo.Context) error {
		v := sbarStore.GetActivities()
		return c.JSON(http.StatusOK, api.JSON{
			"activities": v,
			"count":      len(v),
		})
	})

	g.GET("/events/dfrelations", func(c echo.Context) error {
		v := sbarStore.GetDfRelations()
		return c.JSON(200, api.JSON{
			"dfrelations": v,
			"count":       len(v),
		})
	})

	go func() {
		if err := e.Start(":3000"); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	// wait here before closing all workers
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-termChan // Blocks here until interrupted
	log.Info("SIGTERM received. Shutdown initiated\n")
	ctxTimeout, cancelFunc2 := context.WithTimeout(ctx, time.Duration(time.Second*15))
	if err := e.Shutdown(ctxTimeout); err != nil {
		log.Error(err)
	}

	cancelFunc2()
	cancelFunc()

	// block here until are workers are done
	wg.Wait()
	log.Info("All workers finished.. Shutting down!")
}

func getFileSystem(useOS bool) http.FileSystem {
	if useOS {
		log.Print("using live mode")
		return http.FS(os.DirFS("ui/dist"))
	}

	log.Print("using embed mode")
	fsys, err := fs.Sub(embededFiles, "ui/dist")
	if err != nil {
		panic(err)
	}

	return http.FS(fsys)
}
