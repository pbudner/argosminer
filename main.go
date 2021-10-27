package main

import (
	"context"
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
	kvStore := stores.NewKvStore(store)
	sbarStore := stores.NewSbarStore(store)
	receiverList := []processors.StreamingProcessor{
		processors.NewEventProcessor(eventStore, kvStore),
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
			continue
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
	e.Use(middleware.Logger())
	p := prom.NewPrometheus("echo", nil)
	p.Use(e)

	e.GET("/", func(c echo.Context) error {
		return c.JSON(http.StatusOK, api.JSON{
			"message": "Hello, world! Welcome to ArgosMiner!",
			"version": "0.1",
		})
	})

	e.GET("/events/last/:count", func(c echo.Context) error {
		counter := 10
		i, err := strconv.Atoi(c.Param("count")[1:])
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

	e.GET("/events/frequency", func(c echo.Context) error {
		counter, err := eventStore.CountByDay()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

		return c.JSON(http.StatusOK, api.JSON{
			"frequency": counter,
		})
	})

	e.GET("/events/statistics", func(c echo.Context) error {
		counter, err := kvStore.Get([]byte("EventStoreCounter"))
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

		activityCount, err := sbarStore.CountActivities()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

		dfRelationCount, err := sbarStore.CountDfRelations()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

		return c.JSON(http.StatusOK, api.JSON{
			"event_count":       storage.BytesToUint64(counter),
			"activity_count":    activityCount,
			"df_relation_count": dfRelationCount,
		})
	})

	e.GET("/events/activities", func(c echo.Context) error {
		v, err := sbarStore.GetActivities()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

		return c.JSON(http.StatusOK, api.JSON{
			"activities": v,
			"count":      len(v),
		})
	})

	e.GET("/events/dfrelations", func(c echo.Context) error {
		v, err := sbarStore.GetDfRelations()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, api.JSON{
				"error": err.Error(),
			})
		}

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
