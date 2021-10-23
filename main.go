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

	"github.com/gin-gonic/gin"
	"github.com/pbudner/argosminer/config"
	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/receivers"
	"github.com/pbudner/argosminer/sources"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/stores/backends"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

func prometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()

	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}

func main() {
	cfg, err := config.NewConfig()

	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	store := backends.NewDiskStoreGenerator()
	eventStore := stores.NewEventStore(store)
	eventStoreReceiver := receivers.NewEventStoreReceiver(eventStore)
	receiverList := []receivers.StreamingReceiver{
		//receivers.NewDfgStreamingAlgorithm(store),
	}
	for _, source := range cfg.Sources {
		if !source.Enabled {
			continue
		}

		// file Source
		if source.FileConfig != nil {
			log.Debugf("Starting a file source...")
			wg.Add(1)
			var parser parsers.Parser
			if source.CsvParser.Delimiter != "" {
				log.Debugf("Initializing a CSV parser..")
				// parser = parsers.NewCsvParser(*source.CsvParser)
			}
			fs := sources.NewFileSource(source.FileConfig.Path, source.FileConfig.ReadFrom, parser, receiverList)
			go fs.Run(ctx, wg)
		}

		// kafka Source
		if source.KafkaConfig != nil {
			log.Debugf("Starting a kafka source...")
			wg.Add(1)
			var parser parsers.Parser
			if source.CsvParser != nil {
				// parser = parsers.NewCsvParser(*source.CsvParser)
			}

			if source.JsonParser != nil {
				parser = parsers.NewJsonParser(*source.JsonParser)
			}

			fs := sources.NewKafkaSource(*source.KafkaConfig, parser)
			fs.AddReceiver(eventStoreReceiver)
			for _, receiver := range receiverList {
				fs.AddReceiver(receiver)
			}

			go fs.Run(ctx, wg)
		}
	}

	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "Hello, world!",
			"version": "0.1",
		})
	})

	r.GET("/events/last/*count", func(c *gin.Context) {
		counter := 10
		i, err := strconv.Atoi(c.Param("count")[1:])
		if err == nil && i > 0 && i < 50 {
			counter = i
		}

		events, err := eventStore.GetLast(counter)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(200, gin.H{
			"events": events,
		})
	})

	r.GET("/metrics", prometheusHandler())

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// wait here before closing all workers
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan // Blocks here until interrupted
	log.Info("SIGTERM received. Shutdown initiated\n")
	ctxTimeout, cancelFunc2 := context.WithTimeout(ctx, time.Duration(time.Second*15))
	if err := srv.Shutdown(ctxTimeout); err != nil {
		log.Error(err)
	}

	cancelFunc2()
	cancelFunc()

	// block here until are workers are done
	wg.Wait()
	log.Info("All workers finished.. Shutting down!")
}
