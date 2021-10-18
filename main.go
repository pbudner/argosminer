package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pbudner/argosminer-collector/algorithms"
	"github.com/pbudner/argosminer-collector/config"
	"github.com/pbudner/argosminer-collector/parsers"
	"github.com/pbudner/argosminer-collector/sources"
	"github.com/pbudner/argosminer-collector/stores"
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

	//store := stores.NewMemoryStoreGenerator()
	//store := stores.NewInfluxStoreGenerator(influxServerURL, influxToken, influxBucket, influxOrg, redisOptions)
	store := stores.NewTstorageStoreGenerator()

	for _, source := range cfg.Sources {
		if source.FileConfig.Path != "" { // File Source
			log.Debugf("Starting a file source...")
			wg.Add(1)
			var parser parsers.Parser
			if source.CsvParser.Delimiter != "" {
				log.Debugf("Initializing a CSV parser..")
				parser = parsers.NewCsvParser(source.CsvParser)
			}

			receivers := make([]algorithms.StreamingAlgorithm, 1)
			receivers[0] = algorithms.NewDfgStreamingAlgorithm(store)
			fs := sources.NewFileSource(source.FileConfig.Path, source.FileConfig.ReadFrom, parser, receivers)
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
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan // Blocks here until interrupted
	log.Info("SIGTERM received. Shutdown initiated\n")
	ctxTimeout, cancelFunc2 := context.WithTimeout(ctx, time.Duration(time.Second*15))
	if err := srv.Shutdown(ctxTimeout); err != nil {
		log.Error(err)
	}

	cancelFunc2()
	cancelFunc()

	// Block here until are workers are done
	wg.Wait()
	log.Info("All workers finished.. Shutting down!")
}
