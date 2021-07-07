package main

import (
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/pbudner/argosminer-collector/pkg/algorithms"
	"github.com/pbudner/argosminer-collector/pkg/config"
	"github.com/pbudner/argosminer-collector/pkg/parsers"
	"github.com/pbudner/argosminer-collector/pkg/sources"
	"github.com/pbudner/argosminer-collector/pkg/stores"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var receivedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer",
	Name:      "received_events",
	Help:      "Total number of received events.",
})

func init() {
	// configure logger
	log.SetLevel(log.DebugLevel)

	prometheus.MustRegister(receivedEvents)

	var cfg, err = config.NewConfig()

	if err != nil {
		log.Fatal(err)
	}

	redisOptions := redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	}

	influxServerURL := "http://localhost:8086"
	influxToken := "-EuU9TmvtAr27Cr_3bJvakriAVr7RNS04TsF_xD35-1XWZpog7Iz9dubOQMsCX9NUpRskLlHZSnhEZKvwineog=="
	influxOrg := "ciis"
	influxBucket := "new_process_mining"

	for _, source := range cfg.Sources {
		if source.FileConfig.Path != "" {
			log.Debugf("Starting a file source...")
			var parser parsers.Parser
			if source.CsvParser.Delimiter != "" {
				log.Debugf("Initializing a CSV parser..")
				parser = parsers.NewCsvParser(source.CsvParser)
			}

			receivers := make([]algorithms.StreamingAlgorithm, 1)
			receivers[0] = algorithms.NewDfgStreamingAlgorithm(stores.NewInfluxStoreGenerator(influxServerURL, influxToken, influxBucket, influxOrg, redisOptions))
			fs := sources.NewFileSource(source.FileConfig.Path, source.FileConfig.ReadFrom, parser, receivers)
			go fs.Run()
		}
	}
}

func prometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()

	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}

func main() {
	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		receivedEvents.Inc()
		c.JSON(200, gin.H{
			"message": "Hello, World!",
		})
	})

	r.GET("/metrics", prometheusHandler())
	r.Run()
}
