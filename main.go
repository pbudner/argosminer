// ArgosMiner Entrypoint

package main

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/pbudner/argosminer-collector/config"
	"github.com/pbudner/argosminer-collector/pkg/algorithms"
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

var ctx = context.Background()
var rdb *redis.Client

func init() {
	// configure logger
	log.SetLevel(log.DebugLevel)

	prometheus.MustRegister(receivedEvents)

	var cfg, err = config.NewConfig()

	if err != nil {
		log.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	for _, source := range cfg.Sources {
		if source.FileConfig.Path != "" {
			log.Debugf("Starting a file source...")
			var parser parsers.Parser
			if source.CsvParser.Delimiter != "" {
				log.Debugf("Initializing a CSV parser..")
				parser = parsers.NewCsvParser(source.CsvParser)
			}

			receivers := make([]algorithms.StreamingAlgorithm, 1)
			receivers[0] = algorithms.NewDfgStreamingAlgorithm(stores.NewRedisStore(rdb))

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

	// connect to databases
	influx := influxdb2.NewClientWithOptions("http://localhost:8086", "-EuU9TmvtAr27Cr_3bJvakriAVr7RNS04TsF_xD35-1XWZpog7Iz9dubOQMsCX9NUpRskLlHZSnhEZKvwineog==", influxdb2.DefaultOptions())
	writeAPI := influx.WriteAPI("ciis", "go_test")

	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		receivedEvents.Inc()
		counter, err := rdb.Incr(ctx, "exemplary_dfg").Result()
		line := fmt.Sprintf("my_counter,app=go counter=%d", counter)
		writeAPI.WriteRecord(line)
		if err != nil {
			log.Panic(err)
		}
		c.JSON(200, gin.H{
			"message": counter,
		})
	})

	r.GET("/counter", func(c *gin.Context) {
		receivedEvents.Inc()
		counter, err := rdb.Get(ctx, "exemplary_dfg").Int64()
		if err != nil {
			log.Panic(err)
		}
		c.JSON(200, gin.H{
			"counter": counter,
		})
	})

	r.GET("/metrics", prometheusHandler())
	r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}
