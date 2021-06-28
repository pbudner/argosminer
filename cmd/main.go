// ArgosMiner Entrypoint

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	api "github.com/influxdata/influxdb-client-go/v2/api"
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

func init() {
	prometheus.MustRegister(receivedEvents)
}

func prometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()

	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}

func readFile(file string, influxWriteAPI api.WriteAPI, cache *redis.Client) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		log.Debug(strings.Split(line, ";")[0])
		i++
		if i == 9 {
			break
		}
	}
	log.Debugf("Done!")
}

func main() {
	// configure logger
	log.SetLevel(log.DebugLevel)

	// connect to databases
	mypath := path.Join("/Volumes", "Pascals Drive", "Argos Miner", "TrackingDB_Part2.csv")
	influx := influxdb2.NewClientWithOptions("http://localhost:8086", "-EuU9TmvtAr27Cr_3bJvakriAVr7RNS04TsF_xD35-1XWZpog7Iz9dubOQMsCX9NUpRskLlHZSnhEZKvwineog==", influxdb2.DefaultOptions())
	writeAPI := influx.WriteAPI("ciis", "go_test")
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	go readFile(mypath, writeAPI, rdb)

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
