package main

import (
	"github.com/gin-gonic/gin"
	"github.com/pbudner/argosminer-collector/pkg/algorithms"
	"github.com/pbudner/argosminer-collector/pkg/config"
	"github.com/pbudner/argosminer-collector/pkg/parsers"
	"github.com/pbudner/argosminer-collector/pkg/sources"
	"github.com/pbudner/argosminer-collector/pkg/stores"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

func init() {
	// configure logger
	log.SetLevel(log.InfoLevel)

	var cfg, err = config.NewConfig()

	if err != nil {
		log.Fatal(err)
	}

	//store := stores.NewMemoryStoreGenerator()
	//store := stores.NewInfluxStoreGenerator(influxServerURL, influxToken, influxBucket, influxOrg, redisOptions)
	store := stores.NewTstorageStoreGenerator()

	for _, source := range cfg.Sources {
		if source.FileConfig.Path != "" {
			log.Debugf("Starting a file source...")
			var parser parsers.Parser
			if source.CsvParser.Delimiter != "" {
				log.Debugf("Initializing a CSV parser..")
				parser = parsers.NewCsvParser(source.CsvParser)
			}

			receivers := make([]algorithms.StreamingAlgorithm, 1)
			receivers[0] = algorithms.NewDfgStreamingAlgorithm(store)
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
	// pprof.Register(r)
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "Hello, World!",
			"version": "v0.0.1"
		})
	})

	r.GET("/metrics", prometheusHandler())
	r.Run()
}
