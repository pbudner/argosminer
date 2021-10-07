package main

import (
	"fmt"
	"log"

	"github.com/nakabonne/tstorage"
)

func main() {
	storage, _ := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithDataPath("./data"),
	)

	points, err := storage.Select("activity,name=Anhang\\ archiviert", nil, 1608199042, 1608202642)
	if err != nil {
		log.Fatal(err)
	}

	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
	}

	defer storage.Close()
}
