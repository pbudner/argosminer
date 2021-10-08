package main

import (
	"fmt"
	"log"

	"github.com/nakabonne/tstorage"
)

func test() {
	fmt.Printf("Closing this up here..")
}

func main2() {
	defer test()

	storage, _ := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithDataPath("./data"),
	)
	defer storage.Close()

	points, err := storage.Select("activity,name=Anhang\\ archiviert", nil, 1608199042, 1608202642)
	if err != nil {
		log.Print(err)
	}

	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
	}
}
