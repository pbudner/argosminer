package main

import (
	"fmt"
	"log"
	"time"

	"github.com/boltdb/bolt"
)

func main2() {
	db, err := bolt.Open("collector.db", 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
		return
	}

	// init database
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		answer := b.Get([]byte("answer"))
		log.Printf("The answer to everthing is %s", answer)
		return nil
	})

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		err = b.Put([]byte("answer"), []byte("42"))
		return err
	})

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
}
