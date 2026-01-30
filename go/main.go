package main

import (
	"fmt"
	"log"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
)

func main() {
	// enable WAL-based persistence under ./data
	store := storage.NewWALStorage("./data")
	defer store.Close()
	b := broker.NewBrokerWithStorage(store)

	r := api.NewRouter(b)

	fmt.Println("MQ listening on :8080 (gin + WAL)")
	log.Fatal(r.Run(":8080"))
}
