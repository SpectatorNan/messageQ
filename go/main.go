package main

import (
	"fmt"
	"log"
	"net/http"

	"messageQ/mq/api"
)

func main() {
	http.HandleFunc("/produce", api.ProduceHandler)
	http.HandleFunc("/consume", api.ConsumeHandler)

	fmt.Println("MQ listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
