package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
)

func main() {
	// enable WAL-based persistence under ./data
	store := storage.NewWALStorage("./data")
	defer func() { _ = store.Close() }()
	b := broker.NewBrokerWithStorage(store, 4)

	r := api.NewRouter(b)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// start server
	go func() {
		fmt.Println("MQ listening on :8080 (gin + WAL)")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	// handle graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	// ensure WAL is closed (Flush + background goroutines stopped)
	if err := store.Close(); err != nil {
		log.Printf("wal close error: %v", err)
	}

	fmt.Println("server stopped")
}
