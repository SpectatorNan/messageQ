package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/logger"
	"messageQ/mq/storage"
)

func main() {
	// Initialize logger
	if err := logger.InitDefault(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting MessageQ server")

	// enable WAL-based persistence under ./data
	store := storage.NewWALStorage("./data")
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("Failed to close storage", zap.Error(err))
		}
	}()
	
	b := broker.NewBrokerWithStorage(store, 4)
	logger.Info("Broker initialized", zap.Int("queue_count", 4))

	r := api.NewRouter(b)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// start server
	go func() {
		logger.Info("HTTP server listening", zap.String("addr", ":8080"))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Server failed to start", zap.Error(err))
		}
	}()

	// handle graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Shutdown signal received, shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown error", zap.Error(err))
	}

	// ensure WAL is closed (Flush + background goroutines stopped)
	if err := store.Close(); err != nil {
		logger.Error("WAL close error", zap.Error(err))
	}

	logger.Info("Server gracefully stopped")
}
