package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/config"
	"messageQ/mq/logger"
	"messageQ/mq/storage"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger with config
	loggerCfg := logger.Config{
		Level:      logger.LogLevel(cfg.Logging.Level),
		OutputPath: cfg.Logging.OutputPath,
		Format:     cfg.Logging.Format,
	}
	if err := logger.Init(loggerCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting MessageQ server",
		zap.Int("queue_count", cfg.Broker.QueueCount),
		zap.String("data_dir", cfg.Broker.DataDir),
		zap.String("log_level", cfg.Logging.Level))

	// Enable WAL-based persistence with config
	store := storage.NewWALStorage(
		cfg.Broker.DataDir,
		cfg.Storage.FlushInterval,
		cfg.Storage.CompactInterval,
	)
	store.SetCompactThreshold(cfg.Storage.CompactThreshold)
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("Failed to close storage", zap.Error(err))
		}
	}()

	b := broker.NewBrokerWithStorage(store, cfg.Broker.QueueCount)
	b.SetMaxRetry(cfg.Broker.MaxRetry)
	b.SetProcessingTimeout(cfg.Broker.ProcessingTimeout)
	b.SetRetryBackoff(cfg.Broker.RetryBackoffBase, cfg.Broker.RetryBackoffMultiplier, cfg.Broker.RetryBackoffMax)
	logger.Info("Broker initialized",
		zap.Int("queue_count", cfg.Broker.QueueCount),
		zap.Int("max_retry", cfg.Broker.MaxRetry),
		zap.Duration("processing_timeout", cfg.Broker.ProcessingTimeout),
		zap.Duration("retry_backoff_base", cfg.Broker.RetryBackoffBase),
		zap.Float64("retry_backoff_multiplier", cfg.Broker.RetryBackoffMultiplier),
		zap.Duration("retry_backoff_max", cfg.Broker.RetryBackoffMax))

	r := api.NewRouter(b)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// Start server
	go func() {
		logger.Info("HTTP server listening", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("Server failed to start", zap.Error(err))
		}
	}()

	// Handle graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Shutdown signal received, shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown error", zap.Error(err))
	}

	// Ensure WAL is closed (Flush + background goroutines stopped)
	if err := store.Close(); err != nil {
		logger.Error("WAL close error", zap.Error(err))
	}

	logger.Info("Server gracefully stopped")
}
