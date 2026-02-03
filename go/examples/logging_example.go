package main

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"messageQ/mq/logger"
)

func main() {
	// Example 1: Default configuration (INFO level, console format, stdout)
	fmt.Println("=== Example 1: Default Logger ===")
	if err := logger.InitDefault(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	
	logger.Info("This is an info message")
	logger.Debug("This debug message won't appear (level is INFO)")
	logger.Warn("This is a warning message")
	logger.Error("This is an error message")
	
	logger.Sync()
	fmt.Println()

	// Example 2: Debug level with console format
	fmt.Println("=== Example 2: Debug Level ===")
	cfg := logger.Config{
		Level:      logger.DebugLevel,
		OutputPath: "stdout",
		Format:     "console",
	}
	if err := logger.Init(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	
	logger.Debug("Now debug messages appear!")
	logger.Info("Info message with debug level")
	logger.Sync()
	fmt.Println()

	// Example 3: JSON format for production
	fmt.Println("=== Example 3: JSON Format ===")
	cfg = logger.Config{
		Level:      logger.InfoLevel,
		OutputPath: "stdout",
		Format:     "json",
	}
	if err := logger.Init(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	
	logger.Info("JSON formatted message")
	logger.Error("Error in JSON format")
	logger.Sync()
	fmt.Println()

	// Example 4: Logging to file
	fmt.Println("=== Example 4: File Output ===")
	logFile := "/tmp/messageq-example.log"
	cfg = logger.Config{
		Level:      logger.DebugLevel,
		OutputPath: logFile,
		Format:     "json",
	}
	if err := logger.Init(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	
	logger.Info("This goes to file")
	logger.Debug("Debug message to file")
	logger.Warn("Warning to file")
	logger.Sync()
	
	// Read and display file content
	content, err := os.ReadFile(logFile)
	if err != nil {
		fmt.Printf("Failed to read log file: %v\n", err)
	} else {
		fmt.Printf("Log file content (%s):\n%s\n", logFile, string(content))
	}
	
	// Cleanup
	os.Remove(logFile)
	
	// Example 5: Structured logging with fields
	fmt.Println("=== Example 5: Structured Logging ===")
	cfg = logger.Config{
		Level:      logger.InfoLevel,
		OutputPath: "stdout",
		Format:     "console",
	}
	if err := logger.Init(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	
	logger.Info("User logged in",
		zap.String("user_id", "user123"),
		zap.String("ip", "192.168.1.1"),
		zap.Int("port", 8080))
	
	logger.Warn("Rate limit exceeded",
		zap.String("client_id", "client456"),
		zap.Int("requests", 1000),
		zap.Int("limit", 500))
	
	logger.Error("Database connection failed",
		zap.String("host", "db.example.com"),
		zap.Int("port", 5432),
		zap.Duration("timeout", 5*time.Second))
	
	logger.Sync()
}
