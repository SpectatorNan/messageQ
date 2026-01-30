package api

// handlers.go (deprecated)
// Net/http-style handlers (ProduceHandler, ConsumeHandler) were moved to Gin-based
// handlers in mq/api/gin_handlers.go (ProduceGin, ConsumeGin, AckGin, NackGin).
// Remove or update any remaining imports to use the Gin-based router via api.NewRouter().
