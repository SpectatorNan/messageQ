package api

import (
	"messageQ/mq/respx"
	"net/http"
	"time"

	"messageQ/mq/broker"

	"github.com/gin-gonic/gin"
)

// DelayStatsHandler returns delay scheduler statistics
func DelayStatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := b.Stats()
		result := StatsResponse{
			Groups:    make(map[string]interface{}),
			Timestamp: time.Now(),
		}

		// Convert groupStats to interface{}
		for k, v := range stats {
			result.Groups[k] = v
		}

		// Add delay scheduler stats if available
		if ds := b.GetDelayScheduler(); ds != nil {
			delayStats := ds.Stats()
			result.DelayScheduler = delayStats
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(result))
	}
}
