package api

import (
	"github.com/SpectatorNan/messageQ/go/mq/errx"
	"github.com/SpectatorNan/messageQ/go/mq/respx"
	"net/http"
	"time"

	"github.com/SpectatorNan/messageQ/go/mq/broker"

	"github.com/gin-gonic/gin"
)

// DelayStatsHandler returns broker-wide statistics.
func DelayStatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		bs := b.FullStats()

		topics := make([]TopicStatsResponse, 0, len(bs.Topics))
		for _, ts := range bs.Topics {
			topics = append(topics, TopicStatsResponse{
				Name:       ts.Name,
				Type:       ts.Type,
				QueueCount: ts.QueueCount,
				Total:      ts.Total,
				CreatedAt:  ts.CreatedAt,
			})
		}

		groups := make([]ConsumerGroupStatsResponse, 0, len(bs.ConsumerGroups))
		for _, cgs := range bs.ConsumerGroups {
			topicStats := make([]ConsumerGroupTopicStatsResponse, 0, len(cgs.Topics))
			for _, cts := range cgs.Topics {
				topicStats = append(topicStats, ConsumerGroupTopicStatsResponse{
					Topic:      cts.Topic,
					Total:      cts.Total,
					Completed:  cts.Completed,
					Processing: cts.Processing,
					Pending:    cts.Pending,
				})
			}
			groups = append(groups, ConsumerGroupStatsResponse{
				Group:      cgs.Group,
				Total:      cgs.Total,
				Completed:  cgs.Completed,
				Processing: cgs.Processing,
				Pending:    cgs.Pending,
				Topics:     topicStats,
			})
		}

		result := StatsResponse{
			Topics:         topics,
			ConsumerGroups: groups,
			DelayScheduler: bs.DelayScheduler,
			Total:          bs.Total,
			Completed:      bs.Completed,
			Processing:     bs.Processing,
			Pending:        bs.Pending,
			Timestamp:      time.Now(),
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(result))
	}
}

// TopicStatsHandler returns statistics for a specific topic.
func TopicStatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}

		ts, consumers, err := b.TopicFullStats(topic)
		if err != nil {
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}

		csResp := make([]TopicConsumerStatsResponse, 0, len(consumers))
		for _, cs := range consumers {
			csResp = append(csResp, TopicConsumerStatsResponse{
				Group:      cs.Topic, // Topic field holds group name in per-topic view
				Completed:  cs.Completed,
				Processing: cs.Processing,
				Pending:    cs.Pending,
			})
		}

		resp := TopicDetailStatsResponse{
			Name:       ts.Name,
			Type:       ts.Type,
			QueueCount: ts.QueueCount,
			Total:      ts.Total,
			Consumers:  csResp,
			CreatedAt:  ts.CreatedAt,
			Timestamp:  time.Now(),
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}
