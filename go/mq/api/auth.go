package api

import (
	"strings"

	"messageQ/mq/broker"

	"github.com/gin-gonic/gin"
)

const (
	headerAK      = "X-AK"
	headerAdminAK = "X-Admin-AK"
)

// AuthMiddleware validates normal API access key.
func AuthMiddleware(b *broker.Broker, adminAK string) gin.HandlerFunc {
	adminAK = strings.TrimSpace(adminAK)
	return func(c *gin.Context) {
		if adminAK == "" {
			FailGin(c, ErrMissingSetAdminKey)
			return
		}
		ak := strings.TrimSpace(c.GetHeader(headerAK))
		if ak == "" {
			FailGin(c, ErrUnauthorized)
			return
		}
		if ak == adminAK || b.IsAKValid(ak) {
			c.Next()
			return
		}
		FailGin(c, ErrUnauthorized)
	}
}

// AdminAuthMiddleware validates admin access key for management endpoints.
func AdminAuthMiddleware(adminAK string) gin.HandlerFunc {
	adminAK = strings.TrimSpace(adminAK)
	return func(c *gin.Context) {
		if adminAK == "" {
			FailGin(c, ErrMissingSetAdminKey)
			return
		}
		ak := strings.TrimSpace(c.GetHeader(headerAdminAK))
		if ak == "" || ak != adminAK {
			FailGin(c, ErrUnauthorized)
			return
		}
		c.Next()
	}
}
