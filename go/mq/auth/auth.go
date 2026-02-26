package auth

import (
	"github.com/SpectatorNan/messageQ/go/mq/errx"
	"github.com/SpectatorNan/messageQ/go/mq/respx"
	"strings"

	"github.com/SpectatorNan/messageQ/go/mq/broker"

	"github.com/gin-gonic/gin"
)

// AuthMiddleware validates normal API access key.
func AuthMiddleware(b *broker.Broker, adminAK string) gin.HandlerFunc {
	adminAK = strings.TrimSpace(adminAK)
	return func(c *gin.Context) {
		if adminAK == "" {
			respx.FailGin(c, errx.ErrMissingSetAdminKey)
			c.Abort()
			return
		}
		ak := strings.TrimSpace(c.GetHeader(AuthHeaderKey))
		if ak == "" {
			respx.FailGin(c, errx.ErrUnauthorized)
			c.Abort()
			return
		}
		if ak == adminAK || b.IsAKValid(ak) {
			c.Next()
			return
		}
		respx.FailGin(c, errx.ErrUnauthorized)
		c.Abort()
	}
}

// AdminAuthMiddleware validates admin access key for management endpoints.
func AdminAuthMiddleware(adminAK string) gin.HandlerFunc {
	adminAK = strings.TrimSpace(adminAK)
	return func(c *gin.Context) {
		if adminAK == "" {
			respx.FailGin(c, errx.ErrMissingSetAdminKey)
			c.Abort()
			return
		}
		ak := strings.TrimSpace(c.GetHeader(AdminHeaderKey))
		if ak == "" || ak != adminAK {
			respx.FailGin(c, errx.ErrUnauthorized)
			c.Abort()
			return
		}
		c.Next()
	}
}
