package api

import (
	"net/http"
	"strings"

	"messageQ/mq/broker"

	"github.com/gin-gonic/gin"
)

type akRequest struct {
	AK string `json:"ak"`
}

// ListAKHandler lists all access keys.
func ListAKHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		aks := b.ListAKs()
		c.JSON(http.StatusOK, NewRespSuccess(gin.H{"aks": aks}))
	}
}

// AddAKHandler adds an access key.
func AddAKHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req akRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			FailGin(c, ErrInvalidMessage)
			return
		}
		req.AK = strings.TrimSpace(req.AK)
		if req.AK == "" {
			FailGin(c, ErrInvalidMessage)
			return
		}
		id, err := b.AddAK(req.AK)
		if err != nil {
			FailGin(c, ErrInvalidMessage)
			return
		}
		c.JSON(http.StatusOK, NewRespSuccess(gin.H{"id": id}))
	}
}

// DeleteAKHandler removes an access key.
func DeleteAKHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := strings.TrimSpace(c.Param("id"))
		if id == "" {
			FailGin(c, ErrInvalidMessage)
			return
		}
		if err := b.RemoveAK(id); err != nil {
			FailGin(c, ErrInvalidMessage)
			return
		}
		c.JSON(http.StatusOK, NewRespSuccess(gin.H{"id": id}))
	}
}
