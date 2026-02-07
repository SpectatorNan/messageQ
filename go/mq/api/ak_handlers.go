package api

import (
	"messageQ/mq/broker"
	"messageQ/mq/respx"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ListAKHandler lists all access keys.
func ListAKHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		aks := b.ListAKs()
		list := make([]AccessKey, 0, len(aks))
		for _, ak := range aks {
			list = append(list, AccessKey{
				ID:        ak.ID,
				Name:      ak.Name,
				CreatedAt: ak.CreatedAt,
			})
		}
		c.JSON(http.StatusOK, respx.NewRespList(list, len(list)))
	}
}

// AddAKHandler adds an access key.
func AddAKHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreateAccessKeyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		err := req.Validate()
		if err != nil {
			respx.FailGin(c, err)
			return
		}

		ak, err := b.AddAK(req.Name, req.AccessKey)
		if err != nil {
			respx.FailGin(c, err)
			return
		}

		data := CreateAccessKeyResponse{
			Id:        ak.ID,
			Name:      ak.Name,
			AccessKey: req.AccessKey,
			CreatedAt: ak.CreatedAt,
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(data))
	}
}

// DeleteAKHandler removes an access key.
func DeleteAKHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req DeleteAccessKeyRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		err := req.Validate()
		if err != nil {
			respx.FailGin(c, err)
			return
		}

		if err := b.RemoveAK(req.ID); err != nil {
			respx.FailGin(c, err)
			return
		}
		c.JSON(http.StatusOK, respx.NewRespEmpty())
	}
}
