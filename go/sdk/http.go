package client

import (
	"messageQ/mq/auth"

	"resty.dev/v3"
)

type HTTP struct {
	cli      *resty.Client
	adminKey string
}

func newHTTP(adminKey string) *HTTP {
	return &HTTP{
		cli:      resty.New(),
		adminKey: adminKey,
	}
}

func (h *HTTP) adminRequest() *resty.Request {
	req := h.cli.R()
	req.SetHeader(auth.AdminHeaderKey, h.adminKey)
	return req
}

func (h *HTTP) authRequest(accessKey string) *resty.Request {
	req := h.cli.R()
	req.SetHeader(auth.AuthHeaderKey, accessKey)
	return req
}
