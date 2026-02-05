package api

import (
	"errors"
	client "messageQ/sdk"
	"strings"
)

type CreateAccessKeyRequest client.CreateAccessKeyRequest
type CreateAccessKeyResponse client.CreateAccessKeyResponse

func (r *CreateAccessKeyRequest) Valid() error {
	name := strings.TrimSpace(r.Name)
	accessKey := strings.TrimSpace(r.AccessKey)
	if name == "" {
		return errors.New("name is required")
	}
	if accessKey == "" {
		return errors.New("access key is required")
	}
	return nil
}

type DeleteAccessKeyRequest struct {
	ID string `uri:"id" binding:"required"`
}

func (r *DeleteAccessKeyRequest) Valid() error {
	id := strings.TrimSpace(r.ID)
	if id == "" {
		return errors.New("id is required")
	}
	return nil
}

type AccessKey client.AccessKey
