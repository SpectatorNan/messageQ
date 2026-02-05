package client

type CreateAccessKeyRequest struct {
	Name      string `json:"name"`
	AccessKey string `json:"accessKey"`
}

type CreateAccessKeyResponse struct {
	Id        string `json:"id"`
	Name      string `json:"name"`
	AccessKey string `json:"accessKey"`
	CreatedAt int64  `json:"createdAt"`
}

type AccessKey struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt int64  `json:"createdAt"`
}

type Resp[T any] struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data,omitempty"`
}

type ListResp[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

type ErrResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
