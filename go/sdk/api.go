package client

import (
	"resty.dev/v3"
)

type (
	API struct {
		http      *HTTP
		endpoint  *Endpoint
		accessKey string
	}

	Option func(*API)
)

func NewAPI(baseUrl string, adminKey string, options ...Option) *API {
	api := &API{
		http:     newHTTP(adminKey),
		endpoint: newEndpoint(baseUrl),
	}
	for _, option := range options {
		option(api)
	}
	return api
}

func (h *API) Debug() {
	h.http.cli.SetDebug(true)
}

func (h *API) SetAccessKey(accessKey string) {
	h.accessKey = accessKey
}
func (h *API) adminRequest() *resty.Request {
	return h.http.adminRequest()
}

func (h *API) authRequest() (*resty.Request, error) {
	if h.accessKey == "" {
		return nil, ErrNoAccessKey
	}
	return h.http.authRequest(h.accessKey), nil
}

func (h *API) Get(r *resty.Request, path string) (*ErrResp, error) {
	var errResult *ErrResp

	_, err := r.SetError(&errResult).Get(path)
	if err != nil {
		return nil, err
	}
	return errResult, nil
}
func (h *API) Post(r *resty.Request, path string) (*ErrResp, error) {
	var errResult *ErrResp

	_, err := r.SetError(&errResult).Post(path)
	if err != nil {
		return nil, err
	}
	return errResult, nil
}
func (h *API) Del(r *resty.Request, path string) (*ErrResp, error) {
	var errResult *ErrResp

	_, err := r.SetError(&errResult).Delete(path)
	if err != nil {
		return nil, err
	}
	return errResult, nil
}

func (h *API) ListAccessKeys() (*Resp[ListResp[AccessKey]], *ErrResp, error) {

	var results *Resp[ListResp[AccessKey]]

	//r, err := h.authRequest()
	//if err != nil {
	//	return nil, nil, err
	//}

	r := h.adminRequest()

	errResp, err := h.Get(r.SetResult(&results), h.endpoint.ListAccessKeys())
	if err != nil {
		return nil, nil, err
	}
	return results, errResp, nil
}

func (h *API) CreateAccessKey(name string, accessKey string) (*Resp[CreateAccessKeyResponse], *ErrResp, error) {

	req := CreateAccessKeyRequest{
		Name:      name,
		AccessKey: accessKey,
	}

	r := h.adminRequest()
	var result *Resp[CreateAccessKeyResponse]

	errResp, err := h.Post(r.SetBody(req).SetResult(&result), h.endpoint.CreateAccessKey()if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

func (h *API) DeleteAccessKey(id string) (*Resp[string], *ErrResp, error) {

	r := h.adminRequest()

	var result *Resp[string]

	errResp, err := h.Del(r.SetResult(&result), h.endpoint.DeleteAccessKey(id))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}
