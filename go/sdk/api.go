package client

import (
	"fmt"
	"messageQ/mq/broker"

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

	errResp, err := h.Post(r.SetBody(req).SetResult(&result), h.endpoint.CreateAccessKey())
	if err != nil {
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

func (h *API) CreateTopic(name string, topicType broker.TopicType, queueCount int) (*Resp[TopicResponse], *ErrResp, error) {

	req := CreateTopicRequest{
		Name:       name,
		Type:       topicType,
		QueueCount: queueCount,
	}

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	var result *Resp[TopicResponse]
	errResp, err := h.Post(r.SetBody(req).SetResult(&result), h.endpoint.CreateTopic())
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

func (h *API) GetTopic(topic string) (*Resp[TopicResponse], *ErrResp, error) {

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	var result *Resp[TopicResponse]
	errResp, err := h.Get(r.SetResult(&result), h.endpoint.GetTopic(topic))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

func (h *API) ListTopics() (*Resp[ListResp[TopicResponse]], *ErrResp, error) {

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	var result *Resp[ListResp[TopicResponse]]
	errResp, err := h.Get(r.SetResult(&result), h.endpoint.ListTopics())
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

func (h *API) DelTopic(topic string) (*Resp[DeleteTopicResponse], *ErrResp, error) {

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	var result *Resp[DeleteTopicResponse]
	errResp, err := h.Del(r.SetResult(&result), h.endpoint.DeleteTopic(topic))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

type ProduceDelayOption func(*ProduceMessageRequest)

func WithDelaySeconds(delaySec int64) ProduceDelayOption {
	return func(r *ProduceMessageRequest) {
		r.DelaySec = delaySec
	}
}
func WithDelayMilliseconds(delayMs int64) ProduceDelayOption {
	return func(r *ProduceMessageRequest) {
		r.DelayMs = delayMs
	}
}

func (h *API) ProduceMessage(topic string, tag string, body string, options ...ProduceDelayOption) (*Resp[ProduceMessageResponse], *ErrResp, error) {

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	req := ProduceMessageRequest{
		Tag:  tag,
		Body: body,
	}
	for _, option := range options {
		option(&req)
	}

	var result *Resp[ProduceMessageResponse]
	errResp, err := h.Post(r.SetBody(req).SetResult(&result), h.endpoint.ProduceMessage(topic))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

type ProduceBatchOption func(*ProduceBatchMessage)

func WithBatchDelaySeconds(delaySec int64) ProduceBatchOption {
	return func(m *ProduceBatchMessage) {
		m.DelaySec = delaySec
	}
}

func WithBatchDelayMilliseconds(delayMs int64) ProduceBatchOption {
	return func(m *ProduceBatchMessage) {
		m.DelayMs = delayMs
	}
}

func (h *API) ProduceBatchMessage(topic string, messages []ProduceBatchMessage) (*Resp[ProduceBatchResponse], *ErrResp, error) {
	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	req := ProduceBatchRequest{Messages: messages}
	var result *Resp[ProduceBatchResponse]
	errResp, err := h.Post(r.SetBody(req).SetResult(&result), h.endpoint.ProduceBatchMessage(topic))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

type ConsumeMessageOption func(*resty.Request)

func WithQueueId(queueId int) ConsumeMessageOption {
	return func(r *resty.Request) {
		r.SetQueryParam("queueId", fmt.Sprintf("%d", queueId))
	}
}
func (h *API) ConsumeMessages(topic string, group string, tag string, options ...ConsumeMessageOption) (*Resp[ConsumeMessageResponse], *ErrResp, error) {

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	r.SetQueryParam("tag", tag)

	for _, option := range options {
		option(r)
	}

	var result *Resp[ConsumeMessageResponse]
	errResp, err := h.Get(r.SetResult(&result), h.endpoint.ConsumeMessages(topic, group))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

type ConsumeBatchOption func(*resty.Request)

func WithBatchQueueId(queueId int) ConsumeBatchOption {
	return func(r *resty.Request) {
		r.SetQueryParam("queueId", fmt.Sprintf("%d", queueId))
	}
}

func WithBatchTag(tag string) ConsumeBatchOption {
	return func(r *resty.Request) {
		r.SetQueryParam("tag", tag)
	}
}

func WithBatchMax(max int) ConsumeBatchOption {
	return func(r *resty.Request) {
		r.SetQueryParam("max", fmt.Sprintf("%d", max))
	}
}

func (h *API) ConsumeBatchMessages(topic string, group string, options ...ConsumeBatchOption) (*Resp[ConsumeBatchResponse], *ErrResp, error) {
	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}
	for _, option := range options {
		option(r)
	}

	var result *Resp[ConsumeBatchResponse]
	errResp, err := h.Get(r.SetResult(&result), h.endpoint.ConsumeBatchMessages(topic, group))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

type ListMessagesOption func(*resty.Request)

func WithListQueueId(queueId int) ListMessagesOption {
	return func(r *resty.Request) {
		r.SetQueryParam("queueId", fmt.Sprintf("%d", queueId))
	}
}

func WithListTag(tag string) ListMessagesOption {
	return func(r *resty.Request) {
		r.SetQueryParam("tag", tag)
	}
}

func WithListLimit(limit int) ListMessagesOption {
	return func(r *resty.Request) {
		r.SetQueryParam("limit", fmt.Sprintf("%d", limit))
	}
}

func WithListCursor(cursor int64) ListMessagesOption {
	return func(r *resty.Request) {
		r.SetQueryParam("cursor", fmt.Sprintf("%d", cursor))
	}
}

func (h *API) ListMessages(topic string, group string, state string, options ...ListMessagesOption) (*Resp[ListMessagesResponse], *ErrResp, error) {
	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}
	if state != "" {
		r.SetQueryParam("state", state)
	}
	for _, option := range options {
		option(r)
	}

	var result *Resp[ListMessagesResponse]
	errResp, err := h.Get(r.SetResult(&result), h.endpoint.ListMessages(topic, group))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

func (h *API) AckMessage(topic string, group string, id string) (*Resp[AckMessageResponse], *ErrResp, error) {

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	var result *Resp[AckMessageResponse]
	errResp, err := h.Post(r.SetResult(&result), h.endpoint.AckMessage(topic, group, id))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}

func (h *API) NackMessage(topic string, group string, id string) (*Resp[NackMessageResponse], *ErrResp, error) {

	r, err := h.authRequest()
	if err != nil {
		return nil, nil, err
	}

	var result *Resp[NackMessageResponse]
	errResp, err := h.Post(r.SetResult(&result), h.endpoint.NackMessage(topic, group, id))
	if err != nil {
		return nil, nil, err
	}
	return result, errResp, nil
}
