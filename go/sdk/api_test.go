package client

import (
	"fmt"
	"messageQ/mq/broker"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type APITestSuite struct {
	suite.Suite
	api *API
}

func (suite *APITestSuite) SetupTest() {
	suite.api = NewAPI("http://localhost:8080", "adminkey123")
	suite.api.Debug()
	suite.api.SetAccessKey("test-access-key")
}

func (suite *APITestSuite) TearDownTest() {
	err := suite.api.http.cli.Close()
	if err != nil {
		suite.T().Fatalf("failed to close HTTP client: %v", err)
	}
}

func (suite *APITestSuite) TestListAccessKeys() {
	resp, errResp, err := suite.api.ListAccessKeys()
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
	suite.NotNil(resp.Data)
}

func (suite *APITestSuite) TestCreateAccessKey() {

	name := "test-key"
	key := "test-access-key"
	createResp, errResp, err := suite.api.CreateAccessKey(name, key)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(createResp)
	suite.Equal("ok", createResp.Code)
	suite.NotNil(createResp.Data)
	suite.Equal("test-key", createResp.Data.Name)
}

func (suite *APITestSuite) TestDeleteAccessKey() {
	keyId := "a848cb9c-d1f8-4514-a887-a32b9ce0b7e4"
	_, errResp, err := suite.api.DeleteAccessKey(keyId)
	suite.NoError(err)
	suite.Nil(errResp)
}

func (suite *APITestSuite) TestCreateTopic() {
	topicName := "test-topic-delay"
	topicType := broker.TopicTypeDelay
	queueCount := 4
	createResp, errResp, err := suite.api.CreateTopic(topicName, topicType, queueCount)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(createResp)
	suite.Equal("ok", createResp.Code)
	suite.NotNil(createResp.Data)
	suite.Equal(topicName, createResp.Data.Name)
}

func (suite *APITestSuite) TestGetTopic() {
	topicName := "test-topic-delay"
	getResp, errResp, err := suite.api.GetTopic(topicName)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(getResp)
	suite.Equal("ok", getResp.Code)
	suite.NotNil(getResp.Data)
	suite.Equal(topicName, getResp.Data.Name)
}

func (suite *APITestSuite) TestListTopics() {
	resp, errResp, err := suite.api.ListTopics()
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
	suite.NotNil(resp.Data)
}

func (suite *APITestSuite) TestDeleteTopic() {
	topicName := "test-topic-delay"
	_, errResp, err := suite.api.DelTopic(topicName)
	suite.NoError(err)
	suite.Nil(errResp)
}

func (suite *APITestSuite) TestDelayProduceWithConsume() {
	topic := "test-topic-delay"
	tag := "test-tag"
	delaySeconds := 30

	var wg sync.WaitGroup
	wg.Add(1)
	for i := 0; i < 3; i++ {
		num := i + 1
		timestr := time.Now().Add(time.Second * time.Duration(delaySeconds*num)).Format("2006-01-02 15:04:05")
		body := fmt.Sprintf("Hello, Delay MessageQ! time: %s", timestr)
		produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, body, WithDelaySeconds(int64(delaySeconds*num)))
		suite.NoError(err)
		suite.Nil(errResp)
		suite.NotNil(produceResp)
		suite.Equal("ok", produceResp.Code)
		suite.NotNil(produceResp.Data)
		suite.Equal(topic, produceResp.Data.Topic)
		//wg.Add(1)
	}

	group := "test-group"
	go func() {
		consumeCount := 0
		for true {
			time.Sleep(1 * time.Second)
			consumeResp, errResp, err := suite.api.ConsumeMessages(topic, group, tag)
			if err != nil {
				suite.T().Logf("consume messages error: %v", err)
				continue
			}
			if errResp != nil {
				if errResp.Code != "not_found" {
					suite.T().Logf("consume messages API error: %v", errResp)
				}
				continue
			}
			if consumeResp != nil {
				msg := consumeResp.Data.Message
				timestr := time.Now().Format("2006-01-02 15:04:05")
				suite.T().Logf("Consumed message: %s, body: %s, consume time: %s", msg.ID, msg.Body, timestr)
				consumeCount++
				resp, acrErrResp, err := suite.api.AckMessage(topic, group, msg.ID)
				suite.Nil(err)
				suite.Nil(acrErrResp)
				suite.NotNil(resp)
				suite.Equal("ok", resp.Code)
			}
			if consumeCount >= 3 {
				wg.Done()
			}

		}
	}()

	wg.Wait()

}

func (suite *APITestSuite) TestProduceMessage() {
	topic := "test-topic"
	tag := "test-tag"
	body := "Hello, MessageQ!"
	produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, body)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)
	suite.Equal("ok", produceResp.Code)
	suite.NotNil(produceResp.Data)
	suite.Equal(topic, produceResp.Data.Topic)
}

func (suite *APITestSuite) TestConsumeMessages() {
	topic := "test-topic"
	group := "test-group"
	tag := "test-tag"
	consumeResp, errResp, err := suite.api.ConsumeMessages(topic, group, tag)
	suite.NoError(err)
	if errResp != nil {
		suite.Nil(consumeResp, "expected consumeResp to be nil when errResp is not nil")
		if errResp.Code == "not_found" {
			suite.T().Logf("No messages available for topic: %s, group: %s, tag: %s", topic, group, tag)
			return
		}
		suite.Failf("API error", "consume messages API error: %v", errResp)
		return
	}

	suite.NotNil(consumeResp, "expected consumeResp to be not nil when errResp is nil")
	suite.Equal("ok", consumeResp.Code)
	suite.NotNil(consumeResp.Data)
}

func (suite *APITestSuite) TestBatchProduceConsume() {
	topic := fmt.Sprintf("batch-topic-%d", time.Now().UnixNano())
	group := "batch-group"
	tag := "batch-tag"

	_, errResp, err := suite.api.CreateTopic(topic, broker.TopicTypeNormal, 1)
	suite.NoError(err)
	if errResp != nil && errResp.Code != "topic_exists" {
		suite.T().Fatalf("create topic failed: %v", errResp)
	}

	messages := []ProduceBatchMessage{
		{Body: "batch-1", Tag: tag},
		{Body: "batch-2", Tag: tag},
		{Body: "batch-3", Tag: tag},
	}
	produceResp, errResp, err := suite.api.ProduceBatchMessage(topic, messages)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)
	suite.Equal("ok", produceResp.Code)
	suite.Len(produceResp.Data.Messages, len(messages))
	for _, msg := range produceResp.Data.Messages {
		if msg.ID == "" {
			suite.T().Fatalf("expected message id in batch produce response")
		}
		suite.Equal(topic, msg.Topic)
		suite.Equal(tag, msg.Tag)
	}

	acked := map[string]bool{}
	consumeBatch := func(max int) *Resp[ConsumeBatchResponse] {
		for i := 0; i < 5; i++ {
			resp, errResp, err := suite.api.ConsumeBatchMessages(topic, group, WithBatchQueueId(0), WithBatchTag(tag), WithBatchMax(max))
			suite.NoError(err)
			if errResp != nil {
				if errResp.Code == "not_found" {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				suite.T().Fatalf("consume batch error: %v", errResp)
			}
			if resp == nil {
				suite.T().Fatalf("expected consume batch response, got nil")
			}
			return resp
		}
		suite.T().Fatalf("consume batch timeout")
		return nil
	}

	resp1 := consumeBatch(2)
	suite.Equal("ok", resp1.Code)
	suite.Equal("processing", resp1.Data.State)
	if len(resp1.Data.Messages) == 0 {
		suite.T().Fatalf("expected batch messages, got 0")
	}
	for _, msg := range resp1.Data.Messages {
		if msg.ID == "" {
			suite.T().Fatalf("expected message id in batch consume response")
		}
		suite.Equal(tag, msg.Tag)
		acked[msg.ID] = true
		ackResp, errResp, err := suite.api.AckMessage(topic, group, msg.ID)
		suite.NoError(err)
		suite.Nil(errResp)
		suite.NotNil(ackResp)
		suite.Equal("ok", ackResp.Code)
	}

	resp2 := consumeBatch(10)
	suite.Equal("ok", resp2.Code)
	suite.Equal("processing", resp2.Data.State)
	if len(resp2.Data.Messages) == 0 {
		suite.T().Fatalf("expected remaining batch messages, got 0")
	}
	for _, msg := range resp2.Data.Messages {
		if msg.ID == "" {
			suite.T().Fatalf("expected message id in batch consume response")
		}
		if acked[msg.ID] {
			suite.T().Fatalf("duplicate message consumed: %s", msg.ID)
		}
		suite.Equal(tag, msg.Tag)
		acked[msg.ID] = true
		ackResp, errResp, err := suite.api.AckMessage(topic, group, msg.ID)
		suite.NoError(err)
		suite.Nil(errResp)
		suite.NotNil(ackResp)
		suite.Equal("ok", ackResp.Code)
	}

	suite.Len(acked, len(messages))
}

func (suite *APITestSuite) TestListMessages() {
	group := "test-group"
	tag := "test-tag"

	base := fmt.Sprintf("list-%d", time.Now().UnixNano())
	normalTopic := base + "-normal"
	delayTopic := base + "-delay"

	// create topics (ignore already exists)
	_, errResp, err := suite.api.CreateTopic(normalTopic, broker.TopicTypeNormal, 1)
	if err != nil {
		suite.T().Fatalf("create normal topic error: %v", err)
	}
	if errResp != nil && errResp.Code != "topic_exists" {
		suite.T().Fatalf("create normal topic failed: %v", errResp)
	}
	_, errResp, err = suite.api.CreateTopic(delayTopic, broker.TopicTypeDelay, 1)
	if err != nil {
		suite.T().Fatalf("create delay topic error: %v", err)
	}
	if errResp != nil && errResp.Code != "topic_exists" {
		suite.T().Fatalf("create delay topic failed: %v", errResp)
	}

	// produce multiple normal messages for pending/cursor
	for i := 0; i < 5; i++ {
		_, errResp, err = suite.api.ProduceMessage(normalTopic, tag, fmt.Sprintf("list-message-%d", i+1))
		suite.NoError(err)
		suite.Nil(errResp)
	}

	// pending list (cursor)
	listResp, errResp, err := suite.api.ListMessages(normalTopic, group, "pending", WithListQueueId(0), WithListLimit(1))
	suite.NoError(err)
	if errResp != nil {
		suite.T().Fatalf("list messages error: %v", errResp)
	}
	if listResp == nil {
		suite.T().Skip("list messages endpoint not available on server")
	}
	suite.Equal("ok", listResp.Code)
	suite.Equal("pending", listResp.Data.State)
	suite.Equal(1, len(listResp.Data.Messages), "expected 1 message due to limit=1")
	if len(listResp.Data.Messages) == 0 {
		suite.T().Fatalf("expected pending messages, got 0")
	}
	if listResp.Data.Messages[0].ID == "" {
		suite.T().Fatalf("expected message id in pending list")
	}
	if listResp.Data.Messages[0].QueueID == nil || listResp.Data.Messages[0].Offset == nil {
		suite.T().Fatalf("expected queueId/offset in pending list")
	}
	if listResp.Data.NextCursor != nil {
		listResp2, errResp, err := suite.api.ListMessages(normalTopic, group, "pending", WithListQueueId(0), WithListCursor(*listResp.Data.NextCursor), WithListLimit(10))
		suite.NoError(err)
		suite.Nil(errResp)
		suite.NotNil(listResp2)
		suite.Equal("ok", listResp2.Code)
		suite.Equal("pending", listResp2.Data.State)
	}

	// consume to processing
	consumeResp, errResp, err := suite.api.ConsumeMessages(normalTopic, group, tag, WithQueueId(0))
	suite.NoError(err)
	if errResp != nil {
		suite.T().Fatalf("consume error: %v", errResp)
	}
	msgID := consumeResp.Data.Message.ID

	processingResp, errResp, err := suite.api.ListMessages(normalTopic, group, "processing", WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(processingResp)
	suite.Equal("processing", processingResp.Data.State)
	if len(processingResp.Data.Messages) == 0 {
		suite.T().Fatalf("expected processing messages, got 0")
	}
	foundProcessing := false
	for _, msg := range processingResp.Data.Messages {
		if msg.ID == msgID {
			foundProcessing = true
			if msg.ConsumedAt == nil {
				suite.T().Fatalf("expected consumedAt for processing message")
			}
			break
		}
	}
	if !foundProcessing {
		suite.T().Fatalf("processing list did not include consumed message")
	}

	// ack to completed
	ackResp, errResp, err := suite.api.AckMessage(normalTopic, group, msgID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(ackResp)

	completedResp, errResp, err := suite.api.ListMessages(normalTopic, group, "completed", WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(completedResp)
	suite.Equal("completed", completedResp.Data.State)
	if len(completedResp.Data.Messages) == 0 {
		suite.T().Fatalf("expected completed messages, got 0")
	}
	foundCompleted := false
	for _, msg := range completedResp.Data.Messages {
		if msg.ID == msgID {
			foundCompleted = true
			if msg.AckedAt == nil {
				suite.T().Fatalf("expected ackedAt for completed message")
			}
			break
		}
	}
	if !foundCompleted {
		suite.T().Fatalf("completed list did not include acked message")
	}

	listResp2, errResp, err := suite.api.ListMessages(normalTopic, group, "pending", WithListQueueId(0), WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(listResp2)
	suite.Equal("ok", listResp2.Code)
	suite.Equal("pending", listResp2.Data.State)
	suite.Equal(4, len(listResp2.Data.Messages), "expected 4 pending messages after consuming 1")

	// scheduled list
	_, errResp, err = suite.api.ProduceMessage(delayTopic, tag, "list-message-delay", WithDelaySeconds(30))
	suite.NoError(err)
	suite.Nil(errResp)

	scheduledResp, errResp, err := suite.api.ListMessages(delayTopic, group, "scheduled", WithListQueueId(0), WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(scheduledResp)
	suite.Equal("scheduled", scheduledResp.Data.State)
	if len(scheduledResp.Data.Messages) == 0 {
		suite.T().Fatalf("expected scheduled messages, got 0")
	}
	if scheduledResp.Data.Messages[0].ScheduledAt == nil {
		suite.T().Fatalf("expected scheduledAt for scheduled message")
	}
}

func (suite *APITestSuite) TestGetFullStats() {

	resp, errResp, err := suite.api.GetStats()
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
	suite.NotNil(resp.Data)
}

func (suite *APITestSuite) TestGetTopicStats() {
	topicName := "test-topic"
	resp, errResp, err := suite.api.GetTopicStats(topicName)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
	suite.NotNil(resp.Data)
}

func TestAPITestSuite(t *testing.T) {

	suite.Run(t, new(APITestSuite))
}
