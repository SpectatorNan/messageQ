package client_test

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	serverapi "github.com/SpectatorNan/messageQ/go/mq/api"
	"github.com/SpectatorNan/messageQ/go/mq/broker"
	"github.com/SpectatorNan/messageQ/go/mq/logger"
	"github.com/SpectatorNan/messageQ/go/mq/storage"
	client "github.com/SpectatorNan/messageQ/go/sdk"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type APITestSuite struct {
	suite.Suite
	api       *client.API
	server    *httptest.Server
	broker    *broker.Broker
	store     *storage.WALStorage
	dataDir   string
	adminKey  string
	accessKey string
}

type failingSDKProcessingStore struct {
	*storage.WALStorage
	saveErr          error
	retryEventErr    error
	terminalEventErr error
}

func (s *failingSDKProcessingStore) SaveProcessing(rec storage.ProcessingRecord) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.WALStorage.SaveProcessing(rec)
}

func (s *failingSDKProcessingStore) AppendDeliveryEvent(rec storage.DeliveryEventRecord) error {
	if rec.Event == storage.DeliveryEventRetry && s.retryEventErr != nil {
		return s.retryEventErr
	}
	if (rec.Event == storage.DeliveryEventCancelled || rec.Event == storage.DeliveryEventExpired) && s.terminalEventErr != nil {
		return s.terminalEventErr
	}
	return s.WALStorage.AppendDeliveryEvent(rec)
}

func (suite *APITestSuite) SetupSuite() {
	suite.adminKey = "adminkey123"
	suite.accessKey = "test-access-key"

	suite.Require().NoError(logger.InitDefault())
	suite.Require().NoError(os.Setenv("MQ_ADMIN_KEY", suite.adminKey))

	dataDir, err := os.MkdirTemp("", "messageq-sdk-api-*")
	suite.Require().NoError(err)
	suite.dataDir = dataDir

	store := storage.NewWALStorage(filepath.Join(dataDir, "data"), 10*time.Millisecond, time.Minute)
	suite.store = store
	suite.broker = broker.NewBrokerWithPersistence(store, 4, filepath.Join(dataDir, "data"))
	_, err = suite.broker.AddAK("default-test-access-key", suite.accessKey)
	suite.Require().NoError(err)

	suite.server = httptest.NewServer(serverapi.NewRouter(suite.broker))
}

func (suite *APITestSuite) TearDownSuite() {
	if suite.server != nil {
		suite.server.Close()
	}
	if suite.broker != nil {
		suite.Require().NoError(suite.broker.Close())
	}
	if suite.store != nil {
		suite.Require().NoError(suite.store.Close())
	}
	if suite.dataDir != "" {
		suite.Require().NoError(os.RemoveAll(suite.dataDir))
	}
	suite.Require().NoError(os.Unsetenv("MQ_ADMIN_KEY"))
}

func (suite *APITestSuite) SetupTest() {
	suite.ensureAccessKey()
	suite.ensureTopic("test-topic", broker.TopicTypeNormal, 1)
	suite.ensureTopic("test-topic-delay", broker.TopicTypeDelay, 1)

	suite.api = client.NewAPI(suite.server.URL, suite.adminKey)
	suite.api.SetAccessKey(suite.accessKey)
}

func (suite *APITestSuite) TearDownTest() {
	suite.api = nil
}

func (suite *APITestSuite) ensureAccessKey() {
	if suite.broker.IsAKValid(suite.accessKey) {
		return
	}
	_, err := suite.broker.AddAK("test-access-key-"+uuid.NewString(), suite.accessKey)
	suite.Require().NoError(err)
}

func (suite *APITestSuite) ensureTopic(name string, topicType broker.TopicType, queueCount int) {
	err := suite.broker.CreateTopic(name, topicType, queueCount)
	if err == nil {
		return
	}
	_, getErr := suite.broker.GetTopicConfig(name)
	suite.Require().NoError(getErr)
}

func (suite *APITestSuite) newTopic(prefix string, topicType broker.TopicType) string {
	topic := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	suite.ensureTopic(topic, topicType, 1)
	return topic
}

func (suite *APITestSuite) newGroup(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func (suite *APITestSuite) consumeUntil(topic, group, tag string, deadline time.Time, options ...client.ConsumeMessageOption) *client.Resp[client.ConsumeMessageResponse] {
	for time.Now().Before(deadline) {
		resp, errResp, err := suite.api.ConsumeMessages(topic, group, tag, options...)
		suite.NoError(err)
		if errResp != nil {
			if errResp.Code == "not_found" {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			suite.T().Fatalf("consume messages API error: %v", errResp)
		}
		if resp != nil {
			return resp
		}
	}
	suite.T().Fatalf("consume timed out for topic=%s group=%s", topic, group)
	return nil
}

func (suite *APITestSuite) consumeBatchUntil(topic, group string, deadline time.Time, options ...client.ConsumeBatchOption) *client.Resp[client.ConsumeBatchResponse] {
	for time.Now().Before(deadline) {
		resp, errResp, err := suite.api.ConsumeBatchMessages(topic, group, options...)
		suite.NoError(err)
		if errResp != nil {
			if errResp.Code == "not_found" {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			suite.T().Fatalf("consume batch API error: %v", errResp)
		}
		if resp != nil {
			return resp
		}
	}
	suite.T().Fatalf("consume batch timed out for topic=%s group=%s", topic, group)
	return nil
}

func (suite *APITestSuite) primeGroup(topic, group, tag string) {
	resp, errResp, err := suite.api.ConsumeMessages(topic, group, tag, client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(resp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)
}

func TestConsumeReturnsInternalErrorWhenProcessingPersistenceFails(t *testing.T) {
	const adminKey = "adminkey123"
	const accessKey = "test-access-key"

	require.NoError(t, logger.InitDefault())

	oldAdminKey, hadAdminKey := os.LookupEnv("MQ_ADMIN_KEY")
	require.NoError(t, os.Setenv("MQ_ADMIN_KEY", adminKey))
	defer func() {
		if hadAdminKey {
			_ = os.Setenv("MQ_ADMIN_KEY", oldAdminKey)
			return
		}
		_ = os.Unsetenv("MQ_ADMIN_KEY")
	}()

	dataDir, err := os.MkdirTemp("", "messageq-sdk-api-processing-fail-*")
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	walStore := storage.NewWALStorage(filepath.Join(dataDir, "data"), 10*time.Millisecond, time.Minute)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingSDKProcessingStore{
		WALStorage: walStore,
		saveErr:    errors.New("save processing failed"),
	}
	b := broker.NewBrokerWithPersistence(store, 1, filepath.Join(dataDir, "data"))
	defer func() {
		_ = b.Close()
	}()

	_, err = b.AddAK("default-test-access-key", accessKey)
	require.NoError(t, err)
	require.NoError(t, b.CreateTopic("orders", broker.TopicTypeNormal, 1))

	server := httptest.NewServer(serverapi.NewRouter(b))
	defer server.Close()

	api := client.NewAPI(server.URL, adminKey)
	api.SetAccessKey(accessKey)

	group := "group-" + uuid.NewString()
	tag := "tag-a"

	resp, errResp, err := api.ConsumeMessages("orders", group, tag, client.WithQueueId(0))
	require.NoError(t, err)
	require.Nil(t, resp)
	require.NotNil(t, errResp)
	require.Equal(t, "not_found", errResp.Code)

	produceResp, errResp, err := api.ProduceMessage("orders", tag, "bad-message", client.WithCorrelationID("corr-"+uuid.NewString()))
	require.NoError(t, err)
	require.Nil(t, errResp)
	require.NotNil(t, produceResp)

	consumeResp, errResp, err := api.ConsumeMessages("orders", group, tag, client.WithQueueId(0))
	require.NoError(t, err)
	require.Nil(t, consumeResp)
	require.NotNil(t, errResp)
	require.Equal(t, "internal_error", errResp.Code)

	retryResp, errResp, err := api.ListMessages("orders", group, "retry", client.WithListLimit(10))
	require.NoError(t, err)
	require.Nil(t, errResp)
	require.NotNil(t, retryResp)
	require.Len(t, retryResp.Data.Messages, 1)
	require.Equal(t, produceResp.Data.ID, retryResp.Data.Messages[0].ID)
	require.NotNil(t, retryResp.Data.Messages[0].EventAt)
}

func TestNackReturnsInternalErrorWhenRetryPersistenceFails(t *testing.T) {
	const adminKey = "adminkey123"
	const accessKey = "test-access-key"

	require.NoError(t, logger.InitDefault())

	oldAdminKey, hadAdminKey := os.LookupEnv("MQ_ADMIN_KEY")
	require.NoError(t, os.Setenv("MQ_ADMIN_KEY", adminKey))
	defer func() {
		if hadAdminKey {
			_ = os.Setenv("MQ_ADMIN_KEY", oldAdminKey)
			return
		}
		_ = os.Unsetenv("MQ_ADMIN_KEY")
	}()

	dataDir, err := os.MkdirTemp("", "messageq-sdk-api-retry-fail-*")
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	walStore := storage.NewWALStorage(filepath.Join(dataDir, "data"), 10*time.Millisecond, time.Minute)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingSDKProcessingStore{
		WALStorage:    walStore,
		retryEventErr: errors.New("append retry event failed"),
	}
	b := broker.NewBrokerWithPersistence(store, 1, filepath.Join(dataDir, "data"))
	defer func() {
		_ = b.Close()
	}()

	_, err = b.AddAK("default-test-access-key", accessKey)
	require.NoError(t, err)
	require.NoError(t, b.CreateTopic("orders", broker.TopicTypeNormal, 1))

	server := httptest.NewServer(serverapi.NewRouter(b))
	defer server.Close()

	api := client.NewAPI(server.URL, adminKey)
	api.SetAccessKey(accessKey)

	group := "group-" + uuid.NewString()
	tag := "tag-a"
	resp, errResp, err := api.ConsumeMessages("orders", group, tag, client.WithQueueId(0))
	require.NoError(t, err)
	require.Nil(t, resp)
	require.NotNil(t, errResp)
	require.Equal(t, "not_found", errResp.Code)

	produceResp, errResp, err := api.ProduceMessage("orders", tag, "retry-bad-message", client.WithCorrelationID("corr-"+uuid.NewString()))
	require.NoError(t, err)
	require.Nil(t, errResp)
	require.NotNil(t, produceResp)

	consumeResp, errResp, err := api.ConsumeMessages("orders", group, tag, client.WithQueueId(0))
	require.NoError(t, err)
	require.Nil(t, errResp)
	require.NotNil(t, consumeResp)

	nackResp, errResp, err := api.NackMessage("orders", group, produceResp.Data.ID)
	require.NoError(t, err)
	require.Nil(t, nackResp)
	require.NotNil(t, errResp)
	require.Equal(t, "internal_error", errResp.Code)

	retryResp, errResp, err := api.ListMessages("orders", group, "retry", client.WithListLimit(10))
	require.NoError(t, err)
	require.Nil(t, errResp)
	require.NotNil(t, retryResp)
	require.Len(t, retryResp.Data.Messages, 0)
}

func TestTerminateReturnsInternalErrorWhenTerminalPersistenceFails(t *testing.T) {
	const adminKey = "adminkey123"
	const accessKey = "test-access-key"

	require.NoError(t, logger.InitDefault())

	oldAdminKey, hadAdminKey := os.LookupEnv("MQ_ADMIN_KEY")
	require.NoError(t, os.Setenv("MQ_ADMIN_KEY", adminKey))
	defer func() {
		if hadAdminKey {
			_ = os.Setenv("MQ_ADMIN_KEY", oldAdminKey)
			return
		}
		_ = os.Unsetenv("MQ_ADMIN_KEY")
	}()

	dataDir, err := os.MkdirTemp("", "messageq-sdk-api-terminate-fail-*")
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	walStore := storage.NewWALStorage(filepath.Join(dataDir, "data"), 10*time.Millisecond, time.Minute)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingSDKProcessingStore{
		WALStorage:       walStore,
		terminalEventErr: errors.New("append terminal event failed"),
	}
	b := broker.NewBrokerWithPersistence(store, 1, filepath.Join(dataDir, "data"))
	defer func() {
		_ = b.Close()
	}()

	_, err = b.AddAK("default-test-access-key", accessKey)
	require.NoError(t, err)
	require.NoError(t, b.CreateTopic("orders", broker.TopicTypeNormal, 1))

	server := httptest.NewServer(serverapi.NewRouter(b))
	defer server.Close()

	api := client.NewAPI(server.URL, adminKey)
	api.SetAccessKey(accessKey)

	group := "group-" + uuid.NewString()
	tag := "tag-a"
	produceResp, errResp, err := api.ProduceMessage("orders", tag, "cancel-bad-message", client.WithCorrelationID("corr-"+uuid.NewString()))
	require.NoError(t, err)
	require.Nil(t, errResp)
	require.NotNil(t, produceResp)

	terminateResp, errResp, err := api.TerminateMessage("orders", group, produceResp.Data.ID)
	require.NoError(t, err)
	require.Nil(t, terminateResp)
	require.NotNil(t, errResp)
	require.Equal(t, "internal_error", errResp.Code)
}

func (suite *APITestSuite) TestListAccessKeys() {
	resp, errResp, err := suite.api.ListAccessKeys()
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
	suite.NotEmpty(resp.Data.Items)
}

func (suite *APITestSuite) TestCreateAccessKey() {
	name := "test-key-" + uuid.NewString()
	key := "ak-" + uuid.NewString()

	createResp, errResp, err := suite.api.CreateAccessKey(name, key)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(createResp)
	suite.Equal("ok", createResp.Code)
	suite.Equal(name, createResp.Data.Name)
	suite.NotEmpty(createResp.Data.Id)
}

func (suite *APITestSuite) TestDeleteAccessKey() {
	name := "delete-key-" + uuid.NewString()
	key := "delete-ak-" + uuid.NewString()

	createResp, errResp, err := suite.api.CreateAccessKey(name, key)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(createResp)

	deleteResp, errResp, err := suite.api.DeleteAccessKey(createResp.Data.Id)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(deleteResp)
}

func (suite *APITestSuite) TestCreateTopic() {
	topicName := fmt.Sprintf("create-topic-%d", time.Now().UnixNano())
	createResp, errResp, err := suite.api.CreateTopic(topicName, broker.TopicTypeDelay, 1)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(createResp)
	suite.Equal("ok", createResp.Code)
	suite.Equal(topicName, createResp.Data.Name)
}

func (suite *APITestSuite) TestGetTopic() {
	topicName := suite.newTopic("get-topic", broker.TopicTypeDelay)
	getResp, errResp, err := suite.api.GetTopic(topicName)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(getResp)
	suite.Equal("ok", getResp.Code)
	suite.Equal(topicName, getResp.Data.Name)
}

func (suite *APITestSuite) TestListTopics() {
	suite.newTopic("list-topic", broker.TopicTypeNormal)

	resp, errResp, err := suite.api.ListTopics()
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
	suite.NotEmpty(resp.Data.Items)
}

func (suite *APITestSuite) TestDeleteTopic() {
	topicName := suite.newTopic("delete-topic", broker.TopicTypeNormal)
	resp, errResp, err := suite.api.DelTopic(topicName)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.True(resp.Data.Deleted)
	suite.Equal(topicName, resp.Data.Topic)
}

func (suite *APITestSuite) TestDelayProduceWithConsume() {
	topic := suite.newTopic("delay-produce", broker.TopicTypeDelay)
	group := suite.newGroup("delay-group")
	tag := "test-tag"
	suite.primeGroup(topic, group, tag)

	expected := map[string]string{}
	for i := 0; i < 2; i++ {
		correlationID := fmt.Sprintf("delay-cid-%d-%d", i, time.Now().UnixNano())
		produceResp, errResp, err := suite.api.ProduceMessage(
			topic,
			tag,
			fmt.Sprintf("delayed-message-%d", i+1),
			client.WithDelayMilliseconds(int64(150*(i+1))),
			client.WithCorrelationID(correlationID),
		)
		suite.NoError(err)
		suite.Nil(errResp)
		suite.NotNil(produceResp)
		expected[produceResp.Data.ID] = correlationID
	}

	deadline := time.Now().Add(5 * time.Second)
	consumed := map[string]bool{}
	for len(consumed) < len(expected) && time.Now().Before(deadline) {
		consumeResp, errResp, err := suite.api.ConsumeMessages(topic, group, tag, client.WithQueueId(0))
		if err != nil {
			suite.T().Fatalf("consume messages error: %v", err)
		}
		if errResp != nil {
			if errResp.Code == "not_found" {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			suite.T().Fatalf("consume messages API error: %v", errResp)
		}
		if consumeResp == nil {
			continue
		}

		msg := consumeResp.Data.Message
		suite.Equal(expected[msg.ID], msg.CorrelationID)
		consumed[msg.ID] = true
		ackResp, ackErrResp, err := suite.api.AckMessage(topic, group, msg.ID)
		suite.NoError(err)
		suite.Nil(ackErrResp)
		suite.NotNil(ackResp)
		suite.Equal("ok", ackResp.Code)
	}

	suite.Len(consumed, len(expected))
}

func (suite *APITestSuite) TestProduceMessage() {
	topic := suite.newTopic("produce-topic", broker.TopicTypeNormal)
	tag := "test-tag"
	body := "Hello, MessageQ!"
	correlationID := "produce-cid-" + uuid.NewString()

	produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, body, client.WithCorrelationID(correlationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)
	suite.Equal("ok", produceResp.Code)
	suite.Equal(topic, produceResp.Data.Topic)
	suite.Equal(correlationID, produceResp.Data.CorrelationID)
}

func (suite *APITestSuite) TestProduceMessageCorrelationIDValidation() {
	topic := suite.newTopic("produce-invalid-cid", broker.TopicTypeNormal)
	invalidCorrelationID := strings.Repeat("界", 43) // 129 UTF-8 bytes

	produceResp, errResp, err := suite.api.ProduceMessage(topic, "test-tag", "Hello", client.WithCorrelationID(invalidCorrelationID))
	suite.NoError(err)
	suite.Nil(produceResp)
	suite.NotNil(errResp)
	suite.Equal("invalid_message", errResp.Code)
}

func (suite *APITestSuite) TestConsumeMessages() {
	topic := suite.newTopic("consume-topic", broker.TopicTypeNormal)
	group := suite.newGroup("consume-group")
	tag := "test-tag"
	correlationID := "consume-cid-" + uuid.NewString()
	suite.primeGroup(topic, group, tag)

	produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, "Hello, consume!", client.WithCorrelationID(correlationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)

	consumeResp := suite.consumeUntil(topic, group, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	suite.Equal("ok", consumeResp.Code)
	suite.Equal(correlationID, consumeResp.Data.Message.CorrelationID)
	ackResp, ackErrResp, err := suite.api.AckMessage(topic, group, consumeResp.Data.Message.ID)
	suite.NoError(err)
	suite.Nil(ackErrResp)
	suite.NotNil(ackResp)
}

func (suite *APITestSuite) TestBatchProduceConsume() {
	topic := suite.newTopic("batch-topic", broker.TopicTypeNormal)
	group := suite.newGroup("batch-group")
	tag := "batch-tag"
	suite.primeGroup(topic, group, tag)

	messages := []client.ProduceBatchMessage{
		{Body: "batch-1", Tag: tag, CorrelationID: "batch-cid-1"},
		{Body: "batch-2", Tag: tag, CorrelationID: "batch-cid-2"},
		{Body: "batch-3", Tag: tag, CorrelationID: "batch-cid-3"},
	}
	produceResp, errResp, err := suite.api.ProduceBatchMessage(topic, messages)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)
	suite.Equal("ok", produceResp.Code)
	suite.Len(produceResp.Data.Messages, len(messages))

	expected := map[string]string{}
	for _, msg := range produceResp.Data.Messages {
		expected[msg.ID] = msg.CorrelationID
	}

	acked := map[string]bool{}
	resp1 := suite.consumeBatchUntil(topic, group, time.Now().Add(3*time.Second), client.WithBatchQueueId(0), client.WithBatchTag(tag), client.WithBatchMax(2))
	suite.Equal("ok", resp1.Code)
	suite.Equal("processing", resp1.Data.State)
	for _, msg := range resp1.Data.Messages {
		suite.Equal(expected[msg.ID], msg.CorrelationID)
		acked[msg.ID] = true
		ackResp, ackErrResp, err := suite.api.AckMessage(topic, group, msg.ID)
		suite.NoError(err)
		suite.Nil(ackErrResp)
		suite.NotNil(ackResp)
	}

	resp2 := suite.consumeBatchUntil(topic, group, time.Now().Add(3*time.Second), client.WithBatchQueueId(0), client.WithBatchTag(tag), client.WithBatchMax(10))
	suite.Equal("ok", resp2.Code)
	for _, msg := range resp2.Data.Messages {
		suite.Equal(expected[msg.ID], msg.CorrelationID)
		suite.False(acked[msg.ID])
		acked[msg.ID] = true
		ackResp, ackErrResp, err := suite.api.AckMessage(topic, group, msg.ID)
		suite.NoError(err)
		suite.Nil(ackErrResp)
		suite.NotNil(ackResp)
	}

	suite.Len(acked, len(messages))
}

func (suite *APITestSuite) TestListMessages() {
	group := suite.newGroup("list-group")
	tag := "test-tag"

	normalTopic := suite.newTopic("list-normal", broker.TopicTypeNormal)
	delayTopic := suite.newTopic("list-delay", broker.TopicTypeDelay)
	suite.primeGroup(normalTopic, group, tag)
	suite.primeGroup(delayTopic, group, tag)

	correlations := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		correlationID := fmt.Sprintf("list-cid-%d-%d", i, time.Now().UnixNano())
		correlations = append(correlations, correlationID)
		_, errResp, err := suite.api.ProduceMessage(normalTopic, tag, fmt.Sprintf("list-message-%d", i+1), client.WithCorrelationID(correlationID))
		suite.NoError(err)
		suite.Nil(errResp)
	}

	listResp, errResp, err := suite.api.ListMessages(normalTopic, group, "pending", client.WithListQueueId(0), client.WithListLimit(2))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(listResp)
	suite.Equal("pending", listResp.Data.State)
	suite.NotEmpty(listResp.Data.Messages)
	suite.NotNil(listResp.Data.Messages[0].QueueID)
	suite.NotNil(listResp.Data.Messages[0].Offset)
	suite.NotEmpty(listResp.Data.Messages[0].CorrelationID)

	consumeResp := suite.consumeUntil(normalTopic, group, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	msgID := consumeResp.Data.Message.ID
	msgCorrelationID := consumeResp.Data.Message.CorrelationID

	processingResp, errResp, err := suite.api.ListMessages(normalTopic, group, "processing", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(processingResp)
	suite.Equal("processing", processingResp.Data.State)
	foundProcessing := false
	for _, msg := range processingResp.Data.Messages {
		if msg.ID == msgID {
			foundProcessing = true
			suite.NotNil(msg.ConsumedAt)
			suite.Equal(msgCorrelationID, msg.CorrelationID)
			break
		}
	}
	suite.True(foundProcessing)

	ackResp, errResp, err := suite.api.AckMessage(normalTopic, group, msgID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(ackResp)

	completedResp, errResp, err := suite.api.ListMessages(normalTopic, group, "completed", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(completedResp)
	suite.Equal("completed", completedResp.Data.State)
	foundCompleted := false
	for _, msg := range completedResp.Data.Messages {
		if msg.ID == msgID {
			foundCompleted = true
			suite.NotNil(msg.AckedAt)
			suite.Equal(msgCorrelationID, msg.CorrelationID)
			break
		}
	}
	suite.True(foundCompleted)

	delayCorrelationID := "scheduled-cid-" + uuid.NewString()
	_, errResp, err = suite.api.ProduceMessage(delayTopic, tag, "list-message-delay", client.WithDelayMilliseconds(500), client.WithCorrelationID(delayCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)

	scheduledResp, errResp, err := suite.api.ListMessages(delayTopic, group, "scheduled", client.WithListQueueId(0), client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(scheduledResp)
	suite.Equal("scheduled", scheduledResp.Data.State)
	suite.NotEmpty(scheduledResp.Data.Messages)
	suite.NotNil(scheduledResp.Data.Messages[0].ScheduledAt)
	suite.Equal(delayCorrelationID, scheduledResp.Data.Messages[0].CorrelationID)
}

func (suite *APITestSuite) TestListRetryEvents() {
	topic := suite.newTopic("retry-history-topic", broker.TopicTypeNormal)
	group := suite.newGroup("retry-history-group")
	tag := "retry-history-tag"
	suite.primeGroup(topic, group, tag)

	retryCorrelationID := "retry-history-cid-" + uuid.NewString()
	produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, "retry-me", client.WithCorrelationID(retryCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)

	consumeResp := suite.consumeUntil(topic, group, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	suite.Equal(produceResp.Data.ID, consumeResp.Data.Message.ID)

	nackResp, errResp, err := suite.api.NackMessage(topic, group, produceResp.Data.ID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(nackResp)
	suite.True(nackResp.Data.Nacked)

	retryResp, errResp, err := suite.api.ListMessages(topic, group, "retry", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(retryResp)
	suite.Equal("retry", retryResp.Data.State)

	foundRetry := false
	for _, msg := range retryResp.Data.Messages {
		if msg.ID == produceResp.Data.ID {
			foundRetry = true
			suite.Equal(retryCorrelationID, msg.CorrelationID)
			suite.NotNil(msg.EventAt)
			suite.NotNil(msg.ScheduledAt)
			suite.NotNil(msg.ConsumedAt)
			break
		}
	}
	suite.True(foundRetry)
}

func (suite *APITestSuite) TestListRetryOnlyShowsLatestState() {
	topic := suite.newTopic("retry-latest-topic", broker.TopicTypeNormal)
	group := suite.newGroup("retry-latest-group")
	tag := "retry-latest-tag"
	suite.primeGroup(topic, group, tag)

	suite.broker.SetRetryBackoff(10*time.Millisecond, 1, 10*time.Millisecond)
	defer suite.broker.SetRetryBackoff(1*time.Second, 2, 60*time.Second)

	produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, "retry-then-ack", client.WithCorrelationID("retry-latest-cid-"+uuid.NewString()))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)

	consumeResp := suite.consumeUntil(topic, group, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	suite.Equal(produceResp.Data.ID, consumeResp.Data.Message.ID)

	nackResp, errResp, err := suite.api.NackMessage(topic, group, produceResp.Data.ID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(nackResp)
	suite.True(nackResp.Data.Nacked)

	retryConsumeResp := suite.consumeUntil(topic, group, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	suite.Equal(produceResp.Data.ID, retryConsumeResp.Data.Message.ID)

	ackResp, errResp, err := suite.api.AckMessage(topic, group, produceResp.Data.ID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(ackResp)
	suite.True(ackResp.Data.Acked)

	retryResp, errResp, err := suite.api.ListMessages(topic, group, "retry", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(retryResp)
	for _, msg := range retryResp.Data.Messages {
		suite.NotEqual(produceResp.Data.ID, msg.ID)
	}
}

func (suite *APITestSuite) TestListDLQEvents() {
	topic := suite.newTopic("dlq-history-topic", broker.TopicTypeNormal)
	group := suite.newGroup("dlq-history-group")
	tag := "dlq-history-tag"
	suite.primeGroup(topic, group, tag)

	suite.broker.SetMaxRetry(0)
	defer suite.broker.SetMaxRetry(3)

	dlqCorrelationID := "dlq-history-cid-" + uuid.NewString()
	produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, "dlq-me", client.WithCorrelationID(dlqCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)

	consumeResp := suite.consumeUntil(topic, group, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	suite.Equal(produceResp.Data.ID, consumeResp.Data.Message.ID)

	nackResp, errResp, err := suite.api.NackMessage(topic, group, produceResp.Data.ID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(nackResp)
	suite.True(nackResp.Data.Nacked)

	dlqResp, errResp, err := suite.api.ListMessages(topic, group, "dlq", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(dlqResp)
	suite.Equal("dlq", dlqResp.Data.State)

	foundDLQ := false
	for _, msg := range dlqResp.Data.Messages {
		if msg.ID == produceResp.Data.ID {
			foundDLQ = true
			suite.Equal(dlqCorrelationID, msg.CorrelationID)
			suite.NotNil(msg.EventAt)
			suite.NotNil(msg.ConsumedAt)
			break
		}
	}
	suite.True(foundDLQ)
}

func (suite *APITestSuite) TestTerminateMessage() {
	topic := suite.newTopic("terminate-topic", broker.TopicTypeNormal)
	groupA := suite.newGroup("terminate-group-a")
	groupB := suite.newGroup("terminate-group-b")
	tag := "terminate-tag"
	cancelledCorrelationID := "cancelled-cid-" + uuid.NewString()

	produceResp, errResp, err := suite.api.ProduceMessage(topic, tag, "cancel-me", client.WithCorrelationID(cancelledCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(produceResp)

	terminateResp, errResp, err := suite.api.TerminateMessage(topic, groupA, produceResp.Data.ID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(terminateResp)
	suite.True(terminateResp.Data.Terminated)
	suite.Equal("cancelled", terminateResp.Data.State)

	terminateResp, errResp, err = suite.api.TerminateMessage(topic, groupA, produceResp.Data.ID)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(terminateResp)
	suite.True(terminateResp.Data.Terminated)

	terminateResp, errResp, err = suite.api.TerminateMessage(topic, groupA, uuid.NewString())
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(terminateResp)
	suite.True(terminateResp.Data.Terminated)

	cancelledResp, errResp, err := suite.api.ListMessages(topic, groupB, "cancelled", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(cancelledResp)
	suite.Equal("cancelled", cancelledResp.Data.State)
	foundCancelled := false
	for _, msg := range cancelledResp.Data.Messages {
		if msg.ID == produceResp.Data.ID {
			foundCancelled = true
			suite.Equal(cancelledCorrelationID, msg.CorrelationID)
			break
		}
	}
	suite.True(foundCancelled)

	pendingResp, errResp, err := suite.api.ListMessages(topic, groupA, "pending", client.WithListQueueId(0), client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(pendingResp)
	for _, msg := range pendingResp.Data.Messages {
		suite.NotEqual(produceResp.Data.ID, msg.ID)
	}

	consumeResp, errResp, err := suite.api.ConsumeMessages(topic, groupA, tag, client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)

	consumeResp, errResp, err = suite.api.ConsumeMessages(topic, groupB, tag, client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)

	activeCorrelationID := "active-cid-" + uuid.NewString()
	activeProduceResp, errResp, err := suite.api.ProduceMessage(topic, tag, "keep-me", client.WithCorrelationID(activeCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(activeProduceResp)

	activeConsumeResp := suite.consumeUntil(topic, groupB, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	suite.Equal(activeProduceResp.Data.ID, activeConsumeResp.Data.Message.ID)
	suite.Equal(activeCorrelationID, activeConsumeResp.Data.Message.CorrelationID)
}

func (suite *APITestSuite) TestTerminateBatchMessages() {
	topic := suite.newTopic("terminate-batch-topic", broker.TopicTypeNormal)
	groupA := suite.newGroup("terminate-batch-group-a")
	groupB := suite.newGroup("terminate-batch-group-b")
	tag := "terminate-batch-tag"

	firstCorrelationID := "batch-cid-" + uuid.NewString()
	secondCorrelationID := "batch-cid-" + uuid.NewString()

	firstResp, errResp, err := suite.api.ProduceMessage(topic, tag, "cancel-batch-1", client.WithCorrelationID(firstCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(firstResp)

	secondResp, errResp, err := suite.api.ProduceMessage(topic, tag, "cancel-batch-2", client.WithCorrelationID(secondCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(secondResp)

	terminateResp, errResp, err := suite.api.TerminateBatchMessages(topic, []string{firstResp.Data.ID, secondResp.Data.ID})
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(terminateResp)
	suite.Equal(2, terminateResp.Data.TerminatedCount)
	suite.ElementsMatch([]string{firstResp.Data.ID, secondResp.Data.ID}, terminateResp.Data.MessageIDs)
	suite.Equal("cancelled", terminateResp.Data.State)

	cancelledResp, errResp, err := suite.api.ListMessages(topic, groupA, "cancelled", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(cancelledResp)

	found := map[string]string{}
	for _, msg := range cancelledResp.Data.Messages {
		if msg.ID == firstResp.Data.ID || msg.ID == secondResp.Data.ID {
			found[msg.ID] = msg.CorrelationID
		}
	}
	suite.Equal(firstCorrelationID, found[firstResp.Data.ID])
	suite.Equal(secondCorrelationID, found[secondResp.Data.ID])

	consumeResp, errResp, err := suite.api.ConsumeMessages(topic, groupA, tag, client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)

	consumeResp, errResp, err = suite.api.ConsumeMessages(topic, groupB, tag, client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)
}

func (suite *APITestSuite) TestNewGroupStartsAtLatest() {
	topic := suite.newTopic("latest-start-topic", broker.TopicTypeNormal)
	group := suite.newGroup("latest-start-group")
	tag := "latest-tag"

	_, errResp, err := suite.api.ProduceMessage(topic, tag, "historical-message")
	suite.NoError(err)
	suite.Nil(errResp)

	consumeResp, errResp, err := suite.api.ConsumeMessages(topic, group, tag, client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)

	freshCorrelationID := "latest-cid-" + uuid.NewString()
	freshResp, errResp, err := suite.api.ProduceMessage(topic, tag, "fresh-message", client.WithCorrelationID(freshCorrelationID))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(freshResp)

	consumeResp = suite.consumeUntil(topic, group, tag, time.Now().Add(3*time.Second), client.WithQueueId(0))
	suite.Equal(freshResp.Data.ID, consumeResp.Data.Message.ID)
	suite.Equal(freshCorrelationID, consumeResp.Data.Message.CorrelationID)
}

func (suite *APITestSuite) TestSubscriptionConflict() {
	topic := suite.newTopic("subscription-topic", broker.TopicTypeNormal)
	group := suite.newGroup("subscription-group")

	consumeResp, errResp, err := suite.api.ConsumeMessages(topic, group, "tag-a", client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)

	consumeResp, errResp, err = suite.api.ConsumeMessages(topic, group, "tag-b", client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("subscription_conflict", errResp.Code)
}

func (suite *APITestSuite) TestExpiredMessagesAreFiltered() {
	topic := suite.newTopic("expired-topic", broker.TopicTypeNormal)
	group := suite.newGroup("expired-group")
	tag := "expired-tag"
	suite.primeGroup(topic, group, tag)

	suite.broker.SetMessageRetention(time.Second)
	suite.broker.SetMessageExpiryFactor(2)
	defer suite.broker.SetMessageRetention(7 * 24 * time.Hour)
	defer suite.broker.SetMessageExpiryFactor(2)

	oldID := uuid.NewString()
	err := suite.store.Append(topic, 0, storage.Message{
		ID:            oldID,
		Body:          "expired-message",
		Tag:           tag,
		CorrelationID: "expired-cid",
		Timestamp:     time.Now().Add(-3 * time.Second),
	})
	suite.NoError(err)

	consumeResp, errResp, err := suite.api.ConsumeMessages(topic, group, tag, client.WithQueueId(0))
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)

	expiredResp, errResp, err := suite.api.ListMessages(topic, group, "expired", client.WithListLimit(10))
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(expiredResp)
	suite.Equal("expired", expiredResp.Data.State)
	foundExpired := false
	for _, msg := range expiredResp.Data.Messages {
		if msg.ID == oldID {
			foundExpired = true
			suite.Equal("expired-cid", msg.CorrelationID)
			break
		}
	}
	suite.True(foundExpired)
}

func (suite *APITestSuite) TestGetFullStats() {
	resp, errResp, err := suite.api.GetStats()
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
}

func (suite *APITestSuite) TestGetTopicStats() {
	topicName := suite.newTopic("stats-topic", broker.TopicTypeNormal)
	_, errResp, err := suite.api.ProduceMessage(topicName, "stats-tag", "stats-body")
	suite.NoError(err)
	suite.Nil(errResp)

	resp, errResp, err := suite.api.GetTopicStats(topicName)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal("ok", resp.Code)
}

func TestAPITestSuite(t *testing.T) {
	suite.Run(t, new(APITestSuite))
}
