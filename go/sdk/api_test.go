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

func TestAPITestSuite(t *testing.T) {
	suite.Run(t, new(APITestSuite))
}
