package client

import (
	"messageQ/mq/broker"
	"testing"

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
	topicName := "test-topic"
	topicType := broker.TopicTypeNormal
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
	topicName := "test-topic"
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
	topicName := "test-topic"
	_, errResp, err := suite.api.DelTopic(topicName)
	suite.NoError(err)
	suite.Nil(errResp)
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
	suite.Nil(errResp)
	suite.NotNil(consumeResp)
	suite.Equal("ok", consumeResp.Code)
	suite.NotNil(consumeResp.Data)
}

func TestAPITestSuite(t *testing.T) {
	suite.Run(t, new(APITestSuite))
}
