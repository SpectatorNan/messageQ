package client_test

import (
	"fmt"
	"testing"

	"github.com/SpectatorNan/messageQ/go/mq/broker"
	client "github.com/SpectatorNan/messageQ/go/sdk"
	"github.com/stretchr/testify/suite"
)

type APIClientSuite struct {
	suite.Suite
	api          *client.API
	topic        string
	group        string
	tag          string
	createdTopic bool
}

func (suite *APIClientSuite) SetupSuite() {
	suite.api = client.NewAPI("http://localhost:8082", "adminkey123")
	suite.api.SetAccessKey("test-access-key")
	suite.topic = "cancel1"
}

func (suite *APIClientSuite) SetupTest() {
	suffix := "1774019364255652000" // fmt.Sprintf("%d", time.Now().UnixNano())
	suite.group = "cancel1-group-" + suffix
	suite.tag = "cancel1-tag-" + suffix
	suite.topic = "cancel1"
	suite.createdTopic = false

	topic := "remote-topic-" + suffix
	resp, errResp, err := suite.api.CreateTopic(topic, broker.TopicTypeNormal, 1)
	if err == nil && errResp == nil && resp != nil {
		suite.topic = topic
		suite.createdTopic = true
	}
} //469,093,dab

func (suite *APIClientSuite) TearDownTest() {
	if !suite.createdTopic {
		return
	}
	//_, _, _ = suite.api.DelTopic(suite.topic)
}

func (suite *APIClientSuite) produceMessages(count int) []string {
	msgIDs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		body := fmt.Sprintf("hello Bob %d", i)
		resp, errResp, err := suite.api.ProduceMessage(suite.topic, suite.tag, body)
		suite.Require().NoError(err)
		suite.Require().Nil(errResp)
		suite.Require().NotNil(resp)
		msgIDs = append(msgIDs, resp.Data.ID)
	}
	return msgIDs
}

func (suite *APIClientSuite) consumeMessage() (*client.Resp[client.ConsumeMessageResponse], *client.ErrResp, error) {
	return suite.api.ConsumeMessages(suite.topic, suite.group, suite.tag)
}

func (suite *APIClientSuite) ackMessage(msgID string) {
	resp, errResp, err := suite.api.AckMessage(suite.topic, suite.group, msgID)
	suite.Require().NoError(err)
	suite.Require().Nil(errResp)
	suite.Require().NotNil(resp)
	suite.Require().True(resp.Data.Acked)
}

func (suite *APIClientSuite) TestBatchProduce() {
	msgIDs := suite.produceMessages(5)
	suite.Len(msgIDs, 5)

	seen := make(map[string]struct{}, len(msgIDs))
	for _, msgID := range msgIDs {
		suite.NotEmpty(msgID)
		_, ok := seen[msgID]
		suite.False(ok)
		seen[msgID] = struct{}{}
	}
}

/*
   api_remote_test.go:98: consumed message: {ID:019d0bd4-df3d-721e-b0c1-af9f1a86f818 Body:hello Bob 4 Tag:cancel1-tag-1774019364255652000 CorrelationID: Retry:1 Timestamp:1774019993}
   api_remote_test.go:98: consumed message: {ID:019d0bd4-df3b-757a-bf74-5029cf46c426 Body:hello Bob 0 Tag:cancel1-tag-1774019364255652000 CorrelationID: Retry:1 Timestamp:1774019993}
   api_remote_test.go:98: consumed message: {ID:019d0bd4-df3c-7c19-a15a-fa80f815ea87 Body:hello Bob 2 Tag:cancel1-tag-1774019364255652000 CorrelationID: Retry:1 Timestamp:1774019993}

*/

func (suite *APIClientSuite) TestBatchConsume1() {
	for i := 0; i < 5; i++ {
		resp, errResp, err := suite.consumeMessage()
		if err != nil {
			suite.T().Log(err)
			continue
		}
		if errResp != nil {
			suite.T().Logf("consume error: %s - %s", errResp.Code, errResp.Message)
			continue
		}
		if resp != nil {
			suite.T().Logf("consumed message: %+v", resp.Data.Message)
		}
	}
}
func (suite *APIClientSuite) TestBatchConsume() {
	msgIDs := suite.produceMessages(5)
	for _, msgID := range []string{msgIDs[1], msgIDs[3]} {
		resp, errResp, err := suite.api.TerminateMessage(suite.topic, msgID)
		suite.Require().NoError(err)
		suite.Require().Nil(errResp)
		suite.Require().NotNil(resp)
		suite.Require().True(resp.Data.Terminated)
	}

	expected := map[string]struct{}{
		msgIDs[0]: {},
		msgIDs[2]: {},
		msgIDs[4]: {},
	}
	consumed := make(map[string]struct{}, len(expected))

	for i := 0; i < len(msgIDs); i++ {
		resp, errResp, err := suite.consumeMessage()
		suite.Require().NoError(err)
		if errResp != nil {
			suite.Equal("not_found", errResp.Code)
			break
		}
		suite.Require().NotNil(resp)
		msgID := resp.Data.Message.ID
		_, ok := expected[msgID]
		suite.True(ok, "unexpected message consumed: %s", msgID)
		_, seen := consumed[msgID]
		suite.False(seen, "duplicate message consumed: %s", msgID)
		consumed[msgID] = struct{}{}
		//suite.ackMessage(msgID)
	}

	suite.Len(consumed, len(expected))
	for msgID := range expected {
		_, ok := consumed[msgID]
		suite.True(ok, "expected message not consumed: %s", msgID)
	}

	resp, errResp, err := suite.consumeMessage()
	suite.NoError(err)
	suite.Nil(resp)
	suite.NotNil(errResp)
	suite.Equal("not_found", errResp.Code)
}

func (suite *APIClientSuite) TestConsume() {
	msgIDs := suite.produceMessages(1)

	resp, errResp, err := suite.consumeMessage()
	suite.Require().NoError(err)
	suite.Require().Nil(errResp)
	suite.Require().NotNil(resp)
	suite.Equal(msgIDs[0], resp.Data.Message.ID)
	suite.ackMessage(resp.Data.Message.ID)
}

func (suite *APIClientSuite) TestProduce() {
	resp, errResp, err := suite.api.ProduceMessage(suite.topic, suite.tag, "hello Alice")
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)
	suite.Equal(suite.topic, resp.Data.Topic)
	suite.Equal(suite.tag, resp.Data.Tag)
	suite.NotEmpty(resp.Data.ID)
}

func (suite *APIClientSuite) TestTerminate() {
	msgIDs := suite.produceMessages(1)

	resp, errResp, err := suite.api.TerminateMessage(suite.topic, msgIDs[0])
	suite.Require().NoError(err)
	suite.Require().Nil(errResp)
	suite.Require().NotNil(resp)
	suite.Require().True(resp.Data.Terminated)

	consumeResp, consumeErrResp, err := suite.consumeMessage()
	suite.NoError(err)
	suite.Nil(consumeResp)
	suite.NotNil(consumeErrResp)
	suite.Equal("not_found", consumeErrResp.Code)
}

func (suite *APIClientSuite) TestListTopics() {

	resp, errResp, err := suite.api.ListTopics()
	suite.NoError(err)
	suite.Nil(errResp)
	suite.NotNil(resp)

}

func TestRemoteAPISuite(t *testing.T) {
	suite.Run(t, new(APIClientSuite))
}
