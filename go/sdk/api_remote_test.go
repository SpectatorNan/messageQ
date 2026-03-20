package client_test

import (
	"fmt"
	"testing"

	client "github.com/SpectatorNan/messageQ/go/sdk"
	"github.com/stretchr/testify/suite"
)

type APIClientSuite struct {
	suite.Suite
	api   *client.API
	topic string
	group string
	tag   string
}

func (suite *APIClientSuite) SetupSuite() {
	suite.topic = "cancel1"
	suite.group = "cancel1-group"
	suite.tag = "cancel1-tag"
	suite.api = client.NewAPI("http://localhost:8081", "adminkey123")
	suite.api.SetAccessKey("test-access-key")
}

func (suite *APIClientSuite) TearDownSuite() {}

func (suite *APIClientSuite) TestBatchProduce() {
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("hello Bob %d", i)
		resp, errResp, err := suite.api.ProduceMessage(suite.topic, suite.tag, body)
		suite.NoError(err)
		suite.Nil(errResp)
		suite.T().Log(resp)
	}
}

func (suite *APIClientSuite) TestProduce() {
	body := "hello Alice"
	resp, errResp, err := suite.api.ProduceMessage(suite.topic, suite.tag, body)
	suite.NoError(err)
	suite.Nil(errResp)
	suite.T().Log(resp)
}

func (suite *APIClientSuite) TestConsume() {
	topic := suite.topic
	group := suite.group
	tag := suite.tag

	resp, errResp, err := suite.api.ConsumeMessages(topic, group, tag)
	suite.NoError(err)
	if errResp != nil {
		suite.T().Fatalf("consume messages API error: %+v", errResp)
		return
	}
	suite.Equal("019d0b43-0c96-7167-85c9-0464b116ae2c", resp.Data.Message.ID)
	suite.T().Logf("consume success: %+v", resp)
}

func TestRemoteAPISuite(t *testing.T) {
	suite.Run(t, new(APIClientSuite))
}
