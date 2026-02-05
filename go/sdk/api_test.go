package client

import (
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

func TestAPITestSuite(t *testing.T) {
	suite.Run(t, new(APITestSuite))
}
