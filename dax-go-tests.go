package main

/*
*    This project will be changed to run locally and test against DAXE
*        Note that this is not the official repo and will be used for test purposes only
 */

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"./dax"
)

/*
*  The first tests to be done are: GetItem, PutItem, DeleteItem and UpdateItem
 */

func executeGetItemOperation(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.GetItemOutput, error) {
	// Now execute the GetItem request
	// Run it over the TestTable, pk Nome and sk Idade
	result, err := daxClient.GetItem(
		&dynamodb.GetItemInput{
			TableName: aws.String("TestTable"),
			Key: map[string]*dynamodb.AttributeValue{
				"PartitionKey": {S: aws.String("Nome")},
				"SortKey":      {S: aws.String("Idade")},
			}})

	return result, err
}

func executePutItem(daxClient dynamodbiface.DynamoDBAPI) (string, error) {
	return "", errors.New("Unimpl")
}

func executeDeleteItem(daxClient dynamodbiface.DynamoDBAPI) (string, error) {
	return "", errors.New("Unimpl")
}

func executeUpdateItem(daxClient dynamodbiface.DynamoDBAPI) (string, error) {
	return "", errors.New("Unimpl")
}

func main() {
	// Get default configuration from cluster.go, line 92
	/*
	*	defaultConfig = Config{
	*		MaxPendingConnectionPerHost: 10,
	*		ClusterUpdateInterval: time.Second*4,
	*		ClusterUpdateThreshold: time.Millisecond*125,
	*		Credentials: default,
	*   }
	 */
	cfg := dax.DefaultConfig()
	cfg.HostPorts = []string{"localhost:8001"}
	cfg.Region = "us-central-1"
	client, _ := dax.New(cfg) // Create a new cluster

	_, err := executeGetItemOperation(client)
	if err != nil {
		fmt.Println(err)
	}
}
