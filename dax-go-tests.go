package main

/*
*    This project will be changed to run locally and test against DAXE
*        Note that this is not the official repo and will be used for test purposes only
 */

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"./dax"
)

/*
*  The first tests to be done are: GetItem, PutItem, DeleteItem and UpdateItem
 */

func executeGetItemRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.GetItemOutput, error) {
	// Now execute the GetItem request
	// Run it over the TestTable, pk Nome and sk Idade

	return daxClient.GetItem(nil)

	// return daxClient.GetItem(
	// 	&dynamodb.GetItemInput{
	// 		TableName: aws.String("TestTable"),
	// 		Key: map[string]*dynamodb.AttributeValue{
	// 			"PartitionKey": {S: aws.String("Nome")},
	// 			"SortKey":      {S: aws.String("Idade")},
	// 		}})
}

func executePutItemRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.PutItemOutput, error) {
	return daxClient.PutItem(nil)
	// return daxClient.PutItem(
	// 	&dynamodb.PutItemInput{
	// 		TableName: aws.String("TestTable"),
	// 		Item: map[string]*dynamodb.AttributeValue{
	// 			"PartitionKey": {S: aws.String("Nome")},
	// 			"SortKey":      {S: aws.String("Idade")},
	// 			"ItemData":     {S: aws.String("YES YES")},
	// 		},
	// 	})
}

func executeDeleteItemRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.DeleteItemOutput, error) {
	return daxClient.DeleteItem(nil)
	// return daxClient.DeleteItem(
	// 	&dynamodb.DeleteItemInput{
	// 		TableName: aws.String("TestTable"),
	// 		Key: map[string]*dynamodb.AttributeValue{
	// 			"PartitionKey": {S: aws.String("Nome")},
	// 			"SortKey":      {S: aws.String("Idade")},
	// 		},
	// 	})
}

func executeUpdateItemRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.UpdateItemOutput, error) {
	return daxClient.UpdateItem(nil)
	// return daxClient.UpdateItem(
	// 	&dynamodb.UpdateItemInput{
	// 		TableName: aws.String("TestTable"),
	// 		Key: map[string]*dynamodb.AttributeValue{
	// 			"PartitionKey": {S: aws.String("Nome")},
	// 			"SortKey":      {S: aws.String("Idade")},
	// 		},
	// 	})
}

func executeBatchGetItemRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.BatchGetItemOutput, error) {
	return nil, nil
}

func executeBatchWriteItemRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.BatchWriteItemOutput, error) {
	return nil, nil
}

func executeTransactGetItemsRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.TransactGetItemsOutput, error) {
	return nil, nil
}

func executeTransactWriteItemsRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.TransactWriteItemsOutput, error) {
	return nil, nil
}

func executeQueryRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.QueryOutput, error) {
	return nil, nil
}

func executeScanRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.ScanOutput, error) {
	return daxClient.Scan(nil)
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

	if _, err := executeGetItemRequest(client); err != nil {
		fmt.Println(err)
	}

	if _, err := executePutItemRequest(client); err != nil {
		fmt.Println(err)
	}

	if _, err := executeUpdateItemRequest(client); err != nil {
		fmt.Println(err)
	}

	if _, err := executeDeleteItemRequest(client); err != nil {
		fmt.Println(err)
	}

	if _, err := executeScanRequest(client); err != nil {
		fmt.Println(err)
	}

}
