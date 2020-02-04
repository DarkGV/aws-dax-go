package main

/*
*    This project will be changed to run locally and test against DAXE
*        Note that this is not the official repo and will be used for test purposes only
 */

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"./dax"
)

/*
*  The first tests to be done are: GetItem, PutItem, DeleteItem and UpdateItem
 */

func executeGetItemOperation(daxClient dynamodbiface.DynamoDBAPI) (string, error) {
	return "", errors.New("Unimpl")
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
	fmt.Println("Example")
	cfg := dax.DefaultConfig()
	client, _ := dax.New(cfg)
	_, err := executeGetItemOperation(client)
	if err != nil {
		fmt.Println(err)
	}
}
