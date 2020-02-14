package main

/*
*    This project will be changed to run locally and test against DAXE
*        Note that this is not the official repo and will be used for test purposes only
 */

import (
	"fmt"
	"os"

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
	return daxClient.BatchGetItem(nil)
}

func executeBatchWriteItemRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.BatchWriteItemOutput, error) {
	return daxClient.BatchWriteItem(nil)
}

func executeTransactGetItemsRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.TransactGetItemsOutput, error) {
	return daxClient.TransactGetItems(nil)
}

func executeTransactWriteItemsRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.TransactWriteItemsOutput, error) {
	return daxClient.TransactWriteItems(nil)
}

func executeQueryRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.QueryOutput, error) {
	return daxClient.Query(nil)
}

func executeScanRequest(daxClient dynamodbiface.DynamoDBAPI) (*dynamodb.ScanOutput, error) {
	return daxClient.Scan(nil)
}

func main() {
	erlang_file_header :=
		`%% This is an auto-generated file.
%% Please do not modify this file unless you know exactly what you are doing
%% To generate your own tests, please check the README file
-module(daxe_acceptance_tests_SUITE).

-compile(export_all).

-spec all()
		-> [atom()].
all() ->
	%% This function will return a list containing only one function
	[list_check].

execute_test({TestName, get_item, [TableName, Keys, Opts], Output}) ->
	Result = list_to_binary(daxe_requests:daxe_get_item_263244906_packet_creator(TableName, daxe_ddb2:dynamize_item(Keys), Opts)),
	case binary:match(Output, Result) of
		{0, Value} when Value =:= size(Result) ->
			io:fwrite("{~p, ok}", [TestName]),
			ok;
		_ ->
			io:fwrite("{TestName, Result, Output} = ~n{~p, ~n~p, ~n~p}", [TestName, Result, Output]),
			erlang:error("Go client could not match Erlang client. Check log for more information.")
	end;
execute_test({TestName, put_item, [TableName, Keys, Opts], NonKeyAttributes, ID,  Output}) ->
	%% It is needed to change the input for this function when the operation is the put_item
	Result = list_to_binary(daxe_requests:daxe_put_item_N2106490455_packet_creator(TableName, daxe_ddb2:dynamize_item(Keys), NonKeyAttributes, ID, Opts)),
	case binary:match(Output, Result) of
		{0, Value} when Value =:= size(Result) ->
			io:fwrite("{~p, ok}", [TestName]),
			ok;
		_ ->
			io:fwrite("{TestName, Result, Output} = ~n{~p, ~n~p, ~n~p}", [TestName, Result, Output]),
			erlang:error("Go client could not match Erlang client. Check log for more information.")
	end;
execute_test({TestName, update_item, [TableName, Keys, Opts],  Output}) ->
	%% It is needed to change the input for this function when the operation is the put_item
	Result = list_to_binary(daxe_requests:daxe_update_item_1425579023_packet_creator(TableName, daxe_ddb2:dynamize_item(Keys), Opts)),
	case binary:match(Output, Result) of
		{0, Value} when Value =:= size(Result) ->
			io:fwrite("{~p, ok}", [TestName]),
			ok;
		_ ->
			io:fwrite("{TestName, Result, Output} = ~n{~p, ~n~p, ~n~p}", [TestName, Result, Output]),
			erlang:error("Go client could not match Erlang client. Check log for more information.")
	end;
execute_test({TestName, delete_item, [TableName, Keys, Opts],  Output}) ->
	%% It is needed to change the input for this function when the operation is the put_item
	Result = list_to_binary(daxe_requests:daxe_delete_item_1013539361_packet_creator(TableName, daxe_ddb2:dynamize_item(Keys), Opts)),
	case binary:match(Output, Result) of
		{0, Value} when Value =:= size(Result) ->
			io:fwrite("{~p, ok}", [TestName]),
			ok;
		_ ->
			io:fwrite("{TestName, Result, Output} = ~n{~p, ~n~p, ~n~p}", [TestName, Result, Output]),
			erlang:error("Go client could not match Erlang client. Check log for more information.")
	end.

-spec list_check(Config::list())
		-> ok | no_return().
list_check(_Config) ->
	lists:foreach(
		fun execute_test/1,
		acceptance_tests()
	).

-spec acceptance_tests() 
		-> [{TestName::atom(), RequestName::atom(), ErlcloudFormattedInput::[binary() | [{binary(), {atom(), binary() | integer()}}] | [any()]], Output::binary()}].
acceptance_tests() -> 
	[
		`
	cfg := dax.DefaultConfig()
	cfg.HostPorts = []string{"localhost:8001"}
	cfg.Region = "us-central-1"
	client, _ := dax.New(cfg) // Create a new cluster

	if f, err := os.Create("daxe_acceptance_tests_SUITE.erl"); err != nil { // create a file qwith the acceptance tests
		panic(err)
	} else {
		// Define the header of the file here.
		if _, err := f.Write([]byte(erlang_file_header)); err != nil {
			f.Close()
			panic(err)
		}
		f.Close()
	}

	if _, err := executeGetItemRequest(client); err != nil {
		fmt.Println(err)
	}

	if f, err := os.OpenFile("daxe_acceptance_tests_SUITE.erl", os.O_APPEND|os.O_WRONLY, 0666); err == nil {
		f.Write([]byte(",\n\t\t"))
		f.Close()
	}

	if _, err := executePutItemRequest(client); err != nil {
		fmt.Println(err)
	}

	if f, err := os.OpenFile("daxe_acceptance_tests_SUITE.erl", os.O_APPEND|os.O_WRONLY, 0666); err == nil {
		f.Write([]byte(",\n\t\t"))
		f.Close()
	}

	if _, err := executeUpdateItemRequest(client); err != nil {
		fmt.Println(err)
	}

	if f, err := os.OpenFile("daxe_acceptance_tests_SUITE.erl", os.O_APPEND|os.O_WRONLY, 0666); err == nil {
		f.Write([]byte(",\n\t\t"))
		f.Close()
	}

	if _, err := executeDeleteItemRequest(client); err != nil {
		fmt.Println(err)
	}

	// if _, err := executeScanRequest(client); err != nil {
	// 	fmt.Println(err)
	// }

	// if _, err := executeQueryRequest(client); err != nil {
	// 	fmt.Println(err)
	// }

	// if _, err := executeTransactWriteItemsRequest(client); err != nil {
	// 	fmt.Println(err)
	// }

	// if _, err := executeTransactGetItemsRequest(client); err != nil {
	// 	fmt.Println(err)
	// }

	// if _, err := executeBatchGetItemRequest(client); err != nil {
	// 	fmt.Println(err)
	// }

	// if _, err := executeBatchWriteItemRequest(client); err != nil {
	// 	fmt.Println(err)
	// }

	if f, err := os.OpenFile("daxe_acceptance_tests_SUITE.erl", os.O_APPEND|os.O_WRONLY, 0666); err == nil {
		f.Write([]byte("\n\t]."))
		f.Close()
	}

}
