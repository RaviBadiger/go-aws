package user

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	ErrorFailedToFetchRecord ="Failed to fetch record"
	ErrorFailedToUnMarshalRecord = "Failed to Unmarshal"
	ErrorInvalidUserData = "Invalid User Data"
	ErrorInvalidEmail ="Invalid Email"
	ErrorCouldNotMarshalItem = "Could Not Marshal Item"
	ErrorCouldNotDeleteItem = "Could NotDelete Item"
	ErrorUserAlreadyExist = "user.User already exits"
	ErrorUserDoesNotExist = "user.User does not exist"

)

type User struct{
	Email string `json:"email"`
	FirstName string `json:"firstName"`
	LastName string `json:"lastName"`
}

func FetchUser(email, tableName string, dynaClient dynamodbiface.DynamoDBAPI)(*User,error ){
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"email":{
				S: aws.String(email),
			},
		},
		TableName: aws.String(tableName),
	}
	result, err := dynaClient.GetItem(input)
	if err != nil{
		return nil, errors.New(ErrorFailedToFetchRecord)
	}
	item := new(User)
	err = dynamodbattribute.UnmarshalMap(result.Item, item)
	if( err != nil ){
		return nil, errors.New(ErrorFailedToUnMarshalRecord)
	}
	return item,nil
}

func FetchUsers(tableName string, dynaClient dynamodbiface.DynamoDBAPI)(*[]User,error ){
	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}
	result, err := dynaClient.Scan(input)
	if err != nil {
		return nil, errors.New(ErrorFailedToFetchRecord)
	}
	items :=new([]User)
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items,items)	
	if( err != nil ){
		return nil, errors.New(ErrorFailedToFetchRecord)
	}
	return items,nil
}

func CreateUser(req events.APIGatewayProxyRequest,
	 tableName string, 
	 dynaClient dynamodbiface.DynamoDBAPI)(*User, error){
	var u User
	if err := json.Unmarshal([]byte(req.Body),&u); err != nil {
		return nil, errors.New(ErrorInvalidUserData)
	}
	currentUser, _ := FetchUser(u.Email,tableName,dynaClient)
	if currentUser != nil && len(currentUser.Email)!=0 {
		return nil, errors.New(ErrorUserAlreadyExist)
	}
	av , err := dynamodbattribute.MarshalMap(u)
	if err != nil {
		return nil, errors.New(ErrorCouldNotMarshalItem)

	}	
	input := &dynamodb.PutItemInput{
		Item: av,
		TableName: aws.String(tableName),
	}
	_, err = dynaClient.PutItem(input)
	if err != nil {
		return nil, errors.New("Could not Put Item")
	}
	return &u,nil
}

func UpdateUser(req events.APIGatewayProxyRequest,tableName string, dynaClient dynamodbiface.DynamoDBAPI)(*User, error){
	var u User
	if err := json.Unmarshal([]byte(req.Body),&u); err!= nil{
		return nil, errors.New(ErrorFailedToUnMarshalRecord)
	}

	currentUser,_ := FetchUser(u.Email,tableName,dynaClient)
	if currentUser != nil && len(currentUser.Email) == 0{
		return nil, errors.New(ErrorUserDoesNotExist)

	}

	av, err := dynamodbattribute.MarshalMap(currentUser)
	if err != nil {
		return nil, errors.New(ErrorCouldNotMarshalItem)		
	}
	input := &dynamodb.PutItemInput{
		Item: av,
		TableName: aws.String(tableName),
	}
	_, err = dynaClient.PutItem(input)
	if err != nil {
		return nil, errors.New("Could not Put Item")
	}
	return &u,nil
}

func DeleteUser(req events.APIGatewayProxyRequest, tableName string, dynaClient dynamodbiface.DynamoDBAPI)error{
	email := req.QueryStringParameters["email"]

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"email":{
				S: aws.String(email),
			},
		},
		TableName: aws.String(tableName),
	}
	_, err := dynaClient.DeleteItem(input)
	if err != nil {
		return errors.New(ErrorCouldNotDeleteItem)
	}
	return nil
}