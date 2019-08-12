import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
import decimal


def lambda_handler(event, context):
    # TODO implement
    name1=event['name']
    email = event['email']
    dynamo_db = boto3.resource('dynamodb') 
    match_table = dynamo_db.Table('dr11-userwalletdb')
    matchResponse = match_table.scan(
        FilterExpression = Attr('name').eq(name1) 
        )
        
    print(matchResponse)
    # result=[]
    # result.append({ 
    #     'current1balance' : matchResponse['Items'][0]['balance'],
    #     'mobile11' :  str(matchResponse['Items'][0]['mobile']),
    #     'dob11' :  matchResponse['Items'][0]['dob'] 
    #   })
    # return result
    currentbalance = matchResponse['Items'][0]['balance'] 
    mobile1 =  str(matchResponse['Items'][0]['mobile']) 
    dob1 =  matchResponse['Items'][0]['dob'] 
    
    return { 
            'statusCode': 200, 
            'headers': { 
                "Access-Control-Allow-Origin" : "*", # Required for CORS support to work 
                "Access-Control-Allow-Credentials" : True # Required for cookies, authorization headers with HTTPS  
              }, 
            'body': json.dumps({'name': name1,'email': email,'dob' : dob1,'mobile' : mobile1,'balance' :currentbalance})
            }
