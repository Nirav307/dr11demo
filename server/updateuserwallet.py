import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
import decimal


def lambda_handler(event, context):
    
    
    # TODO implement
    fee1=int(event['fee'])
    name1=event['name']
    email = event['email']
    sign = event['sign']
    dynamo_db = boto3.resource('dynamodb') 
    match_table = dynamo_db.Table('dr11-userwalletdb')
    matchResponse = match_table.scan(
        FilterExpression = Attr('name').eq(name1)
        )
        
    print(matchResponse)
    currentbalance = matchResponse['Items'][0]['balance']   
    #matchResponseitem = matchResponse['Items'][0]
    #currentbalance = int(matchResponseitem['balance'])
    #sign='-'
    updatedbalance = int(currentbalance)
    if(sign == '-'):
        fee1 = -1*fee1
    if (int(currentbalance)+fee1) >= 0 :
        result = "true"
        updatedbalance = int(currentbalance) + fee1
        #print(type(decimal.Decimal(updatedbalance)))
        response = match_table.update_item(
            Key={
                'name':name1
            },
            UpdateExpression="SET balance = :val",
            ExpressionAttributeValues={
                ':val': int(updatedbalance)
            }
        )
    
    else:
        result = "false"
    
    return { 
            'statusCode': 200, 
            'headers': { 
                "Access-Control-Allow-Origin" : "*", # Required for CORS support to work 
                "Access-Control-Allow-Credentials" : True # Required for cookies, authorization headers with HTTPS  
              }, 
            'body': json.dumps({'result':result,'updatedbalance':updatedbalance} ) 
        } 

