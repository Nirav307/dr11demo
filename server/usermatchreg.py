import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
import decimal


def lambda_handler(event, context):
    # TODO implement
    matchid1=event['matchid']
    name1=event['name']
    pmatchid1=event['privateid']
    squad1=event['squad']
    mstartpoints=0
    dynamo_db = boto3.resource('dynamodb') 
    match_table = dynamo_db.Table('dr11-playerdata')
    squad1 =squad1.split(',')
    for i in range(len(squad1)):
        playerid1=squad1[i]
        #print(playerid1)
        matchResponse = match_table.scan(
            FilterExpression = Attr('playerid').eq(str(playerid1))
            )
        #print(matchResponse)
        eachpoints = matchResponse['Items'][0]['points']
        #print(eachpoints)
        mstartpoints=int(mstartpoints)+int(eachpoints)
    #print(matchResponse)
     
    
    dynamo_db1 = boto3.resource('dynamodb') 
    match_table1 = dynamo_db1.Table('dr11-usermatchhistory')  
    
    
    
    #my_matches_id=['match_ids' :  list(match_table1['Items'][0]['matchid']]
    #my_matches_id['match_ids'].append(matchid1)
    response = match_table1.put_item(
     Item={
        'name': name1,
        'UserTeamPoints': "0",
        'matchid' : matchid1,
        'moneywon' : "0",
        'privateid' : pmatchid1,
        'squad' : squad1,
        'squad_final_points':mstartpoints,
        'squad_initial_points': mstartpoints,
        'status':"future",
        'dummy':matchid1+name1+pmatchid1
        
     }
    )
    print(pmatchid1, "*************************")
    if(int(pmatchid1)==0):
        pubtable = dynamo_db.Table('dr11-publicmatch') 
        pubResponse = pubtable.scan(
            FilterExpression = Attr('MatchId').eq(matchid1)
            )
            
        newcount = int(pubResponse['Items'][0]['count1'])+1
        Response = pubtable.update_item(
        Key={
            'MatchId':matchid1
            },
            UpdateExpression="SET count1 = :val",
            ExpressionAttributeValues={
            ':val': str(newcount)
            }
            )
    else:
        pritable = dynamo_db.Table('dr11-privatematch') 
        print()
        priResponse = pritable.scan(
            FilterExpression = (Attr('MatchId').eq(matchid1) & Attr('PrivateId').eq(pmatchid1))
            )
            
        newcount = int(priResponse['Items'][0]['count1']) + 1
        Response = pritable.update_item(
        Key={
            'MatchId':matchid1,
            'PrivateId':pmatchid1
            },
            UpdateExpression="SET count1 = :val",
            ExpressionAttributeValues={
            ':val': str(newcount)
            }
            )

        
    
    return { 
            'statusCode': 200, 
            'headers': { 
                "Access-Control-Allow-Origin" : "*", # Required for CORS support to work 
                "Access-Control-Allow-Credentials" : True # Required for cookies, authorization headers with HTTPS  
              }, 
            'body': json.dumps('Hello') 
        } 

