import boto3
import json
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):
    # TODO implement
    matchid1=str(event['matchid'])
    name1=str(event['name'])
    privateid1 = str(event['privateid'])
    dynamo_db = boto3.resource('dynamodb') 
    match_table = dynamo_db.Table('dr11-usermatchhistory')
    oppResponse = match_table.scan(
        FilterExpression = (Attr('privateid').eq(str(privateid1)) & Attr('matchid').eq(str(matchid1)) & Attr('name').ne(str(name1)))
                         )
    #oppteampoints = matchResponse['Items'][0]['UserTeamPoints']
    #oppnames = matchResponse['Items'][0]['name']
    print(oppResponse) 
    #squad_list = list(oppResponse['Items'][0]['squad'])
    #print(squad_list)
    #for i in range(len(squad_list)):
    #    squad_list[i] = int(squad_list[i])
    #print(squad_list)
    oppResponse=oppResponse["Items"]
    result1=[] 
    for i in range(len(oppResponse)): 
            result1.append([oppResponse[i]['name'],list(oppResponse[i]['squad']), oppResponse[i]["UserTeamPoints"]]) 
    print(result1)
    """" finalresp=[]
    for i in range(len(oppResponse)):
       finalresp.append({
          'Points':result1,
          'Squad':squad_list[i]
    })
    """
    return result1
    
    
        