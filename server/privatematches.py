import json
import boto3
from boto3.dynamodb.conditions import Key,Attr 
import random

#create pivate id using random function
#add the column called limit while adding it to the database

def lambda_handler(event, context):
    # TODO implement
    #print(event)
    matchid = event['MatchId']
    registeration_fee = event['Fee']
    limit_on_players  = event['limit'] #To set the limit on the number of players
    
    #print(type(matchid))
    dynamodb = boto3.resource('dynamodb') 
    match_table = dynamodb.Table('dr11-publicmatch') 
    matchResponse  = match_table.scan( 
        FilterExpression = Attr('MatchId').eq(str(matchid)) 
    ) 
    Date =  matchResponse['Items'][0]['Date']
    Hours =  matchResponse['Items'][0]['Hours']
    Minutes =  matchResponse['Items'][0]['Minutes']
    team1 =  matchResponse['Items'][0]['Team'] 
    team2 =  matchResponse['Items'][0]['Team2'] 
    leagueid = matchResponse['Items'][0]['LeagueId'] 
    status = matchResponse['Items'][0]['Status'] 
    count = 0
    team1_list =  matchResponse['Items'][0]['Team1 Players']
    myteam1 = set()
    for i in range(len(team1_list)):
        myteam1.add(team1_list[i])
            
    team2_list =  matchResponse['Items'][0]['Team2 Players']
    myteam2 = set()
    for i in range(len(team2_list)):
        myteam2.add(team2_list[i])
        
    print(myteam1)
    print(myteam2)
    #print(matchResponse)
   
    dynamodb1 = boto3.resource('dynamodb') 
    match_table1 = dynamodb1.Table('dr11-privatematch') 
    LeagueId = leagueid
    MatchId = matchid
    Fee = registeration_fee
    Team1 = team1
    Team2 = team2
    #new_list = old_list.copy()
    #new_list = list(old_list)
    #Team1_Players = []
    #Team1_Players = team1_list.copy()
    #print(Team1_Players)
    #Team2_Players = []
    #Team2_Players = team2_list.copy()
    #print(Team2_Players)
    priid = str(random.randint(1, 10000))
    
    response = match_table1.put_item(
     Item={
        'LeagueId': LeagueId,
        'MatchId': MatchId,
        'Fee' : Fee,
        'Team1' : Team1,
        'Team2' : Team2,
        'Status':status,
        'count1':str(count),
        'limit':limit_on_players,
        'PrivateId':priid,
        'Date':Date,
        'Hours':Hours,
        'Minutes':Minutes

    }
   )
    print(response)
    return {
        'statusCode': 200,
        'body': json.dumps('')
    }
