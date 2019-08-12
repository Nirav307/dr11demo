import json
import boto3
from itertools import groupby
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    
    name = event['name']
    dynamodb = boto3.resource("dynamodb")
    match_history = dynamodb.Table('dr11-usermatchhistory') 
    response1 = match_history.scan(FilterExpression=Attr('name').eq(name))
    my_matches_record=[]
    public_table_match = dynamodb.Table('dr11-publicmatch')
    private_table_match = dynamodb.Table('dr11-privatematch')
    #print(response1)
    for i in range (response1['Count']):
        pid = int(response1['Items'][i]['privateid'])
        #print(pid)
        mid = response1['Items'][i]['matchid']
        #print(mid)
        if pid == 0:
            response2 = public_table_match.scan(FilterExpression=Attr('MatchId').eq(mid))
            print(response2)
            my_matches_record.append({
                            'Team1': response2['Items'][0]['Team'],
                            'Team2': response2['Items'][0]['Team2'], 
                            'Date' : response2['Items'][0]['Date'],
                            'Hours' : response2['Items'][0]['Hours'],
                            'Minutes' : response2['Items'][0]['Minutes'],
                            'Status': response1['Items'][i]['status'],
                            'Squad': str(response1['Items'][i]['squad']),
                            'Moneywon' : response1['Items'][i]['moneywon'],
                            'LeagueId' : response2['Items'][0]['LeagueId'],
                            'MatchId' : response2['Items'][0]['MatchId']
                            
            })
        else:
            response3 = private_table_match.scan(FilterExpression=Attr('MatchId').eq(mid))
            #print(response3)
            my_matches_record.append({
                            'Team1': response3['Items'][0]['Team1'],
                            'Team2': response3['Items'][0]['Team2'], 
                            'Date' : response3['Items'][0]['Date'],
                            'Hours' : response3['Items'][0]['Hours'],
                            'Minutes' : response3['Items'][0]['Minutes'],
                            'Status': response1['Items'][i]['status'],
                            'Squad': str(response1['Items'][i]['squad']),
                            'Moneywon' : response1['Items'][i]['moneywon'],
                            'LeagueId' : response3['Items'][0]['LeagueId'],
                            'MatchId' : response3['Items'][0]['MatchId'],
                            'PrivateId' : response3['Items'][0]['PrivateId']
                            
            })
            
        

    matches_by_status = {}
    for content in my_matches_record:
        stat = content['Status']
        if stat not in matches_by_status.keys():
            matches_by_status[stat] = []
        matches_by_status[stat].append(content)
    return matches_by_status    
    