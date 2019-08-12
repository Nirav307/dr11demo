import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    match_history = dynamodb.Table('dr11-publicmatch') 
    response = match_history.scan()
    all_matches_record=[]
    for i in range (response['Count']):
        date = response['Items'][i]['Date']
        datesplit = date.split('/')
        year =int(datesplit[2])
        month = int(datesplit[1])
        day = int(datesplit[0])
        t = datetime(year, month, day, int(response['Items'][i]['Hours']), int(response['Items'][i]['Minutes']), 0, 0)
        now = datetime.now()
        seconds = (t-now).total_seconds()
        print(seconds)
        timelimit = True
        if(seconds<3600):
            timelimit =False

        if(response['Items'][i]['Status']=='active' and timelimit):
            all_matches_record.append({
                            'Team1': response['Items'][i]['Team'],
                            'Team2': response['Items'][i]['Team2'],
                            'Hours' : response['Items'][i]['Hours'],
                            'Minutes' : response['Items'][i]['Minutes'],
                            'LeagueId' : response['Items'][i]['LeagueId'],
                            'MatchId' : response['Items'][i]['MatchId'],
                            'PrivateId' : str(0)
                            
            })

    all_matches_json={}
    all_matches_json['AllMatches']=all_matches_record  
   
    return all_matches_json
    
    
    
    
    

