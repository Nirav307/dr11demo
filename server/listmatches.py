import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
#import datetime
from datetime import datetime




#use username if he can signup or not



def lambda_handler(event, context):
    username=event['username']
    
    dynamodb = boto3.resource("dynamodb")
    user_history = dynamodb.Table('dr11-usermatchhistory') 
    user_matches = user_history.scan(
            FilterExpression = (Attr('name').eq(str(username)))
            )
    
    user_matches = user_matches['Items']
    midplusprid = []
    mid = []
    for match in user_matches:
        if(int(match['privateid']) == 0):
            mid.append(match['matchid'])
        else:
            midplusprid.append(match['matchid']+match['privateid'])
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
            print(now)
            seconds = (t-now).total_seconds()
            print(seconds, response['Items'][i]['MatchId'])
            timelimit = True
            if(seconds<3600):
                timelimit =False
            if(response['Items'][i]['MatchId'] in mid):#if already reistered
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
    
    


    match_history = dynamodb.Table('dr11-privatematch') 
    response = match_history.scan()
    print(response)
    for i in range(response['Count']):
        t = datetime(year, month, day, int(response['Items'][i]['Hours']), int(response['Items'][i]['Minutes']), 0, 0)
        now = datetime.now()
        seconds = (t-now).total_seconds()
        print(seconds)
        timelimit = True
        if(seconds<3600):
            timelimit =False
        if(response['Items'][i]['MatchId'] + response['Items'][i]['PrivateId'] in midplusprid):#if already reistered
            timelimit =False
        if(timelimit and response['Items'][i]['Status']=='active' and (int(response['Items'][i]['limit']) > int(response['Items'][i]['count1']))):
            all_matches_record.append({
                            'Team1': response['Items'][i]['Team1'],
                            'Team2': response['Items'][i]['Team2'],
                            'Hours' : response['Items'][i]['Hours'],
                            'Minutes' : response['Items'][i]['Minutes'],
                            'LeagueId' : response['Items'][i]['LeagueId'],
                            'MatchId' : response['Items'][i]['MatchId'],
                            'PrivateId' : response['Items'][i]['PrivateId']
                            
            })
        
    all_matches_json={}
    all_matches_json['AllMatches']=all_matches_record  
   
    return all_matches_json
    
    
    
    
    

