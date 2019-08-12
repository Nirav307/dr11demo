import json
import boto3
from boto3.dynamodb.conditions import Key,Attr 

def lambda_handler(event, context):
    # TODO implement
    matchid = event['MatchId']
    #privateid = event['PrivateId']
    print(type(matchid))
    dynamodb = boto3.resource('dynamodb')
    match_table = dynamodb.Table('dr11-usermatchhistory') 
    matchResponse  = match_table.scan(
        FilterExpression = Attr('matchid').eq(matchid)
     )
    matchResponse=matchResponse["Items"]
    #print(matchResponse)
    print(len(matchResponse))
    result=[] 
    for i in range(len(matchResponse)): 
        if (matchResponse[i]['matchid'])==matchid: 
            result.append([matchResponse[i]['name'], matchResponse[i]["UserTeamPoints"]]) 
    print(result) 
    for i in range(len(result)): 
        result[i][1]=int(result[i][1]) 
    print(result)         
    result=sorted(result, key=lambda x:x[1]) 
     
    result=result[-1::-1] 
    #print(result)
    resp=[] 
    
    dict=[]
    count=1
    for i in range(len(result)): 
              curr_user=result[i][0]
              curr_score=result[i][1] 
              resp.append({ 
              'Username':result[i][0],
              'Score':result[i][1], 
              'Rank':count
          }) 
              count+=1
    return resp
    
