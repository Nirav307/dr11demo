import json
import boto3
from boto3.dynamodb.conditions import Key,Attr 



def lambda_handler(event, context):
    # TODO implement
    #print(event)
    matchid = int(event['MatchId'])
    print(type(matchid))
    dynamodb = boto3.resource('dynamodb') 
    match_table = dynamodb.Table('dr11-currentball') 
    matchResponse  = match_table.scan( 
        FilterExpression = Attr('matchid').eq(str(matchid)) 
    ) 
    print(matchResponse)
    matchid1 = matchResponse['Items'][0]['matchid']
    batsman = matchResponse['Items'][0]['batsman']
    bowler = matchResponse['Items'][0]['bowler']
    message = matchResponse['Items'][0]['message']
    nonstriker = matchResponse['Items'][0]['nonstriker']
    over = matchResponse['Items'][0]['over']
    score = matchResponse['Items'][0]['score']
    
    if batsman=="null":
        batsman=''
    if bowler=="null":
        bowler=''
    if nonstriker=="null":
        nonstriker=''
    if over=="null":
        over=''
    if score=="null":
        score=''
        
    result=[]
    result.append({
        'matchid':matchid1,
        'Batsman':batsman,
        'Bowler':bowler,
        'Message':message,
        'Nonstriker':nonstriker,
        'Over':over,
        'Score':score
    })
    print(result)
    return result
    

    
        