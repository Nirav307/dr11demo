import boto3
import json
from boto3.dynamodb.conditions import Key, Attr



def lambda_handler(event, context):
    
    
    # TODO implement
    matchid1=event['MatchId']
    name1=event['name']
    privateid1 = event['privateid']
    dynamo_db = boto3.resource('dynamodb') 
    match_table = dynamo_db.Table('dr11-usermatchhistory')
    matchResponse = match_table.scan(
        FilterExpression = (Attr('name').eq(name1) & Attr('matchid').eq(str(matchid1)) & Attr('privateid').eq(str(privateid1)) )
        )
        
    #print(matchResponse)
    squad_list = list(matchResponse['Items'][0]['squad'])
    #print(squad_list)
    for i in range(len(squad_list)):
        squad_list[i] = int(squad_list[i])
    print(squad_list)
    teampoints = matchResponse['Items'][0]['UserTeamPoints']
   
    oppResponse = match_table.scan(
        FilterExpression = (Attr('matchid').eq(matchid1) & Attr('name').ne(name1))
        )
    #oppteampoints = matchResponse['Items'][0]['UserTeamPoints']
    #oppnames = matchResponse['Items'][0]['name']
    #print(oppResponse) 
    oppResponse=oppResponse["Items"]
    result1=[] 
    for i in range(len(oppResponse)): 
            result1.append([oppResponse[i]['name'], oppResponse[i]["UserTeamPoints"]]) 
    #print(result1)
    
    
    leaderResponse  = match_table.scan(
        FilterExpression = Attr('matchid').eq(matchid1)
     )
    leaderResponse=leaderResponse["Items"]
   # print(leaderResponse)
    #print(len(leaderResponse))
    result=[] 
    for i in range(len(leaderResponse)): 
        #if (leaderResponse[i]['matchid'])==matchid: 
            result.append([leaderResponse[i]['name'], leaderResponse[i]["UserTeamPoints"]]) 
     
    for i in range(len(result)): 
        result[i][1]=int(result[i][1]) 
             
    result=sorted(result, key=lambda x:x[1]) 
     
    result=result[-1::-1] 
    resp=[] 
    #print(result) 
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
    
    #SCORE 
    dynamodb1 = boto3.resource('dynamodb') 
    match_table1 = dynamodb1.Table('dr11-currentball') 
    matchResponse1  = match_table1.scan( 
        FilterExpression = Attr('matchid').eq(str(matchid1)) 
    ) 
    print(matchResponse1)
    matchid2 = matchResponse1['Items'][0]['matchid']
    batsman = matchResponse1['Items'][0]['batsman']
    bowler = matchResponse1['Items'][0]['bowler']
    message = matchResponse1['Items'][0]['message']
    nonstriker = matchResponse1['Items'][0]['nonstriker']
    over = matchResponse1['Items'][0]['over']
    score = matchResponse1['Items'][0]['score']
    
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
    result3=[]
    result3.append({
        'matchid':matchid2,
        'Batsman':batsman,
        'Bowler':bowler,
        'Message':message,
        'Nonstriker':nonstriker,
        'Over':over,
        'Score':score
    })
    #print(result3)
    #return result
    
         
              
    #print(resp)
    finalresp=[]
    finalresp.append({
               'MySquad':squad_list,
               'MyPoints':teampoints,
               'OppPoints':result1,
               'Leaderboard':resp,
               'Score':result3
    })
    print(finalresp)
    return finalresp
    #return { 
      #      'statusCode': 200, 
      #      'headers': { 
      #          "Access-Control-Allow-Origin" : "*", # Required for CORS support to work 
       #         "Access-Control-Allow-Credentials" : True # Required for cookies, authorization headers with HTTPS  
      #        }, 
    #return finalresp
       #     }
