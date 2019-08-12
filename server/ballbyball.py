import json
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
#decide prize based on number of players



def lambda_handler(event, context):
    # TODO implement
    matchid=event['matchid']
    time = event['time']
    moneywon=0
    
    dynamo_db = boto3.resource('dynamodb') 
    score_table = dynamo_db.Table('dr11-scoredb')
    score = score_table.scan(
            FilterExpression = (Attr('matchid').eq(str(matchid)) & Attr('time').eq(str(time)))
            )
    #print(score)
    if(int(score['Count'])>0):#match is still happeing
        score_response = score['Items'][0]
        #print(score_response)
        
        #Adding new ball to the current ball database
        current_score_table = dynamo_db.Table('dr11-currentball')
        response = current_score_table.put_item(
         Item={   
            'matchid':score_response['matchid'] ,
            'nonstriker': score_response['nonstriker'],
            'bowler' : score_response['bowler'],
            'over' : score_response['over'],
            'score' : score_response['score'],
            'batsman' : score_response['batsman'],
            'message' : score_response['message'],
         }
        )
        
        
        #update player points
        if(score_response['pid']!='null'):
            player_table = dynamo_db.Table('dr11-playerdata')
            
            myplayer  = player_table.scan(
                FilterExpression = Attr('playerid').eq(str(score_response['pid']))
            )
            updatedpoints = str(int(myplayer['Items'][0]['points']) + int(score_response['points']))
            #print(updatedpoints,score_response['points'])
            response = player_table.update_item(
                Key={
                    'playerid':str(score_response['pid'])
                },
                UpdateExpression="SET points = :val",
                ExpressionAttributeValues={
                    ':val': str(updatedpoints)
                }
            )
            print
            #find users with the matchid and having that player id in the squad list and recompute the score
            user_details = dynamo_db.Table('dr11-usermatchhistory') 
            userhistory  = user_details.scan(
                FilterExpression = Attr('matchid').eq(matchid)
            )
            userhistory=userhistory["Items"]
            print(userhistory)
            for i in range(len(userhistory)):
                #print(i,userhistory)
                if(score_response['pid'] in userhistory[i]['squad']):
                    
                    response = user_details.update_item(
                        Key={
                            'dummy':userhistory[i]['matchid']+userhistory[i]['name']+userhistory[i]['privateid']
                        },
                        UpdateExpression="SET squad_final_points = :val",
                        ExpressionAttributeValues={
                            ':val': str(int(userhistory[i]['squad_final_points'])+int(score_response['points']))
                        }
                    )
                    #print(response)
                    response = user_details.update_item(
                        Key={
                            'dummy':userhistory[i]['matchid']+userhistory[i]['name']+userhistory[i]['privateid']
                        },
                        UpdateExpression="SET UserTeamPoints = :val",
                        ExpressionAttributeValues={
                            ':val': str(int(userhistory[i]['UserTeamPoints'])+int(score_response['points']))
                        }
                    )

    else:
        #after match code
        
        #update wallet for all those who played with the match id
        match_table = dynamo_db.Table('dr11-usermatchhistory') 
        matchResponse  = match_table.scan(
            FilterExpression = Attr('matchid').eq(matchid)
        )
        matchResponse=matchResponse["Items"]
        private_match_ids = []
        for i in range(len(matchResponse)): 
            private_match_ids.append(matchResponse[i]['privateid'])
        private_match_ids = list(set(private_match_ids))
        updatedbalance=0
        result=[] 
        
        for prid in private_match_ids:
            #public match inactive
            if(int(prid) == 0):
                pubtable = dynamo_db.Table('dr11-publicmatch')
                Response_pub = pubtable.update_item(
                    Key={
                            'matchid':matchid
                        },
                    UpdateExpression="SET Status = :val",
                    ExpressionAttributeValues={
                        ':val': "inactive"
                    }
                    )
                
            else:
                #remove from private table
                pritable = dynamo_db.Table('dr11-privatematch')
                Response_pri     = pubtable.update_item(
                    Key={
                            'matchid':matchid
                        },
                    UpdateExpression="SET Status = :val",
                    ExpressionAttributeValues={
                        ':val': "inactive"
                    }
                    )


                
            for i in range(len(matchResponse)):
                if(matchResponse[i]['privateid']==prid):
                    result.append([matchResponse[i]['name'], matchResponse[i]['UserTeamPoints'],matchResponse[i]['privateid'],matchResponse[i]['status']]) 
            
    
            for i in range(len(result)): 
                result[i][1]=int(result[i][1]) 
                result[i][2]=int(result[i][2]) 
            result=sorted(result, key=lambda x:x[1]) 
            result=result[-1::-1] 
            count=len(result)
            if count>10:
                count=10
            match_table1 = dynamo_db.Table('dr11-userwalletdb')
            
            
            #here decide the prize based on the count and the registration fee
            
            prize = {0: 1000,1: 700,2: 500,3: 100,4: 100,5: 100,6: 100,7: 100,8: 100,9: 100}
            print(result)
            for i in range(count-1):
                if(matchResponse[i]['status']!='completed'):
                    user_name=result[i][0] #taking the top rankers usernames in order
                    match_table1 = dynamo_db.Table('dr11-userwalletdb')
                    userResponse = match_table1.scan(
                    FilterExpression = Attr('name').eq(user_name)
                        )
                        
                    print(userResponse)
                    balance=userResponse['Items'][0]['balance']
                    email=userResponse['Items'][0]['email']
                    moneywon = prize[i]
                    updatedbalance=int(balance)+int(moneywon)
                    #print(updatedbalance)
                    Response = match_table1.update_item(
                    Key={
                            'name':user_name
                        },
                    UpdateExpression="SET balance = :val",
                    ExpressionAttributeValues={
                        ':val': str(updatedbalance)
                    }
                    )
        
            table = dynamo_db.Table('dr11-usermatchhistory')
        
            for i in range(len(result)-1):
                if(matchResponse[i]['status']!='completed'):
                    curr_user=result[i][0]
                    randompriv=str(result[i][2])
                    name1=curr_user
                    pmatchid1=randompriv
                    dummyi=matchid+name1+pmatchid1
                    Response = table.update_item(
                        Key={
                                'dummy':matchid+name1+pmatchid1
                            },
                        UpdateExpression='SET #ts = :val1',
                        ExpressionAttributeValues={
                            ":val1": "completed"
                        },
                        ExpressionAttributeNames={
                            "#ts": "status"
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