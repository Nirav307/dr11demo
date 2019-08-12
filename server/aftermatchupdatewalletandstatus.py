import json
import boto3
from boto3.dynamodb.conditions import Key,Attr 

def lambda_handler(event, context):
    # TODO implement
    matchid1 = event['MatchId']
    moneywon=0
    #privateid = event['PrivateId']
    print(type(matchid1))
    
    #LEADERBOARD CODE 
    dynamodb = boto3.resource('dynamodb')
    match_table = dynamodb.Table('dr11-usermatchhistory') 
    matchResponse  = match_table.scan(
        FilterExpression = Attr('matchid').eq(matchid1)
     )
    #print(matchResponse)
    matchResponse=matchResponse["Items"]
    #print(matchResponse)
    print(len(matchResponse))
    
    updatedbalance=0
    result=[] 
    for i in range(len(matchResponse)): 
        #result.append([matchResponse[i]['name'], matchResponse[i]["UserTeamPoints"]])
        result.append([matchResponse[i]['name'], matchResponse[i]['UserTeamPoints'],matchResponse[i]['privateid']]) 
    print(result)   #just as above line appended order
    
    
    for i in range(len(result)): 
        result[i][1]=int(result[i][1]) 
        result[i][2]=int(result[i][2]) 
        
    
    print(result)    # proper order name,int type userteampoints,int type privateid
    
    
    result=sorted(result, key=lambda x:x[1]) 
    
    print(result)  #players in reverse ranking order
    
    # for i in range(len(result)): 
    #     result[i][1]=int(result[i][1]) 
    # #print(result)         
    # result=sorted(result, key=lambda x:x[1]) 
     
    result=result[-1::-1] 
    resp=[] 
    print(result)   # players in ranking order
    
    
    dict=[]
    count=1
    for i in range(len(result)): 
              curr_user=result[i][0]
              curr_score=result[i][1] 
              resp.append({ 
              'Username':result[i][0],
              'Score':result[i][1], 
              'Rank':count,
              'privateid':result[i][2]
          }) 
              count+=1
    #return resp
    
    #UPDATING THE WALLET OF TOP 10 PLAYERS after the completion of match based on leaderboard of the match.
    totalcount=count
    if count>10:
        count=10
    dynamo_db = boto3.resource('dynamodb')
    match_table1 = dynamo_db.Table('dr11-userwalletdb')
    
    for i in range(count-1):
        user_name=result[i][0] #taking the top rankers usernames in order
        match_table1 = dynamo_db.Table('dr11-userwalletdb')
        userResponse = match_table1.scan(
        FilterExpression = Attr('name').eq(user_name)
            )
        balance=userResponse['Items'][0]['balance']
        email=userResponse['Items'][0]['email']
        #print(balance)
        
        prize = {
        0: 10,
        1: 7,
        2: 5,
        3: 1,
        4: 1,
        5: 1,
        6: 1,
        7: 1,
        8: 1,
        9: 1,
           
        }
        moneywon = prize[i]
        updatedbalance=int(balance)+int(moneywon)
        #print(updatedbalance)
        Response = match_table1.update_item(
        Key={
                'name':user_name,
                #'email' : email
            },
        UpdateExpression="SET balance = :val",
        ExpressionAttributeValues={
            ':val': str(updatedbalance)
        }
        )
        
    #CODE FOR STATUS UPDATE for all matches in usermatchhhistory database
    
   
    
    # Responseofmatch  = table.scan(
    #     FilterExpression = Attr('matchid').eq(matchid1)
    #  )
    # #print(Responseofmatch)
    
    #Responseofmatch=Responseofmatch["Items"]
    # Response = table.update_item(
    #     Key={
                
    #             'matchid':matchid1
    #         },
    #     UpdateExpression='SET #ts = :val1',
    #     ExpressionAttributeValues={
    #         ":val1": "completed"
    #     },
    #     ExpressionAttributeNames={
    #         "#ts": "status"
    #     }
    #     )
    dynamodb1 = boto3.resource('dynamodb')
    table = dynamodb1.Table('dr11-usermatchhistory')
    
    for i in range(totalcount-1):
        #print(i)
    #for i in range(len(result)): 
        curr_user=result[i][0]
        #print(curr_user)
        randompriv=str(result[i][2])
        name1=curr_user
        pmatchid1=randompriv
        dummyi=matchid1+name1+pmatchid1
        print(dummyi)
        Response = table.update_item(
            Key={
                    'dummy':matchid1+name1+pmatchid1
                },
            UpdateExpression='SET #ts = :val1',
            ExpressionAttributeValues={
                ":val1": "completed"
            },
            ExpressionAttributeNames={
                "#ts": "status"
            }
            )
    