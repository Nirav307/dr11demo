import json
import os
import json
import boto3
from os import walk
import traceback
import logging
from boto3.dynamodb.conditions import Key, Attr

def player2pid(player, player_to_pid):
    return player_to_pid[player]

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('bucket-data-prashanth')  
    for s3_object in my_bucket.objects.all():
        #print(s3_object)
        path, filename = os.path.split(s3_object.key)
        #print(filename,"^^^^^^^^^^")
        my_bucket.download_file(s3_object.key, '/tmp/' + filename)
    
    #s3 = boto3.resource('s3')
    #bucket = s3.Bucket('bucket-data-prashanth')
    #obj = bucket.Object('data')

    #with open('filename', 'wb') as data:
    #    obj.download_fileobj(data)
    
    #download_s3_bucket(bucket_name = 'bucket-data-prashanth', local_folder = "/tmp")
    
    
    #print(os.path.dirname(os.path.realpath(__file__)))
    #print(os.path.isfile('/tmp/' + 'player_to_pid.json'))
    
    
    files_names = []
    for dirpath, dirnames, files in os.walk('/tmp'):
        for file_name in files:
            initial = file_name[0:8]
            #print(initial)
            if(str(file_name) != 'player_to_pid.json' and initial != 'metadata'):
                files_names.append(file_name)
    #directory ="ipl_json"

    	

    file = '/tmp/player_to_pid.json'
    with open(file) as handle:
        try:
            player_to_pid = json.loads(handle.read())
            print(player_to_pid, '888888888888888')
        except:
            print("Error opening the player to pid file")


    for filename in files_names:  
        with open('/tmp/' + filename, 'r') as stream:
            try:
                #print(stream)
                converted  = json.load(stream)
                #print(converted)
                matchid = filename.split('.')[0]
                team1 = converted['info']['teams'][0]
                team2 = converted['info']['teams'][1]
                venue = 'We are at ' + converted['info']['venue'] + ', ' + converted['info']['city']
                players = 'This time it is ' + team1 + ' vs ' + team2
                toss = converted['info']['toss']['winner'] + ' wins the toss and chooses to ' + converted['info']['toss']['decision']
                directory_for_metadata = '/tmp/'
                with open(os.path.join(directory_for_metadata,'metadata' + matchid + '.json')) as meta:
                    try:
                        meta = json.loads(meta.read())
                        #print(meta)
                    except:
                        print("Error with metadata")
              
                
                #time_hours = meta["time_hours"]
                #time_mins = meta["time_mins"]
                #Squad1 = meta["Squad1"]
                #Squad2 = meta["Squad2"]
              
                      
                match = converted['innings']
                innings1deliveries = match[0]['1st innings']['deliveries']
                number_of_deliveries_inningss_1 = len(innings1deliveries)
                innings2deliveries = match[1]['2nd innings']['deliveries'] 
                number_of_deliveries_inningss_2 = len(innings2deliveries)  
                print(number_of_deliveries_inningss_2)
                score = 0
                wickets = 0
              
                match_data = []
                t1 = [matchid, 0, venue, 'null', 'null', 'null', [['null'],['null']], 'null','null']
                t2 = [matchid, 5, players, 'null', 'null', 'null', [['null'],['null']], 'null','null']
                t3 = [matchid, 10, toss, 'null', 'null', 'null', [['null'],['null']], 'null','null']
                match_data.append(t1)
                match_data.append(t2)
                match_data.append(t3)
                time = 10
    
                #Innings 1
                for i in range(number_of_deliveries_inningss_1):
                    time += 5
                    update_points = []
                    update_player_id = []
                    current_del = innings1deliveries[i][list(innings1deliveries[i].keys())[0]]
                    #print(list(innings1deliveries[i].keys())[0])
                    wicket = False
                    kind = ''
                    fielders = []
                    batsman = current_del['batsman']
                    bowler = current_del['bowler']
                    non_striker = current_del['non_striker']
                    batsman_score = current_del['runs']['batsman']
                    extras = current_del['runs']['extras']
                    total = current_del['runs']['total']
                    score += total
                  
                    if(batsman_score and extras):
                        message = 'Batsman scored ' + str(batsman_score) + ' and extras obtained is ' + str(extras)
                        update_player_id.append(player2pid(batsman, player_to_pid))
                        update_points.append(batsman_score)
    
                    if(batsman_score==0 and extras==0):
                        message = 'No runs from this ball'
                      
                    if(batsman_score):
                        message = 'Batsman scored ' + str(batsman_score) + ' runs'
                        update_player_id.append(player2pid(batsman, player_to_pid))
                        update_points.append(int(batsman_score))       
                    if(extras):
                        message = 'Extras of ' + str(extras) + ' runs'                  
                    if('wicket' in current_del.keys()):
                        wickets += 1
                        print(wickets)
                        if(current_del['wicket']['kind'] == 'bowled'):
                            message = current_del['wicket']['player_out'] +' bowled by ' + bowler
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(50)
                        if(current_del['wicket']['kind'] == 'lbw'):
                            message = current_del['wicket']['player_out'] +' lbw dismissal by ' + bowler
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(50)
                          
                        if(current_del['wicket']['kind'] == 'caught'):
                            message = current_del['wicket']['player_out'] +' caught out by ' + current_del['wicket']['fielders'][0]
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(50)
                            update_player_id.append(player2pid(current_del['wicket']['fielders'][0], player_to_pid))
                            update_points.append(30)
    
                        if(current_del['wicket']['kind'] == 'run out'):
                            message = current_del['wicket']['player_out'] +' run out by ' + current_del['wicket']['fielders'][0]
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(20)
                            update_player_id.append(player2pid(current_del['wicket']['fielders'][0],player_to_pid))
                            update_points.append(50)
    
                      
                              
                              
                    match_data.append([matchid, time, message, batsman, non_striker, bowler, [update_player_id,update_points], str(score)+'/' + str(wickets), str(list(innings1deliveries[i].keys())[0])])                    
    
                time += 5
                t6 = [matchid, time, 'Moving on to the second innings', 'null', 'null', 'null', [['null'],['null']], 'null','null']
                match_data.append(t6)
                score = 0
                wickets = 0
                #Innings 2
                for i in range(number_of_deliveries_inningss_2):
                    time += 5
                    update_points = []
                    update_player_id = []
                    current_del = innings2deliveries[i][list(innings2deliveries[i].keys())[0]]
                    #print(current_del)
                    wicket = False
                    kind = ''
                    fielders = []
                    batsman = current_del['batsman']
                    bowler = current_del['bowler']
                    non_striker = current_del['non_striker']
                    batsman_score = current_del['runs']['batsman']
                    extras = current_del['runs']['extras']
                    total = current_del['runs']['total']
                    score += total
                  
                    if(batsman_score and extras):
                        message = 'Batsman scored ' + str(batsman_score) + ' and extras obtained is ' + str(extras)
                        update_player_id.append(player2pid(batsman, player_to_pid))
                        update_points.append(batsman_score)
                  
                    if(batsman_score==0 and extras==0):
                        message = 'No runs from this ball'
        
                    if(batsman_score):
                        message = 'Batsman scored ' + str(batsman_score) + ' runs'
                        update_player_id.append(player2pid(batsman, player_to_pid))
                        update_points.append(int(batsman_score))       
                    if(extras):
                        message = 'Extras of ' + str(extras) + ' runs'                  
                    if('wicket' in current_del.keys()):
                        wickets += 1
                        print(wickets)
                        if(current_del['wicket']['kind'] == 'bowled'):
                            message = current_del['wicket']['player_out'] +' bowled by ' + bowler
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(50)
                        if(current_del['wicket']['kind'] == 'lbw'):
                            message = current_del['wicket']['player_out'] +' lbw dismissal by ' + bowler
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(50)
                          
                        if(current_del['wicket']['kind'] == 'caught'):
                            message = current_del['wicket']['player_out'] +' caught out by ' + current_del['wicket']['fielders'][0]
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(50)
                            update_player_id.append(player2pid(current_del['wicket']['fielders'][0], player_to_pid))
                            update_points.append(30)
    
                        if(current_del['wicket']['kind'] == 'run out'):
                            message = current_del['wicket']['player_out'] +' run out by ' + current_del['wicket']['fielders'][0]
                            update_player_id.append(player2pid(bowler, player_to_pid))
                            update_points.append(20)
                            update_player_id.append(player2pid(current_del['wicket']['fielders'][0], player_to_pid))
                            update_points.append(50)
    
                      
                              
                              
                    match_data.append([matchid, time, message, batsman, non_striker, bowler, [update_player_id,update_points], str(score)+'/' + str(wickets),str(list(innings2deliveries[i].keys())[0])])                    
    
    
    
    
                final_outcome = converted['info']['outcome']['winner'] +  ' wins the game'
                player_of_match = 'And the player of the match is ' + converted['info']['player_of_match'][0]
                time += 5
                t4 = [matchid, time, final_outcome, 'null', 'null', 'null', [['null'],['null']], 'null','null']
                time += 5
                t5 = [matchid, time, player_of_match, 'null', 'null', 'null', [['null'],['null']], 'null','null']
                match_data.append(t4)
                match_data.append(t5)      
             
                
              
                #print(converted['info']['player_of_match'])
                dynamo_db = boto3.resource('dynamodb') 
                match_table = dynamo_db.Table('dr11-scoredb')
                for event in match_data:
                    #print(event)
                    if(len(event[6][0])==0):
                        event[6][0].append('null')
                        event[6][1].append('null')
                    #print(points,pid, event[6][0])
                    response = match_table.put_item(
                        Item={
                            'matchid': event[0],
                            'time': str(event[1]),
                            'batsman' : event[3],
                            'bowler' : event[5],
                            'nonstriker' : event[4],
                            'message': event[2],
                            'over': event[8],
                            'score': event[7],
                            'pid': str(event[6][0][0]),
                            'points':str(event[6][1][0])
                            }
                        )
                    
            except Exception as e:
                logging.error(traceback.format_exc())
                
    
    dynamo_db = boto3.resource('dynamodb')
    #player database
    file = '/tmp/metadataplayer.json'
    with open(file) as handle:
        metadataplayer = json.loads(handle.read())

    
    
    player_table = dynamo_db.Table('dr11-playerdata')
    for key in metadataplayer:
        response = player_table.put_item(
                            Item={
                                'playerid': str(metadataplayer[key]['playerid']),
                                'credits': str(metadataplayer[key]['credits']),
                                'category' : str(metadataplayer[key]['category']),
                                'playername' : str(key),
                                'points' : str(metadataplayer[key]['points'])
                                }
                            )
    #public match data
    for filename in files_names:
        print(filename,"*********************")
        with open('/tmp/' +'metadata' + filename, 'r') as stream:
            matchdetails = json.loads(stream.read())
    
        pub_match_table = dynamo_db.Table('dr11-publicmatch')
        response = pub_match_table.put_item(
                            Item={
                                'LeagueId': str(matchdetails['LeagueId']),
                                'MatchId': str(matchdetails['MatchId']),
                                'Date' : str(matchdetails['Date']),
                                'Hours' : str(matchdetails['Hours']),
                                'Minutes' : str(matchdetails['Minutes']),
                                "Team":str(matchdetails['Team']),
                                "Team2":str(matchdetails['Team2']),
                                "Team1 Players":str(matchdetails['Team1 Players']),
                                "Team2 Players":str(matchdetails['Team2 Players']),
                                "Status":str(matchdetails['Status']),
                                "count1":str(matchdetails['count1'])
                                }
                            )
                            
                            
                            
                            
    
    
    return {
        'statusCode': 200,
        'headers': { 
                "Access-Control-Allow-Origin" : "*", 
                "Access-Control-Allow-Credentials" : True  
              },
        'body': json.dumps('Hello from Lambda!')
    }













        

    



