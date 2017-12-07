import http.client
import json
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import time
import datetime

# Sleep between each request
# ie: 50 requests by minutes
time_sleep = 1

# Features
counter_compet = 0
counter_play = 0
counter_team = 0

# Database
conn = sqlite3.connect('/home/Project_DS/Database/Sports_Results.db')
#conn = sqlite3.connect('Sports_Results.db')
c = conn.cursor()
#engine = create_engine("sqlite:///Sports_Results.db")
engine = create_engine("sqlite:////home/Project_DS/Database/Sports_Results.db")

# Connection API
connection = http.client.HTTPConnection('api.football-data.org')
headers = { 'X-Auth-Token': 'f3720f8a971b4ffbb347cc0690da19bd', 'X-Response-Control': 'minified' }

# Computate the season
now = datetime.datetime.now()

if now.month < 8:
    season_current = now.year - 1
    season_previous = now.year
if now.month > 8:
    season_current = now.year
    season_previous = now.year - 1

#print(season_current, season_previous)

# Block
def block_collect_1(feature, id_out):
        try: 
            response = pd.DataFrame(json.loads(connection.getresponse().read().decode()))
            input_data = response[feature]
            total = len(input_data)
            
            for j in range(total):
                if j == 0:
                    df_init = pd.DataFrame([input_data.iloc[0]], columns = input_data.iloc[0].keys())
                if j > 0:
                    df_init = pd.concat([df_init,pd.DataFrame([input_data.iloc[j]], columns = input_data.iloc[j].keys())])
            
            df_init.reset_index(drop = True, inplace = True)
            df_init[id_out] = i
        except:
            print('block 1 broken') 
            pass
        time.sleep(time_sleep)
        
        return df_init

def block_collect_2(feature, id_out, df_init):  
        try: 
            response = pd.DataFrame(json.loads(connection.getresponse().read().decode()))
            input_data = response[feature]
            nb_team = len(input_data)
            
            for j in range(nb_team):
                if j == 0:
                    df = pd.DataFrame([input_data.iloc[0]], columns = input_data.iloc[0].keys())
                if j >0:
                    df = pd.concat([df,pd.DataFrame([input_data.iloc[j]], columns = input_data.iloc[j].keys())])
            df[id_out] = i
            df_init = pd.concat([df_init,df])
            
            df_init.reset_index(drop = True, inplace = True)
        except:
            pass
        time.sleep(time_sleep)
        
        return df_init 

# Competitions
connection.request('GET', '/v1/competitions/?season=' + str(season_current), None, headers )
df_current = pd.DataFrame(json.loads(connection.getresponse().read().decode()))
time.sleep(time_sleep)

connection.request('GET', '/v1/competitions/?season=' + str(season_previous), None, headers )
df_previous = pd.DataFrame(json.loads(connection.getresponse().read().decode()))
time.sleep(time_sleep)

df = pd.concat([df_current,df_previous])
df.reset_index(inplace = True)

# Insert a row of data
c.execute('''DROP TABLE IF EXISTS Competitions;''')
df.to_sql('Competitions',engine)

# Collect the leaderboard for each competitions
id_competitions = df.id

for i in id_competitions:
    if counter_compet == 0:
        counter_compet += 1
        
        connection.request('GET', '/v1/competitions/' + str(i) + '/leagueTable', None, headers )
        
        df_init = block_collect_1('standing','id')
        
    if counter_compet > 0:
        connection.request('GET', '/v1/competitions/' + str(i) + '/leagueTable', None, headers )
        
        df_init = block_collect_2('standing','id', df_init)

# Insert a row of data        
c.execute('''DROP TABLE IF EXISTS Leaderboards;''')
df_init.to_sql('Leaderboards',engine)

# Collect the players for each team
id_team = df_init['teamId']

for i in id_team:
    if counter_play == 0:
        connection.request('GET', '/v1/teams/' + str(i) +'/players', None, headers)

        try: 
            df_init = block_collect_1('players','id')
            counter_play += 1 
            time.sleep(time_sleep)
            #print(df_init)
        except UnboundLocalError:
            pass

    if counter_play > 0:
        connection.request('GET', '/v1/teams/' + str(i) +'/players', None, headers )
        try: 
            df_init = block_collect_2('players','id', df_init) 
            time.sleep(time_sleep)
           # print(df_init)
        except UnboundLocalError:
            pass

# Insert a row of data
c.execute('''DROP TABLE IF EXISTS Players;''')
df_init.to_sql('Players',engine)

# Collect informations for all teams
for i in id_competitions:
    if counter_team == 0:
        connection.request('GET', '/v1/competitions/' + str(i) +'/teams', None, headers)

        try: 
            df_init = block_collect_1('teams','teamId')
            counter_team += 1 
            time.sleep(time_sleep)
        except UnboundLocalError:
            pass

    if counter_team > 0:
        connection.request('GET', '/v1/competitions/' + str(i) +'/teams', None, headers )
        try: 
            df_init = block_collect_2('teams','teamId', df_init) 
            time.sleep(time_sleep)
        except UnboundLocalError:
            pass

# Insert a row of data
c.execute('''DROP TABLE IF EXISTS Teams;''')
df_init.to_sql('Teams',engine)

# Save (commit) the changes
conn.commit()
# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()
