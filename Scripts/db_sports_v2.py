import http.client
import json
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import time

# Sleep between each request
# ie: 50 requests by minutes
time_sleep = 2

# Features
counter_compet = 0
counter_play = 0

# Database
conn = sqlite3.connect('/home/Project_DS/Database/Sports_Results.db')
#conn = sqlite3.connect('Sports_Results.db')
c = conn.cursor()
#engine = create_engine("sqlite:///Sports_Results.db")
engine = create_engine("sqlite:////home/Project_DS/Database/Sports_Results.db")

# Connection API
connection = http.client.HTTPConnection('api.football-data.org')
headers = { 'X-Auth-Token': 'f3720f8a971b4ffbb347cc0690da19bd', 'X-Response-Control': 'minified' }

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
connection.request('GET', '/v1/competitions', None, headers )
df = pd.DataFrame(json.loads(connection.getresponse().read().decode()))
time.sleep(time_sleep)

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

del df_init

for i in id_team:
    if counter_play == 0:
        print(i)
        connection.request('GET', '/v1/teams/' + str(i) +'/players', None, headers)

        try: 
            df_init = block_collect_1('players','teamId')
            counter_play += 1 
            time.sleep(time_sleep)
            #print(df_init)
        except UnboundLocalError:
            pass

    if counter_play > 0:
        connection.request('GET', '/v1/teams/' + str(i) +'/players', None, headers )
        print(i)
        try: 
            df_init = block_collect_2('players','teamId', df_init) 
            time.sleep(time_sleep)
           # print(df_init)
        except UnboundLocalError:
            pass

# Insert a row of data
c.execute('''DROP TABLE IF EXISTS Players;''')
print(df_init.columns)
df_init.to_sql('Players',engine)

# Save (commit) the changes
conn.commit()
# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()
