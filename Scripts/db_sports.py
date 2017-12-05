import http.client
import json
import pandas as pd

select_compet = str(456)

connection = http.client.HTTPConnection('api.football-data.org')
headers = { 'X-Auth-Token': 'f3720f8a971b4ffbb347cc0690da19bd', 'X-Response-Control': 'minified' }
connection.request('GET', '/v1/competitions/' + select_compet + '/leagueTable', None, headers )

# Collect the leaderboard
#print(connection.getresponse().read().decode())
try: 
    response = pd.DataFrame(json.loads(connection.getresponse().read().decode()))
except ValueError:
    response = json.loads(connection.getresponse().read().decode())

input_data = response['standing']
nb_team = len(input_data)

for i in range(nb_team):
    if i == 0:
        df = pd.DataFrame([input_data.iloc[0]], columns = input_data.iloc[0].keys())
    if i >0:
        df = pd.concat([df,pd.DataFrame([input_data.iloc[i]], columns = input_data.iloc[i].keys())])

df.reset_index(drop = True, inplace = True)
        
#print(df)

# Collect the players for each team
id_team = df['teamId']
j = 0

for i in id_team:
    if j == 0:
        i = str(i)
        connection = http.client.HTTPConnection('api.football-data.org')
        headers = { 'X-Auth-Token': 'f3720f8a971b4ffbb347cc0690da19bd', 'X-Response-Control': 'minified' }
        connection.request('GET', '/v1/teams/' + i +'/players', None, headers )
        
        try: 
            response = pd.DataFrame(json.loads(connection.getresponse().read().decode()))
        except ValueError:
            response = json.loads(connection.getresponse().read().decode())
        
        input_data = response['players']
        nb_play = len(input_data)
        
        for i in range(nb_play):
            if i == 0:
                df = pd.DataFrame([input_data.iloc[0]], columns = input_data.iloc[0].keys())
            if i >0:
                df = pd.concat([df,pd.DataFrame([input_data.iloc[i]], columns = input_data.iloc[i].keys())])
        
        df.reset_index(drop = True, inplace = True)
        df['teamId'] = i
        j += 1
        
import sqlite3
from sqlalchemy import create_engine

conn = sqlite3.connect('Sports_Results.db')

c = conn.cursor()
c.execute('''DROP TABLE IF EXISTS Leaderboard;''')

engine = create_engine("sqlite:///Sports_Results.db")


# Create table
#c.execute('''CREATE TABLE Leaderboard ('name', 'marketValue', 'jerseyNumber', 'dateOfBirth','contractUntil', 'nationality', 'position', 'id', 'teamId')''')

# Insert a row of data
#c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
df.to_sql('Leaderboard',engine)

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

r'''        
    if j >0:
        i = str(i)
        connection = http.client.HTTPConnection('api.football-data.org')
        headers = { 'X-Auth-Token': 'f3720f8a971b4ffbb347cc0690da19bd', 'X-Response-Control': 'minified' }
        connection.request('GET', '/v1/teams/' + i +'/players', None, headers )
        
        try: 
            response = pd.DataFrame(json.loads(connection.getresponse().read()))
        except ValueError:
            response = json.loads(connection.getresponse().read())
        
        input_data = response['players']
        nb_play = len(input_data)
        
        for i in range(nb_play):
            if i == 0:
                df = pd.DataFrame([input_data.iloc[0]], columns = input_data.iloc[0].keys())
            if i >0:
                df = pd.concat([df,pd.DataFrame([input_data.iloc[i]], columns = input_data.iloc[i].keys())])
        
        df.reset_index(drop = True, inplace = True)
        df['teamId'] = i        
    print(df)
'''    
