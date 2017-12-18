import peyton #throne package => https://pypi.python.org/pypi/peyton 
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import time
time_sleep = 2

#list of the competition available on throme.ai (10/12/2017)
#competitions = ['English Championship','English Premier League','Italian Serie A','Spanish La Liga','NBA','NFL','NHL']
competitions = ['Italian Serie A']

#ThroneUser = "JulienHeiduk"
#ThroneToken = "bcebc6d7-f224-4940-a816-74e3e6d4c34a" # Julien
ThroneUser = "NicoDupont"
ThroneToken = "802f4123-4696-4b35-9e31-e8c32d91f658" #Nico

# API throne.ai :
throne = peyton.Throne(username=ThroneUser, token=ThroneToken)

def ThroneData(df):
    df.reset_index()
    #df.drop('Unnamed: 0', 1, inplace=True)
    df['year'] = df.date.str.slice(0,4)
    df['month'] = df.date.str.slice(5,7)
    df['day'] = df.date.str.slice(8,10)

# Get historical data for competitions. Is it possible to loop with the throttling limits?
#columns = Throne_historical_data.columns.tolist()
#print(columns)
i = 0
for compet in competitions:
    throne.competition(compet).get_historical_data()
    if i == 0:
        NewData = throne.competition.historical_data
        NewData['competition'] = compet
        #print(compet)
        #print(i)
    else:
        temp = throne.competition.historical_data
        temp['competition'] = compet
        NewData = pd.concat([NewData,temp],ignore_index=True,axis=0)
        #print(compet)
        #print(i)
    i += 1
    time.sleep(time_sleep)

#clean data :
ThroneData(NewData)
#print(NewData.info())
#print('------------------')
#print(NewData.tail())
#print('------------------')
#print(NewData.competition.value_counts())

# Database
con = sqlite3.connect('/home/Project_DS/Database/Sports_Results.db')
c = con.cursor()
engine = create_engine("sqlite:////home/Project_DS/Database/Sports_Results.db")


#c.execute('''DROP TABLE IF EXISTS ThroneHistorical;''')
#table_exist = c.execute("SELECT name FROM sqlite_master WHERE type='table' and name='ThroneHistorical';").fetchall()
#print(table_exist)
#if table_exist != []:
#    HistoId = pd.read_sql_query("select id from ThroneHistorical;",con)
#    HistoExist = 1;
#else:
#    HistoExist = 0;
#Throne_historical_data = pd.read_sql_query("select * from ThroneHistorical;",con)
#print(HistoExist)
# test if id's are already on the database
# problem because columns are not the same between competitions..
#if HistoExist == 0:
#    NewData.to_sql('ThroneHistorical',engine,index=False)
#else:
#    c.execute('''DROP TABLE IF EXISTS ThroneHistorical;''')
#    NewId = pd.merge(NewData,HistoId, on='id', how='outer',indicator=True)
#    NewId = NewId[NewId._merge == 'left_only'].drop(['_merge'], axis=1)
#    ThroneData(NewId)
#    print(NewId.info())
#    NewId.to_sql('ThroneHistorical',engine,index=False,if_exists='append')


c.execute('''DROP TABLE IF EXISTS ThroneHistorical;''')
NewData.to_sql('ThroneHistorical',engine,index=False,if_exists='append')
#c.execute("select count(*) from ThroneHistorical;")
#results = c.fetchall()
#print(results)

