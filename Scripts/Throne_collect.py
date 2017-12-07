import peyton

throne = peyton.Throne(username='JulienHeiduk', token="bcebc6d7-f224-4940-a816-74e3e6d4c34a")

# Get historical data for a competition
throne.competition('Italian Serie A').get_historical_data()
my_historical_data = throne.competition.historical_data

# Get competition data for a competition
throne.competition('Italian Serie A').get_competition_data()
my_competition_data = throne.competition.competition_data
print(my_competition_data)

