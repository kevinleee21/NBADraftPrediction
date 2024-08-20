# import libraries
import requests
import pandas as pd
import sqlite3

# Connect to SQLite database (or create it if it doesn't exist)
# conn = sqlite3.connect('NBAPLayer.db')
# cursor = conn.cursor()

# cursor.execute("CREATE TABLE NBA_Player_stats24 (GP INTEGER, MIN FLOAT, PTS FLOAT, FGM FLOAT, FGA FLOAT, "
#                "FGP FLOAT, Three_PM FLOAT, Three_PA FLOAT, Three_PP FLOAT, FTM FLOAT, FTA FLOAT, FTP FLOAT, OREB FLOAT,"
#                " DREB FLOAT,REB FLOAT, AST FLOAT, TOV FLOAT, STL FLOAT, BLK FLOAT, PF FLOAT, DD2 FLOAT,"
#                "TD3 FLOAT, Plus_Minus FLOAT)")

season_id = '2022-23'
per_mode = 'PerGame'

player_info_url = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=' + per_mode + '&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=' + season_id + '&SeasonSegment=&SeasonType=Playoffs&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}

response = requests.get(url=player_info_url, headers=headers).json()

player_info = response['resultSets'][0]['rowSet']
print(player_info)
