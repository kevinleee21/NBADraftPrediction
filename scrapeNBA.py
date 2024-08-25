# import libraries
import os
import requests
import pandas as pd
import sqlite3

db_path = '/DataBase/Player.db'
if os.path.exists(db_path):
    print('Database already exists')

conn = sqlite3.connect('DataBase/Player.db')
cursor = conn.cursor()

season_years = [
    '1996-97', '1997-98', '1998-99', '1999-00', '2000-01',
    '2001-02', '2002-03', '2003-04', '2004-05', '2005-06',
    '2006-07', '2007-08', '2008-09', '2009-10', '2010-11',
    '2011-12', '2012-13', '2013-14', '2014-15', '2015-16',
    '2016-17', '2017-18', '2018-19', '2019-20', '2020-21',
    '2021-22', '2022-23', '2023-24'
]

per_mode = 'PerGame'
season_type = 'Regular Season'

for season in season_years:
    player_info_url = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=' + per_mode + '&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=' + season + '&SeasonSegment=&SeasonType=' + season_type + '&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='

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

    # Regular season, per game player data from the given year,
    player_info = response['resultSets'][0]['rowSet']
    table_name = f'NBA_Player_stats_{season}'
    # get table name from database
    listOfTables = cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_exists = False
    # flatten tuple into a list
    table_names = [table[0] for table in listOfTables]

    # check if the table exists in the database
    table_exists = table_name in table_names

    if table_exists:
        print(f"Table '{table_name}' exists.")
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        row_count = cursor.fetchone()[0]

        if row_count == 0:
            insert_query = f"""
            INSERT INTO "{table_name}"(
                Player_ID, Player_Name, Nickname, TEAM_ID, TEAM_ABBREVIATION, AGE, GP, W, L, W_PCT, MIN, FGM, FGA, FGP, Three_PM, 
                Three_PA, Three_PCT, FTM, FTA, FT_PCT, OREB, DREB, REB, AST, TOV, STL, BLK, BLKA, PF, PFD, PTS, Plus_Minus, 
                NBA_FANTASY_PTS, DD2, TD3, WNBA_FANTASY_PTS, GP_RANK, W_RANK, L_RANK, W_PCT_RANK, MIN_RANK, FGM_RANK, FGA_RANK, 
                FG_PCT_RANK, FG3M_RANK, FG3A_RANK, FG3_PCT_RANK, FTM_RANK, FTA_RANK, FT_PCT_RANK, OREB_RANK, DREB_RANK, REB_RANK, 
                AST_RANK, TOV_RANK, STL_RANK, BLK_RANK, BLKA_RANK, PF_RANK, PFD_RANK, PTS_RANK, PLUS_MINUS_RANK, NBA_FANTASY_PTS_RANK, 
                DD2_RANK, TD3_RANK, WNBA_FANTASY_PTS_RANK
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor.executemany(insert_query, player_info)
            conn.commit()
        elif row_count > 0:
            continue

    else:
        table_query = f"""CREATE TABLE "{table_name}" (Player_ID INTEGER, Player_Name TEXT, Nickname TEXT, TEAM_ID TEXT,
                                   TEAM_ABBREVIATION TEXT, AGE INTEGER, GP INTEGER, W INT, L INT, W_PCT FLOAT, MIN FLOAT, FGM FLOAT, FGA FLOAT,
                                   FGP FLOAT, Three_PM FLOAT, Three_PA FLOAT, Three_PCT FLOAT, FTM FLOAT, FTA FLOAT, FT_PCT FLOAT,
                                   OREB FLOAT,DREB FLOAT,REB FLOAT, AST FLOAT, TOV FLOAT, STL FLOAT, BLK FLOAT, BLKA FLOAT, PF FLOAT,
                                   PFD FLOAT, PTS FLOAT, Plus_Minus FLOAT, NBA_FANTASY_PTS FLOAT, DD2 FLOAT, TD3 FLOAT, 
                                   WNBA_FANTASY_PTS FLOAT, GP_RANK INTEGER, W_RANK INTEGER, L_RANK INTEGER, W_PCT_RANK INTEGER,
                                   MIN_RANK INTEGER,FGM_RANK INTEGER, FGA_RANK INTEGER, FG_PCT_RANK INTEGER, FG3M_RANK INTEGER, 
                                   FG3A_RANK INTEGER, FG3_PCT_RANK INTEGER, FTM_RANK INTEGER, FTA_RANK INTEGER,
                                   FT_PCT_RANK INTEGER, OREB_RANK INTEGER, DREB_RANK INTEGER, REB_RANK INTEGER, AST_RANK INTEGER,
                                   TOV_RANK INTEGER,STL_RANK INTEGER, BLK_RANK INTEGER, BLKA_RANK INTEGER,PF_RANK INTEGER, PFD_RANK INTEGER,
                                   PTS_RANK INTEGER, PLUS_MINUS_RANK INTEGER,NBA_FANTASY_PTS_RANK INTEGER, DD2_RANK INTEGER, TD3_RANK INTEGER, 
                                   WNBA_FANTASY_PTS_RANK INTEGER)"""
        cursor.execute(table_query)

        insert_query = f"""
                        INSERT INTO "{table_name}" (
                            Player_ID, Player_Name, Nickname, TEAM_ID, TEAM_ABBREVIATION, AGE, GP, W, L, W_PCT, MIN, FGM, FGA, FGP, Three_PM, 
                            Three_PA, Three_PCT, FTM, FTA, FT_PCT, OREB, DREB, REB, AST, TOV, STL, BLK, BLKA, PF, PFD, PTS, Plus_Minus, 
                            NBA_FANTASY_PTS, DD2, TD3, WNBA_FANTASY_PTS, GP_RANK, W_RANK, L_RANK, W_PCT_RANK, MIN_RANK, FGM_RANK, FGA_RANK, 
                            FG_PCT_RANK, FG3M_RANK, FG3A_RANK, FG3_PCT_RANK, FTM_RANK, FTA_RANK, FT_PCT_RANK, OREB_RANK, DREB_RANK, REB_RANK, 
                            AST_RANK, TOV_RANK, STL_RANK, BLK_RANK, BLKA_RANK, PF_RANK, PFD_RANK, PTS_RANK, PLUS_MINUS_RANK, NBA_FANTASY_PTS_RANK, 
                            DD2_RANK, TD3_RANK, WNBA_FANTASY_PTS_RANK
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
        cursor.executemany(insert_query, player_info)
        conn.commit()

    # if data was successfully inserted into the given table
    print('Successfully inserted')

# close connection
conn.close()

