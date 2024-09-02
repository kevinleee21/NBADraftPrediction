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
player_info_url = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=' + per_mode + '&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=' + season + '&SeasonSegment=&SeasonType=' + season_type + '&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
player_info_url_adv = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2023-24&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
player_info_url_phy = 'https://stats.nba.com/stats/leaguedashplayerbiostats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season=2023-24&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
adv_player_columns = {"PLAYER_ID": "INTEGER", "PLAYER_NAME": "TEXT", "NICKNAME": "TEXT", "TEAM_ID": "TEXT",
                      "AGE": "INTEGER", "GP": "INTEGER",
                      "W": "INT", "L": "INT", "W_PCT": "FLOAT", "MIN": "FLOAT", "E_OFF_RATING": "FLOAT",
                      "OFF_RATING": "FLOAT", "sp_work_OFF_RATING": "FLOAT",
                      "E_DEF_RATING": "FLOAT", "DEF_RATING": "FLOAT", "sp_work_DEF_RATING": "FLOAT",
                      "E_NET_RATING": "FLOAT", "NET_RATING": "FLOAT",
                      "sp_work_NET_RATING": "FLOAT", "AST_PCT": "FLOAT", "AST_TO": "FLOAT", "AST_RATIO": "FLOAT",
                      "OREB_PCT": "FLOAT", "DREB_PCT": "FLOAT", "REB_PCT": "FLOAT",
                      "TM_TOV_PCT": "FLOAT", "E_TOV_PCT": "FLOAT", "EFG_PCT": "FLOAT", "TS_PCT": "FLOAT",
                      "USG_PCT": "FLOAT", "E_USG_PCT": "FLOAT", "E_PACE": "FLOAT",
                      "PACE": "FLOAT", "PACE_PER40": "FLOAT", "sp_work_PACE": "FLOAT", "PIE": "FLOAT",
                      "POSS": "INTEGER", "FGM": "INTEGER", "FGA": "INTEGER", "FGM_PG": "FLOAT",
                      "FGA_PG": "FLOAT", "FG_PCT": "FLOAT", "GP_RANK": "INTEGER", "W_RANK": "INTEGER",
                      "L_RANK": "INTEGER", "W_PCT_RANK": "INTEGER", "MIN_RANK": "INTEGER",
                      "E_OFF_RATING_RANK": "INTEGER", "OFF_RATING_RANK": "INTEGER",
                      "sp_work_OFF_RATING_RANK": "INTEGER", "E_DEF_RATING_RANK": "INTEGER",
                      "DEF_RATING_RANK": "INTEGER",
                      "sp_work_DEF_RATING_RANK": "INTEGER", "E_NET_RATING_RANK": "INTEGER",
                      "NET_RATING_RANK": "INTEGER", "sp_work_NET_RATING_RANK": "INTEGER", "AST_PCT_RANK": "INTEGER",
                      "AST_TO_RANK": "INTEGER", "AST_RATIO_RANK": "INTEGER", "OREB_PCT_RANK": "INTEGER",
                      "DREB_PCT_RANK": "INTEGER", "REB_PCT_RANK": "INTEGER", "TM_TOV_PCT_RANK": "INTEGER",
                      "E_TOV_PCT_RANK": "INTEGER", "EFG_PCT_RANK": "INTEGER", "TS_PCT_RANK": "INTEGER",
                      "USG_PCT_RANK": "INTEGER", "E_USG_PCT_RANK": "INTEGER", "E_PACE_RANK": "INTEGER",
                      "PACE_RANK": "INTEGER", "sp_work_PACE_RANK": "INTEGER", "PIE_RANK": "INTEGER",
                      "FGM_RANK": "INTEGER", "FGA_RANK": "INTEGER", "FGM_PG_RANK": "INTEGER",
                      "FGA_PG_RANK": "INTEGER", "FG_PCT_RANK": "INTEGER"}
physical_attr_columns = {"PLAYER_ID": "INTEGER", "PLAYER_NAME": "TEXT",
                         "TEAM_ID": "INTEGER", "TEAM_ABBREVIATION": "TEXT",
                         "AGE": "FLOAT", "PLAYER_HEIGHT": "TEXT",
                         "PLAYER_HEIGHT_INCHES": "INTEGER",
                         "PLAYER_WEIGHT": "TEXT", "COLLEGE": "TEXT",
                         "COUNTRY": "TEXT", "DRAFT_YEAR": "TEXT",
                         "DRAFT_ROUND": "TEXT", "DRAFT_NUMBER": "TEXT",
                         "GP": "INTEGER", "PTS": "FLOAT", "REB": "FLOAT",
                         "AST": "FLOAT", "NET_RATING": "FLOAT", "OREB_PCT": "FLOAT",
                         "DREB_PCT": "FLOAT", "USG_PCT": "FLOAT", "TS_PCT": "FLOAT",
                         "AST_PCT": "FLOAT"
                         }


def create_table(table_name, columns):
    column_vals = " ".join([f'{col_name} {data_type}' for col_name, data_type in columns.items()])
    table_query = f"""CREATE TABLE "{table_name}" ({column_vals})"""
    return table_query


def insert(table_name, column_names):
    column_vals = ", ".join(f'"{col_name}"' for col_name, data_type in column_names.items())
    placeholders = ", ".join(["?"] * len(column_names))
    insert_data = f""" INSERT INTO {table_name} ({column_vals}) VALUES ({placeholders})"""
    return insert_data


def scrape_data(url, season_type, mode_type, season_years, column_names, table_name):
    """
    scrape nba player data based on the specific url, season type(regular, playoff, etc), per mode stats, number of seasons,
    and insert that data into the data in the form of tables
    :param url: specific url of the nba stats website
    :param season_type: the type of season (regular, playoffs, allstar, play-in, etc)
    :param mode_type: type of statistics we want ranging from totals, per game, per 48 min, etc.
    :param season_years: all the years recorded by the nba stats website
    :param column_names: inserts the scraped data into the table in the database
    :param table_name: name of the table
    :return:
    """
    # iterate over each year starting from 1996 to 2024
    for season in season_years:
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
        response = requests.get(url=player_info_url_adv, headers=headers).json()
        # Regular season, per game player data from the given year,
        player_info = response['resultSets'][0]['rowSet']
        table_name_season = table_name + {season}
        # get table name from database
        listOfTables = cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_exists = False
        # flatten tuple into a list
        table_names = [table[0] for table in listOfTables]
        # check if the table exists in the database
        table_exists = table_name_season in table_names
        if table_exists:
            print(f"Table '{table_name}' exists.")
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            row_count = cursor.fetchone()[0]
            if row_count == 0:
                insert_query = insert(table_name, column_names)
                cursor.executemany(insert_query, player_info)
                conn.commit()
            elif row_count > 0:
                continue
        else:
            table_query = create_table(table_name, column_names)
            cursor.execute(table_query)
            insert_query = insert(table_name, column_names)
            cursor.executemany(insert_query, player_info)
            conn.commit()
        # if data was successfully inserted into the given table
        print('Successfully inserted')
    # close connection
    conn.close()


if __name__ == '__main__':
    nba_adv_stats = f'NBA_Player_adv_stats_'
    nba_phy_stats = f'NBA_Player_phy_stats_'
    scrape_data(player_info_url_adv, season_type, per_mode, season_years, adv_player_columns, nba_adv_stats)
    scrape_data(player_info_url_phy, season_type, per_mode, season_years, physical_attr_columns, nba_phy_stats)
