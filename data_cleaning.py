import numpy as np
import pandas as pd
import sqlite3

conn = sqlite3.connect('DataBase/Player.db')
cursor = conn.cursor()

# # get a list of all the table names to iterate over
# get_tables = f"""
# SELECT name
# from sqlite_master
# WHERE type ='table'
# """
# tables = cursor.execute(get_tables).fetchall()
# # list of all table names
# table_names = [table[0] for table in tables]
#
# # drop columns that are unnecessary for player archetypes
# for table in table_names:
#     cursor.execute(f"""PRAGMA table_info("{table}")""")
#     cols = cursor.fetchall()
#
#
#     drop_statements = []
#     for column in cols:
#         if any(keyword in column[1] for keyword in ['RANK', 'WNBA', 'NBA', 'TEAM', 'Nickname']):
#             drop_statements.append(f"""ALTER TABLE '{table}' DROP COLUMN {column[1]};""")
#
#     print(len(drop_statements))
#     for drops in drop_statements:
#         # print(drops)
#         cursor.execute(drops)
#

season_years = [
    '1996-97', '1997-98', '1998-99', '1999-00', '2000-01',
    '2001-02', '2002-03', '2003-04', '2004-05', '2005-06',
    '2006-07', '2007-08', '2008-09', '2009-10', '2010-11',
    '2011-12', '2012-13', '2013-14', '2014-15', '2015-16',
    '2016-17', '2017-18', '2018-19', '2019-20', '2020-21',
    '2021-22', '2022-23', '2023-24'
]


for season in season_years:
    table_name = 'NBA_Player_adv_stats_'
    table_f = table_name + season
    cursor.execute(f""" DROP TABLE "{table_f}" """)

conn.commit()
conn.close()
