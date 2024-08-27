import numpy as np
import pandas as pd
import sqlite3

conn = sqlite3.connect('DataBase/Player.db')
cursor = conn.cursor()

# get a list of all the table names to iterate over
get_tables = f"""
SELECT name 
from sqlite_master 
WHERE type ='table'
"""
tables = cursor.execute(get_tables).fetchall()
# list of all table names
table_names = [table[0] for table in tables]

# drop columns that are unnecessary for player archetypes
for table in table_names:
    cursor.execute(f"""PRAGMA table_info("{table}")""")
    cols = cursor.fetchall()

    drop_statements = []
    for column in cols:
        if any(keyword in column[1] for keyword in ['RANK', 'WNBA', 'NBA', 'TEAM', 'Nickname']):
            drop_statements.append(f"""ALTER TABLE '{table}' DROP COLUMN {column[1]};""")

    print(len(drop_statements))
    for drops in drop_statements:
        # print(drops)
        cursor.execute(drops)

conn.commit()
conn.close()
