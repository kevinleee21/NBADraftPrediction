import sqlite3

conn = sqlite3.connect('DataBase/Player.db')
cursor = conn.cursor()

cursor.execute(""" SELECT * PLAYER_ID, PLAYER_NAME, PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES, 
PLAYER_WEIGHT, COLLEGE, COUNTRY, DRAFT_YEAR, DRAFT_ROUND, DRAFT_NUMBER, GP FROM  """)
