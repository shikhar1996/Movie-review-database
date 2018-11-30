import psycopg2 as pc
import pandas as pd
from math import isnan

conn = pc.connect(
    host="localhost",
    dbname='testdb',
    user='postgres',
    password='postgres'
)
cursor = conn.cursor()
# server_side_cursor = conn.cursor('names_to_titles', withhold=True)

na_vals = {'isOriginalTitle': 0}

db = pd.read_csv('title.principals.tsv', sep='\t', nrows=100, na_values='\N').fillna('')
akas = pd.read_csv('title.akas.tsv', sep='\t', nrows=100, na_values='\N').fillna(na_vals)
ratings_db = pd.read_csv('title.ratings.tsv', sep='\t', nrows=100, na_values='\N').fillna(0)

print akas.head()

cursor.execute("DROP TABLE IF EXISTS principals")
cursor.execute(
    """CREATE TABLE principals (
            id          integer PRIMARY KEY,
            tconst      text    NOT NULL,    
            ordering    integer NOT NULL,
            nconst      text    NOT NULL,
            category    text,
            job         text,
            characters  text
        );"""
)

cursor.execute("DROP TABLE IF EXISTS ratings")
cursor.execute(
    """CREATE TABLE ratings(
            id          integer PRIMARY KEY,
            tconst      text    NOT NULL,
            avgRating   real,
            numVotes    integer
    );"""
)

cursor.execute("DROP TABLE IF EXISTS release_info")
cursor.execute(
    """CREATE TABLE release_info(
            id      integer PRIMARY KEY,
            tconst  text    NOT NULL,
            ordering    integer NOT NULL,
            title       text    NOT NULL,
            region      text,
            language    text,
            types       text,
            attributes  text,
            isOriginal  bool
    );"""
)
conn.commit()

for index, row in db.iterrows():
    char = '' if (isinstance(row[5], float) and isnan(row[5])) else row[5][1:-1]

    query_insert_row = """
        INSERT INTO principals VALUES (
            {}, '{}', '{}', '{}', '{}', '{}', '{}'
        );""".format(index+1, row[0], row[1], row[2], row[3], row[4], char)

    cursor.execute(query_insert_row)
conn.commit()

for index, row in ratings_db.iterrows():
    query_insert_row = """
        INSERT INTO ratings VALUES(
            {}, '{}', '{}', '{}'
        );""".format(index+1, row[0], row[1], row[2])

    cursor.execute(query_insert_row)
conn.commit()

for index, row in akas.iterrows():
    query_insert_row = """
        INSERT INTO release_info VALUES(
            {}, '{}', '{}','
        """.format(index+1, row[0], row[1]) + row[2].replace("'", "''") + """', '{}', '{}', '{}', '{}');""".format(row[3], row[4], row[5], row[6])
    cursor.execute(query_insert_row)
conn.commit()
