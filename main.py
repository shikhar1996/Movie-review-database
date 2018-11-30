import psycopg2 as pc
import pandas as pd
from math import isnan

conn = pc.connect("dbname='testdb' user='postgres' host='localhost' password='postgres'")

cur = conn.cursor()

query_create_table = """
    CREATE TABLE crew(
        tconst  varchar(20) PRIMARY KEY,
        directors  text[],
        writers     text[]
    );
"""

db = pd.read_csv('title.crew.tsv', sep='\t', na_values='\N')
print db.head()

cur.execute("DROP TABLE IF EXISTS crew;")
cur.execute(query_create_table)
conn.commit()

for index, row in db.iterrows():
    # print index
    # direc = '{'+row[1]+'}'
    direc = '{}' if (isinstance(row[1], float) and isnan(row[1])) else '{'+row[1]+'}'
    writers = '{}' if (isinstance(row[2], float) and isnan(row[2])) else '{'+row[2]+'}'
    # print direc, writers
    query_insert_row = """
        INSERT INTO crew VALUES (
            '{}', '{}', '{}'
        );""".format(row[0], direc, writers)

    cur.execute(query_insert_row)
conn.commit()

