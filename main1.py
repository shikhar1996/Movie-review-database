import psycopg2 as pc
import pandas as pd
import time
import io

st = time.time()

conn = pc.connect(
    host="localhost",
    dbname='testdb',
    user='postgres',
    password='postgres'
)
cursor = conn.cursor()
server_side_cursor = conn.cursor('names_to_titles', withhold=True)

print('Dropping old database tables...')
cursor.execute('DROP TABLE IF EXISTS titles')
cursor.execute('DROP TABLE IF EXISTS names')
cursor.execute('DROP TABLE IF EXISTS names_to_titles')
conn.commit()

print('Creating database tables...')
cursor.execute(
    '''CREATE TABLE names
    (
        nconst text PRIMARY KEY,
        primary_name text,
        birth_year integer,
        death_year integer,
        primary_profession text,
        known_for_titles text
    )'''
)
cursor.execute(
    '''CREATE TABLE titles
    (   
        tconst text PRIMARY KEY,
        title_type text,
        primary_title text,
        original_title text,
        is_adult bool,
        start_year integer,
        end_year integer,
        runtime_mins integer,
        genres text
    )'''
)
cursor.execute(
    '''CREATE TABLE names_to_titles
    (   
        PRIMARY KEY(id_names, id_titles),
        id_names text,
        id_titles text
    )'''
)

print('Importing name.basics.tsv...')
with open('name.basics.tsv') as names:
    # Omit header
    names.readline()
    cursor.copy_from(names, 'names')

cursor.execute('ALTER TABLE names ALTER COLUMN known_for_titles TYPE text[] '
               'USING string_to_array(known_for_titles, \',\')')
conn.commit()

print('Creating names table indexes...')
cursor.execute('CREATE INDEX primary_name_idx ON names (primary_name)')

print('Importing title.basics.tsv...')
with open('title.basics.tsv') as titles:
    # Omit header
    titles.readline()
    cursor.copy_from(titles, 'titles')

cursor.execute('ALTER TABLE titles ALTER COLUMN genres TYPE text[] USING string_to_array(genres, \',\')')
conn.commit()

print('Creating titles table indexes...')
cursor.execute('CREATE INDEX genres_idx ON titles (genres)')
cursor.execute('CREATE INDEX start_year_idx ON titles (start_year)')

print('Creating many-to-many relationships (names_to_titles)...')
server_side_cursor.execute("SELECT nconst, known_for_titles FROM names")
counter = 0
while True:
    counter += 5000
    names_to_titles = server_side_cursor.fetchmany(5000)
    if not names_to_titles:
        break
    data = io.BytesIO() # anything can be used as a file if it has .read() and .readline() methods
    for name in names_to_titles:
        if name[1]:
            for tconst in name[1]:
                relationship = '\t'.join([name[0], tconst])+'\n'
                data.write(relationship)
    data.seek(0)
    server_side_cursor.copy_from(data, 'names_to_titles')
    conn.commit()
    print('{} names has been processed\r'.format(counter))

print('Creating names_to_titles indexes...')
cursor.execute('CREATE INDEX id_titles_idx ON names_to_titles (id_titles)')
cursor.execute('CREATE INDEX id_names_idx ON names_to_titles (id_names)')

conn.close()

print()
print('Done.\n Executed in (sec):', time.time() - st)