# %%
import sqlite3
import pandas as pd

df = pd.read_csv('ships.csv')
df

# %%


schema = '''
CREATE TABLE IF NOT EXISTS ships (
    name TEXT,
    lat FLOAT NOT NULL,
    lng FLOAT NOT NULL
);
'''

db_file = 'ships.db'
conn = sqlite3.connect(db_file)
conn.executescript(schema)

try:
    with conn as cur:
        cur.execute('BEGIN')
        df.to_sql('ships', conn, if_exists='append', index=False)
except:
    print('transaction failed')
finally:
    conn.close()

# %%
