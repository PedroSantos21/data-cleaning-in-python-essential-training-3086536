# %%
"""
Fix the data frame. At the end, row should have the following columns:
- start: pd.Timestemap
- end: pd.Timestamp
- name: str
- topic: str (python or go)
- earnings: np.float64
"""
# %%
import numpy as np
import pandas as pd

df = pd.read_csv('workshops.csv')
df


# %%
# renomeando columnas
df.columns = df.columns.str.lower()
df.columns.tolist()

# %%
df['year'].fillna(method='ffill', inplace=True)
df['month'].fillna(method='ffill', inplace=True)
df

# %%
# Filtering
mask = df.eval('name.isnull()')
df_filter = df[~mask]
df_filter
# %%
df2 = df_filter.copy()
df2.dtypes
# year to int
df2['year'] = df2['year'].astype(int).astype(str)
df2['start'] = df2['start'].astype(int).astype(str)
df2['end'] = df2['end'].astype(int).astype(str)
df2

# %%
df2['mont_num'] = df2['month'].replace({'June': '06',
                                        'July': '07'})

df2

# %%
# - start: pd.Timestemap
# - end: pd.Timestamp

df2['start'] = pd.to_datetime(
    df2['year']+'-'+df2['mont_num']+'-'+df2['start'])
df2['end'] = pd.to_datetime(
    df2['year']+'-'+df2['mont_num']+'-'+df2['end'])
df2

# %%
df2.drop(columns=['mont_num', 'year', 'month'])
# %%
df2['name'] = df2['name'].str.upper()
df2['topic'] = df2.name.apply(lambda x: 'PYTHON' if 'PYTHON' in x else 'GO')
df2[['name', 'topic']]
# %%
df2['earnings'] = pd.to_numeric(
    df2['earnings'].str.replace(r'[$,]', '')
).astype(np.float64)

df2

# %%

df_final = df2[['start', 'end', 'name', 'topic', 'earnings']]
df_final
# %%
