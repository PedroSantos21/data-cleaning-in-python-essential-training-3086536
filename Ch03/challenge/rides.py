# %%
import pandas as pd
import numpy as np

df = pd.read_csv('rides.csv')
df
# %%
# Find out all the rows that have bad values
# - Missing values are not allowed
## First trating invalid data on plate
df['plate'] = df['plate'].str.strip()
df['plate'] = df['plate'].replace({'': np.nan})
df.plate.value_counts(dropna=False)
# %%
# Creating mask for missing values
mask_miss = df.isnull().any(axis=1)
df[mask_miss]

# %%
# - A plate must be a combination of at least 3 upper case letters or digits
regex =  '^[0-9A-Z]{3,}'
mask_plate = ~df['plate'].str.match(pat=regex, na=False)
df[mask_plate]

# %%
# - Distance much be bigger than 0
mask_distance = df['distance'] < 0
df[mask_distance]
# %%
mask_invalid = mask_miss | mask_plate | mask_distance
df[mask_invalid]
# %%
