import pandas as pd
import seaborn as sns
# load Lake Mead data
df = pd.read_csv('data/lake_mead.csv')

# convert from feet to meters because imperial is dumb
df['JAN'] = df['JAN']*0.3048
df['FEB'] = df['FEB']*0.3048
df['MAR'] = df['MAR']*0.3048
df['APR'] = df['APR']*0.3048
df['MAY'] = df['MAY']*0.3048
df['JUN'] = df['JUN']*0.3048
df['JUL'] = df['JUL']*0.3048
df['AUG'] = df['AUG']*0.3048
df['SEP'] = df['SEP']*0.3048
df['OCT'] = df['OCT']*0.3048
df['NOV'] = df['NOV']*0.3048
df['DEC'] = df['DEC']*0.3048

# create mean column (have to ignore the Year column or else that gets added in)
df['mean'] = df[['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']].mean(axis=1)
df.reset_index()

# only take period after Lake Mead filled up for plotting
df_full = df[(df['Year'] >= 1950)]

# dump to csv
df_full.to_csv('map/lake_mead_metric.csv',header=True,index=False)

