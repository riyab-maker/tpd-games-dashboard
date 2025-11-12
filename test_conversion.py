import pandas as pd

df = pd.read_csv('data/summary_data.csv')
print('Summary Data:')
print(df)
print()

started_instances = int(df[df.Event=='Started']['Instances'].iloc[0])
completed_instances = int(df[df.Event=='Completed']['Instances'].iloc[0])

print(f'Started Instances: {started_instances:,}')
print(f'Completed Instances: {completed_instances:,}')
print(f'Expected Conversion: {completed_instances / started_instances * 100:.2f}%')

