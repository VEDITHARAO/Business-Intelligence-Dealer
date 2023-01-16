import pyodbc
import pandas as pd
from fuzzywuzzy import fuzz
import time

#----Database Connection----

conn=pyodbc.connect(
"Driver={SQL Server Native Client 11.0};"
"Server= CBTSQLR01;"
"Database=Tradesight;"
"Trusted_Connection=yes;") 

#----Data Query & Cleaning----

banks_data_cleaned = pd.read_sql("SELECT [DealerKey] AS [DEALER_KEY], [DealerCode] AS [DEALER_CODE], [Dealername] AS [DEALER_NAME], [AddresssLine1] AS [DEALER_ADDRESS], [City] AS [DEALER_CITY], [State] AS [DEALER_STATE], [PostalCode] AS [DEALER_ZIP]FROM [Tradesight].[dbo].[DimDealer]WHERE [DealerName] in (SELECT DISTINCT [DealerName]FROM [Tradesight].[dbo].[DimDealer]GROUP BY [DealerName]HAVING COUNT([DealerName]) = 1)ORDER BY [DEALER_NAME], [PostalCode], [State], [City];", conn)
banks_data_cleaned["DEALER_ADDRESS"] = banks_data_cleaned[["DEALER_ADDRESS", "DEALER_CITY", "DEALER_STATE"]].apply(", ".join, axis=1)

banks_data_cleaned['DEALER_NAME'] = banks_data_cleaned['DEALER_NAME'].str.replace('#','')
banks_data_cleaned['DEALER_NAME'] = banks_data_cleaned['DEALER_NAME'].str.replace('.','')
banks_data_cleaned['DEALER_NAME'] = banks_data_cleaned['DEALER_NAME'].str.replace(', ','')
banks_data_cleaned['DEALER_ADDRESS'] = banks_data_cleaned['DEALER_ADDRESS'].str.replace('#','')
banks_data_cleaned['DEALER_ADDRESS'] = banks_data_cleaned['DEALER_ADDRESS'].str.replace('  ',' ')

banks_data_cleaned = banks_data_cleaned.drop(axis = 1, columns= ['DEALER_CITY','DEALER_STATE'])
banks_data_cleaned.dropna(axis=0, how='any', thresh=None, subset=None, inplace=False)
banks_data_cleaned.drop_duplicates(subset = ['DEALER_ADDRESS'], keep='first', inplace=True)

banks_data_cleaned.to_csv('banks_data_cleaned.11.02.csv')

tradesight_data_cleaned = pd.read_sql("SELECT [DEALER_NAME], [DEALER_ADDRESS], [DEALER_CITY], [DEALER_STATE], [DEALER_ZIP]FROM [Tradesight].[dbo].[Tradesight_Summary]WHERE [DEALER_NAME] in(SELECT DISTINCT [DEALER_NAME]FROM [Tradesight].[dbo].[Tradesight_Summary]GROUP BY [DEALER_NAME]HAVING COUNT([DEALER_NAME]) = 1)AND [DEALER_ADDRESS] is not null AND [DEALER_ZIP] is not null ORDER BY [DEALER_NAME], [DEALER_ZIP], [DEALER_STATE], [DEALER_CITY];",conn)
tradesight_data_cleaned["DEALER_ADDRESS"] = tradesight_data_cleaned[["DEALER_ADDRESS", "DEALER_CITY", "DEALER_STATE"]].apply(", ".join, axis=1)

tradesight_data_cleaned['DEALER_NAME'] = tradesight_data_cleaned['DEALER_NAME'].str.replace('#','')
tradesight_data_cleaned['DEALER_NAME'] = tradesight_data_cleaned['DEALER_NAME'].str.replace('.','')
tradesight_data_cleaned['DEALER_NAME'] = tradesight_data_cleaned['DEALER_NAME'].str.replace(', ','')
tradesight_data_cleaned['DEALER_ADDRESS'] = tradesight_data_cleaned['DEALER_ADDRESS'].str.replace('#','')
tradesight_data_cleaned['DEALER_ADDRESS'] = tradesight_data_cleaned['DEALER_ADDRESS'].str.replace('  ',' ')

tradesight_data_cleaned = tradesight_data_cleaned.drop(axis = 1, columns= ['DEALER_CITY','DEALER_STATE'])
tradesight_data_cleaned.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
tradesight_data_cleaned.drop_duplicates(subset = ['DEALER_ADDRESS'], keep='first', inplace=True)

tradesight_data_cleaned.to_csv('tradesight_data_cleaned.11.02.csv')


#----Testing----

    #----Name----
df_sample = tradesight_data_cleaned.iloc[:10,:]

reference_name = banks_data_cleaned['DEALER_NAME'].values
df_sample['extract_name'] = None
df_sample['best_score_name'] = None
df_sample['bank_address'] = None
t0 = time.time()
for idx, row in df_sample.iterrows():
    score_lis = []
    for refer in reference_name:
        score = fuzz.ratio(row['DEALER_NAME'].lower(), refer.lower())
        score_lis.append(score)
        if score >= 70:
            row['extract_name'] = refer
    row['best_score_name'] = max(score_lis)
   
    if int(idx)%100 == 0:
        print(round(int(idx)*1/df_sample.shape[0], 2), " %")
print(time.time()-t0)

df_sample_1 = df_sample.loc[(df_sample['best_score_name'] >= 70)]

def lookup_addr(dealer_name):
    return banks_data_cleaned.loc[banks_data_cleaned['DEALER_NAME'] == dealer_name]['DEALER_ADDRESS'].iloc[0]

for idx, row in df_sample_1.iterrows():
    df_sample_1.loc[idx, 'bank_address'] = lookup_addr(row['extract_name'])


    #----Address----

df_sample_A = tradesight_data_cleaned.iloc[10:20,:]
reference_address = banks_data_cleaned['DEALER_ADDRESS'].values
df_sample_A['extract_address'] = None
df_sample_A['best_score_address'] = None
df_sample_A['bank_name'] = None

t0 = time.time()
for idx, row in df_sample_A.iterrows():
    score_lis = []
    for refer in reference_address:
        score = fuzz.ratio(row['DEALER_ADDRESS'].lower(), refer.lower())
        score_lis.append(score)
        if score >= 70:
            row['extract_address'] = refer
    row['best_score_address'] = max(score_lis)
    if int(idx)%100 == 0:
        print(round(int(idx)*1/df_sample_A.shape[0], 2), " %")
print(time.time()-t0)

df_sample_2 = df_sample_A.loc[(df_sample_A['best_score_address'] >= 70)]


def lookup_name(dealer_addr):
    return banks_data_cleaned.loc[banks_data_cleaned['DEALER_ADDRESS'] == dealer_addr]['DEALER_NAME'].iloc[0]

for idx, row in df_sample_2.iterrows():
    df_sample_2.loc[idx, 'bank_name'] = lookup_name(row['extract_address'])
    
    
#----Fuzzy----


    #----Fuzzy With Name----
tradesight_data_fuzzy_name = tradesight_data_cleaned.iloc[0:,0:]
reference_name = banks_data_cleaned['DEALER_NAME'].values #numpy array(will run faster)
tradesight_data_fuzzy_name['extract_name'] = None
tradesight_data_fuzzy_name['best_score_name'] = None


t0 = time.time()
for idx, row in tradesight_data_fuzzy_name.iterrows():
    score_lis = []
    for refer in reference_name:
        score = fuzz.ratio(row['DEALER_NAME'].lower(), refer.lower())
        score_lis.append(score)
        if score >= 70:#threshold here
            row['extract_name'] = refer
    row['best_score_name'] = max(score_lis)
    if int(idx)%500 == 0:
        print(round(int(idx)*100/tradesight_data_fuzzy_name.shape[0], 2), " %")
print(time.time()-t0)

tradesight_data_fuzzy_name_1 = tradesight_data_fuzzy_name.loc[(tradesight_data_fuzzy_name['best_score_name'] >= 70)]


tradesight_data_fuzzy_name['bank_address'] = None

def lookup_addr(dealer_name):
    return banks_data_cleaned.loc[banks_data_cleaned['DEALER_NAME'] == dealer_name]['DEALER_ADDRESS'].iloc[0]

for idx, row in tradesight_data_fuzzy_name_1.iterrows():
    tradesight_data_fuzzy_name_1.loc[idx, 'bank_address'] = lookup_addr(row['extract_name'])

tradesight_data_fuzzy_name_1.to_csv('Fuzzy_Name.11.03.csv')


    #----Fuzzy With Address----

tradesight_data_fuzzy_addr = tradesight_data_cleaned.iloc[0:,0:]
reference_address = banks_data_cleaned['DEALER_ADDRESS'].values
tradesight_data_fuzzy_addr['extract_address'] = None
tradesight_data_fuzzy_addr['best_score_address'] = None


t0 = time.time()
for idx, row in tradesight_data_fuzzy_addr.iterrows():
    score_lis = []
    for refer in reference_address:
        score = fuzz.ratio(row['DEALER_ADDRESS'].lower(), refer.lower())
        score_lis.append(score)
        if score >= 70:#threshold here
            row['extract_address'] = refer
    row['best_score_address'] = max(score_lis)
    if int(idx)%500 == 0:
        print(round(int(idx)*100/tradesight_data_fuzzy_addr.shape[0], 2), " %")
print(time.time()-t0)


tradesight_data_fuzzy_addr_1 = tradesight_data_fuzzy_addr.loc[(tradesight_data_fuzzy_addr['best_score_address'] >= 70)]


tradesight_data_fuzzy_addr['bank_name'] = None

def lookup_name(dealer_addr):
    return banks_data_cleaned.loc[banks_data_cleaned['DEALER_ADDRESS'] == dealer_addr]['DEALER_NAME'].iloc[0]

for idx, row in tradesight_data_fuzzy_addr_1.iterrows():
    tradesight_data_fuzzy_addr_1.loc[idx, 'bank_name'] = lookup_name(row['extract_address'])

tradesight_data_fuzzy_addr_1.to_csv('Fuzzy_Address.11.03.csv')








