## FOREWORD ##
## I'll be honest here, sir. I don't quite understand this whole thing,
## even after rewatching the video recording a couple times, but I'll try.
## As you said it yourself this isn't something that can be done in hours,
## plus it's an actual technical assessment and I don't think my skills are quite there yet.
## But again, I shall try.
## P.S: ( (classwork.submission()==1 & classwork.quality()=="BAD") ) > (classwork==NULL) == TRUE
## or something, I suppose.

import json
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

schema_json = '/sql/schemas/user_address.json'
create_schema_sql = """create table user_address_2018_snapshots {};"""
zip_small_file = '/data/dataset-small.zip'
result_ingestion_check_sql = '/sql/queries/result_ingestion_user_address.sql'
small_file_name = 'dataset-small.csv'
database='shipping_orders'
user='postgres'
password=''
host='127.0.0.1'
port='5432'
table_name = 'user_address_2018_snapshots'



with open(schema_json, 'r') as schema:
    content = json.loads(schema.read())

list_schema = []
for c in content:
     col_name = c['column_name']
     col_type = c['column_type']
     constraint = c['is_null_able']
     ddl_list = [col_name, col_type, constraint]
     list_schema.append(ddl_list)

list_schema_2 = []
for l in list_schema:
     s = ' '.join(l)
     list_schema_2.append(s)

create_schema_sql_final = create_schema_sql.format(tuple(list_schema_2)).replace("'", "")

#Init Posgres conn
conn = pg.connect(database=database,
                  user=user,
                  password=password,
                  host=host,
                  port=port)

conn.autocommit=True
cursor=conn.cursor()

try:
    cursor.execute(create_schema_sql_final)
    print("DDL schema created succesfully...")
except pg.errors.DuplicateTable:
    print("table already created...")


#Load zipped file to dataframe
zf = ZipFile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name), header=None)


col_name_df = [c['column_name'] for c in content]
df.columns = col_name_df

df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] < '2018-12-31')]

#create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

#insert to postgres
df_filtered.to_sql(table_name, engine, if_exists='append', index=False) 


print(f'Total inserted rows: {len(df_filtered)}')
print(f'Inital created_at: {df_filtered.created_at.min()}')
print(f'Last created_at: {df_filtered.created_at.max()}')

cursor.execute(open(result_ingestion_check_sql, 'r').read())
result = cursor.fetchall()
print(result) # 88rising ^