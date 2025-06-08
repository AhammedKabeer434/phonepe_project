import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine # create_engine will help in inserting bulk datas in the table
import pandas as pd #reading file

connection=psycopg2.connect(
    host='localhost',
    port='5432',
    user='postgres',
    password='Kabeer434',
)

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor=connection.cursor()

cursor.execute('create database phonepe_db')

host='localhost'
port='5432'
database='phonepe_db'
user='postgres'
password='Kabeer434'

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

df=pd.read_csv("aggregated_transactions.csv")

table_name="phonepe"

df.to_sql(table_name,engine,index=False,if_exists="replace") # other options for if_exists = "append", "fail", "replace"