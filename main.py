import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()


# storing and fetching credentials
db_password = os.getenv("AIVEN_PASSWORD")
db_hostname = os.getenv("AIVEN_HOSTNAME")
db_port = os.getenv("AIVEN_PORT")
db_name = os.getenv("AIVEN_DBNAME")
db_user = os.getenv("AIVEN_USERNAME")


# extract csv values and store in df
df = pd.read_csv('amazon.csv')


# clean dataframe
df['discounted_price'] = df['discounted_price'].str.replace('₹', '').str.replace(',','')
df['actual_price'] = df['actual_price'].str.replace('₹','').str.replace(',','')
df['discount_percentage'] = df['discount_percentage'].str.replace('%','')
df['rating_count'] = df['rating_count'].str.replace(',','')


df.dropna(subset=['discounted_price', 'actual_price', 'discount_percentage', 'rating_count'])


engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_hostname}:/defaultdb?sslmode=require')
df.to_sql('amazon_products',engine, if_exists='replace', index=False)

