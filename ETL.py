import json
import urllib
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import psycopg2
from sqlalchemy import create_engine
import subprocess
import requests

"""request url and retrieve JSON data and normalize data"""

def load_json_data(url):
	response = urllib.urlopen(url)
	data = json.loads(response.read())
	return data

#normalize json data structure and store into a pandas dataframe
def normalize_data(data):
	df = json_normalize(data)
	df.columns = map(unicode.lower, df.columns)
	return df

def write_to_csv_and_copy_to_db(file_name):
	df.to_csv(file_name, encoding='utf-8', index=True, index_label='id')

def build_schema(sql_command, engine):
	with engine.connect() as conn:
		conn.execute(sql_command)

data = load_json_data("http://www.veterans.gc.ca/xml/jsonp/app.cfc?method=remoteGetAllCasualtyInfo&start=1&end=500&callback=?&language=en")

df = normalize_data(data['casualties'])
write_to_csv_and_copy_to_db('canadian_memorial.csv')
#establish connection object to hosted pg db
engine = create_engine('postgresql://root:backstagetesting123456789@backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com:5432/prod')
#build schema
sql_command = open('/Users/schakravorty/Development/Backstage/schema.sql', 'r').read()
build_schema(sql_command, engine)



connection = engine.connect()
connection.copy_from("canadian_memorial.csv", 'canadian_war_memorial')
"""load data via psql terminal commands"""

#psql --host=backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com --port=5432 --username=root --dbname=prod --password
#\COPY canadian_war_memorial FROM '~/Development/Backstage/canadian_memorial.csv'  CSV HEADER
#COPY 499

#process via genderize's API

#extract only the first word from each string for easy mapping
df['first_names'] = df['forenames'].apply(lambda x: x.split(' ', 1)[0])
#determine the distinct names
unique_names = df['first_names'].unique()

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]


chunked_list = list(chunks(unique_names, 10))



for i, x in enumerate(df['forenames'], n):
	print """?name[{0}]={1}&""".format(i,x)








genderize_url = "https://api.genderize.io/?name[0]=JAMES"
response = requests.get(genderize_url)


def main():
	return None

if __name__ == "__main__":
	main()