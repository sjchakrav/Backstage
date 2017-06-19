import json
import urllib
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import psycopg2
from sqlalchemy import create_engine

"""request url and retrieve JSON data and normalize data"""

def load_json_data(url):
	response = urllib.urlopen(url)
	data = json.loads(response.read())
	return data

data = load_json_data("http://www.veterans.gc.ca/xml/jsonp/app.cfc?method=remoteGetAllCasualtyInfo&start=1&end=500&callback=?&language=en")

#normalize json data structure and store into a pandas dataframe 
df = json_normalize(data['casualties'])


#GET https://api.genderize.io/?name[0]=peter&name[1]=lois&name[2]=stevie
#GET https://api.genderize.io/?name=kim&country_id=dk

engine = create_engine('postgresql://root:backstagetesting123456789@backstage.ctjtojl7pj7i.us-west-2.rds.amazonaws.com:5432/backstage')

df.to_sql('canadian_war_memorial', engine, if_exists='append', index=True)

copy_command = \
"""
COPY zip_codes FROM '~/Development/Backstage/canadian_war_memorial.csv' DELIMITER ',' CSV
;"""


connection_string = "host='backstage.ctjtojl7pj7i.us-west-2.rds.amazonaws.com' port='5432' dbname='backstage' user='root' password='backstagetesting123456789' "

def db_connection(connection_string):
	conn = psycopg2.connect(connection_string)
	return conn

# schema_query = open("schema.sql", "r").read()

db_connection = db_connection(connection_string)

cur = db_connection.cursor()

response = cur.execute(schema_query)

create_schema()


def main():
	return None

if __name__ == "__main__":
	main()