import json
import urllib
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import psycopg2
from sqlalchemy import create_engine
import subprocess
import requests
from __future__ import print_function
from StringIO import StringIO
import sys

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

def write_to_csv(df, file_name):
	df.to_csv(file_name, encoding='utf-8', index=True, index_label='id')

def build_schema(sql_command, engine):
	with engine.connect() as conn:
		conn.execute(sql_command)

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

def print_all_unique_names():
	base_url = "https://api.genderize.io/?"
	for i in xrange(0, 15):
		print(base_url, end='')
		for i2, x in enumerate(chunked_list[i]):
			print("name[{0}]={1}&""".format(i2, x), end='')


def get_print_out():
	old_stdout = sys.stdout
	result = StringIO()
	sys.stdout = result
	print_all_unique_names()
	sys.stdout = old_stdout
	output_string = result.getvalue()
	split = output_string.split('h')
	return split

def normalize_json_response(url):
	response = requests.get(url)
	data = json_normalize(response.json())
	return data


def generate_url_and_retrieve_data(split):
	appended_data = []
	for i, url in enumerate(split):
		if (i > 0) & (i <= len(split)):
			set_url = 'h' + url
			data = normalize_json_response(set_url)
			appended_data.append(data)	
	appended_data = pd.concat(appended_data, axis=0)
	appended_data.reset_index(drop=True)
	appended_data = appended_data.rename(columns={'name':'first_names'})
	return appended_data


def main():
	data = load_json_data("http://www.veterans.gc.ca/xml/jsonp/app.cfc?method=remoteGetAllCasualtyInfo&start=1&end=500&callback=?&language=en")
	df = normalize_data(data['casualties'])

	#establish connection object to hosted pg db
	engine = create_engine('postgresql://root:backstagetesting123456789@backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com:5432/prod')
	#build schema
	sql_command = open('/Users/schakravorty/Development/Backstage/schema.sql', 'r').read()
	build_schema(sql_command, engine)

	#process via genderize's API
	#extract only the first word from each string for easy mapping
	df['first_names'] = df['forenames'].apply(lambda x: x.split(' ', 1)[0])
	#determine the distinct names
	unique_names = df['first_names'].unique()
	chunked_list = list(chunks(unique_names, 10))
	split = get_print_out()
	gender_data = generate_url_and_retrieve_data(split)

	"""join data with pd.merge on 'first_names'"""

	merged_data = pd.merge(df, gender_data, on='first_names', how='inner')
	merged_data['count'] = merged_data['count'].fillna(0.0).astype(int)

	write_to_csv(merged_data, 'canadian_memorial.csv')

	"""load data via psql terminal commands"""
	#psql --host=backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com --port=5432 --username=root --dbname=prod --password
	#\COPY canadian_war_memorial FROM '~/Development/Backstage/canadian_memorial.csv'  CSV HEADER
	#COPY 499

if __name__ == "__main__":
	main()