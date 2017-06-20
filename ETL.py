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

def write_to_csv_and_copy_to_db(df, file_name):
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


def normalize_json_response(url):
	response = requests.get(url)
	data = json_normalize(response.json())
	return data

# for x in split:
# 	print('h'+x)

old_stdout = sys.stdout
result = StringIO()
sys.stdout = result
print_all_unique_names()
sys.stdout = old_stdout
output_string = result.getvalue()
split = output_string.split('h')

def generate_url_and_retrieve_data():
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



# concat_data = pd.concat([gender_0, gender_1, gender_2, gender_3, gender_4, gender_5, gender_6, gender_7, gender_8, gender_9, gender_10, gender_11, gender_12, gender_13, gender_14])
# gender_data = concat_data.reset_index(drop=True)
# gender_data = gender_data.rename(columns={'name':'first_names'})





data = load_json_data("http://www.veterans.gc.ca/xml/jsonp/app.cfc?method=remoteGetAllCasualtyInfo&start=1&end=500&callback=?&language=en")
df = normalize_data(data['casualties'])
#establish connection object to hosted pg db
engine = create_engine('postgresql://root:backstagetesting123456789@backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com:5432/prod')
#build schema
sql_command = open('/Users/schakravorty/Development/Backstage/schema.sql', 'r').read()
build_schema(sql_command, engine)

"""load data via psql terminal commands"""
#psql --host=backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com --port=5432 --username=root --dbname=prod --password
#\COPY canadian_war_memorial FROM '~/Development/Backstage/canadian_memorial.csv'  CSV HEADER
#COPY 499

#process via genderize's API

#extract only the first word from each string for easy mapping
df['first_names'] = df['forenames'].apply(lambda x: x.split(' ', 1)[0])
#determine the distinct names
unique_names = df['first_names'].unique()
chunked_list = list(chunks(unique_names, 10))
gender_data = generate_url_and_retrieve_data()


# genderize_url_0 = "https://api.genderize.io/?name[0]=JAMES&name[1]=WILLIAM&name[2]=EDWARD&name[3]=COLIN&name[4]=ROBERT&name[5]=ERIC&name[6]=VICTOR&name[7]=FREDERICK&name[8]=JOHN&name[9]=PETER"
# genderize_url_1 = "https://api.genderize.io/?name[0]=FRANK&name[1]=HERBERT&name[2]=DAVID&name[3]=GUY&name[4]=ARTHUR&name[5]=LEONARD&name[6]=LAURANCE&name[7]=CHARLES&name[8]=LESLIE&name[9]=CARLETON"
# genderize_url_2 = "https://api.genderize.io/?name[0]=PEARLY&name[1]=JOSEPH&name[2]=HEDLEY&name[3]=DANIEL&name[4]=THOMAS&name[5]=WALTER&name[6]=GORDON&name[7]=LAWRENCE&name[8]=GEORGE&name[9]=EUGENE"
# genderize_url_3 = "https://api.genderize.io/?name[0]=ALEXANDER&name[1]=JAY&name[2]=ELTON&name[3]=SYDNEY&name[4]=ALFRED&name[5]=HENRY&name[6]=RICHARD&name[7]=ISAAC&name[8]=NORMAN&name[9]=SIMON"
# genderize_url_4 = "https://api.genderize.io/?name[0]=HARRY&name[1]=FERRIER&name[2]=HECTOR&name[3]=PATRICK&name[4]=LE&name[5]=HUGH&name[6]=HARVEY&name[7]=ARCHIBALD&name[8]=NEIL&name[9]=MARWOOD"
# genderize_url_5 = "https://api.genderize.io/?name[0]=AENEAS&name[1]=DONALD&name[2]=FRED&name[3]=HOWARD&name[4]=PHILIP&name[5]=ALBERT&name[6]=WILFRID&name[7]=CLIFFORD&name[8]=WRIGHT&name[9]=SAMUEL"
# genderize_url_6 = "https://api.genderize.io/?name[0]=RAY&name[1]=ALLAN&name[2]=RANDALL&name[3]=FRANCIS&name[4]=WILFORD&name[5]=DAMIEN&name[6]=CECIL&name[7]=HAROLD&name[8]=SIDNEY&name[9]=MATTHEW"
# genderize_url_7 = "https://api.genderize.io/?name[0]=GLENN&name[1]=ERNEST&name[2]=WEBSTER&name[3]=STANLEY&name[4]=WILFRED&name[5]=BARNEY&name[6]=NICOLAS&name[7]=OLIVER&name[8]=MORLEY&name[9]=MICHAEL"
# genderize_url_8 = "https://api.genderize.io/?name[0]=EVAN&name[1]=WRAY&name[2]=FILMER&name[3]=NERO&name[4]=GOLIATH&name[5]=GERALD&name[6]=JEAN&name[7]=FREEMAN&name[8]=PAUL&name[9]=NORVAL"
# genderize_url_9 = "https://api.genderize.io/?name[0]=PERCY&name[1]=RAYMOND&name[2]=HUBERT&name[3]=EARNEST&name[4]=KENNETH&name[5]=SANDFORD&name[6]=REUBEN&name[7]=BAZIL&name[8]=REGINALD&name[9]=TRUEMAN"
# genderize_url_10 = "https://api.genderize.io/?name[0]=JOSEPHAT&name[1]=ROY&name[2]=OSMOND&name[3]=GRAHAM&name[4]=ROLAND&name[5]=RALPH&name[6]=ELLIS&name[7]=TYLER&name[8]=ARMAND&name[9]=ALPHONSE"
# genderize_url_11 = "https://api.genderize.io/?name[0]=DELBERT&name[1]=ANGUS&name[2]=ORVAL&name[3]=JERRY&name[4]=WILLARD&name[5]=PERCIVAL&name[6]=GROVER&name[7]=BERNARD&name[8]=NICHOLAS&name[9]=HENRI"
# genderize_url_12 = "https://api.genderize.io/?name[0]=EMERSON&name[1]=HAZARD&name[2]=ABEL&name[3]=BENJAMIN&name[4]=LAURENCE&name[5]=MARK&name[6]=LEMOYNE&name[7]=FREDRICK&name[8]=FRANKLIN&name[9]=DANTE"
# genderize_url_13 = "https://api.genderize.io/?name[0]=AMOS&name[1]=RAE&name[2]=MURRAY&name[3]=EDOUARD&name[4]=PATTERSON&name[5]=LOUIS&name[6]=STEPHEN&name[7]=LEWIS&name[8]=FLOYD&name[9]=RODERICK"
# genderize_url_14 = "https://api.genderize.io/?name[0]=ANDREW&name[1]=GARFIELD"

# gender_0 = normalize_json_response(genderize_url_0)
# gender_1 = normalize_json_response(genderize_url_1)
# gender_2 = normalize_json_response(genderize_url_2)
# gender_3 = normalize_json_response(genderize_url_3)
# gender_4 = normalize_json_response(genderize_url_4)
# gender_5 = normalize_json_response(genderize_url_5)
# gender_6 = normalize_json_response(genderize_url_6)
# gender_7 = normalize_json_response(genderize_url_7)
# gender_8 = normalize_json_response(genderize_url_8)
# gender_9 = normalize_json_response(genderize_url_9)
# gender_10 = normalize_json_response(genderize_url_10)
# gender_11 = normalize_json_response(genderize_url_11)
# gender_12 = normalize_json_response(genderize_url_12)
# gender_13 = normalize_json_response(genderize_url_13)
# gender_14 = normalize_json_response(genderize_url_14)

# concat_data = pd.concat([gender_0, gender_1, gender_2, gender_3, gender_4, gender_5, gender_6, gender_7, gender_8, gender_9, gender_10, gender_11, gender_12, gender_13, gender_14])
# gender_data = concat_data.reset_index(drop=True)
# gender_data = gender_data.rename(columns={'name':'first_names'})

"""join data with pd.merge on 'first_names'"""

merged_data = pd.merge(df, gender_data, on='first_names', how='inner')
merged_data['count'] = merged_data['count'].fillna(0.0).astype(int)

write_to_csv_and_copy_to_db(merged_data, 'canadian_memorial.csv')

def main():
	return None

if __name__ == "__main__":
	main()