To install the requirements for this python script, simple run: pip install -r requirements.txt

This python script utilizes pandas and retrieves data from the Canadian virtual war memorial, normalizes the JSON structure, and processes the data through use of the genderize.io API and saves the data into a CSV format for easy access via pandas or another analytics library that can read in data in a CSV format.

I've also hosted a free-tier postgres database instance on AWS RDS along with a read replica, with a read-only user for easy querability via SQL.

-Copied the CSV directly into the database via psql:

\COPY canadian_war_memorial FROM '~/Path/Backstage/canadian_memorial.csv'  CSV HEADER

username = 'readonly'
password = 'readonly'
host = 'backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com'
port = '5432'
database = 'prod'

