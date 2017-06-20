import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def build_schema(sql_command, engine):
    with engine.connect() as conn:
        conn.execute(sql_command)
        return "SUCCESS"


def main():
    # establish connection object to hosted pg db
    engine = create_engine('postgresql://root:backstagetesting123456789@backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com:5432/prod')
    # build schema
    sql_command = open('/Users/schakravorty/Development/Backstage/schema.sql', 'r').read()
    if build_schema(sql_command, engine) == "SUCCESS":
        print('SCHEMA HAS BEEN BUILT.')

if __name__ == "__main__":
    main()