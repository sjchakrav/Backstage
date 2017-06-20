To install the requirements for this python script, simple run: pip install -r requirements.txt

To run the script: python2.7 ETL.py

This python script utilizes pandas and retrieves data from the Canadian virtual war memorial, normalizes the JSON structure, and processes the data through use of the genderize.io API and saves the data into a CSV format for easy access via pandas or another analytics library that can read in data in a CSV format. 

I've also hosted a free-tier postgres database instance on AWS RDS along with a read replica, with a read-only user for easy querability via SQL. The python script also establishes a connection to the database and builds the schema (schema.sql).

-Copied the CSV directly into the database via psql:

\COPY canadian_war_memorial FROM '~/Path/Backstage/canadian_memorial.csv'  CSV HEADER

username = 'readonly'
password = 'readonly'
host = 'backstageinstance.ctjtojl7pj7i.us-west-2.rds.amazonaws.com'
port = '5432'
database = 'prod'


- Of Canadian names that were considered masculine in various years in the past, what percentage are now gender-ambiguous?

SELECT MIN(probability), first_names, gender
FROM canadian_war_memorial
GROUP BY first_names, gender
ORDER BY 1 ASC;

-> this would give us the probability of the names stacked up with the gender 

- Which branches of the Canadian military forces have employed the most men whose names are now gender-ambiguous?

-> using the force and force_desc columns, we can see that there are 2 distinct values (Army and Air Force). We can further filter this down on the regiment information, and when matching it to the users that now have low probability associated with their name being male and doing acount on those - we should be on the right track towards answering the question. 


- What percentage of Canadian men died on the same day of the month as they were born?
	With pandas:

	len(df[(df['day_date_of_death']==df['day_date_of_birth'])])/len(df)*100 # 1.8036072144288577
	
	OR

SELECT y.same_day, x.total, (y.same_day::float/x.total::float*100) as percentage
FROM
(
	SELECT COUNT(id) as same_day
	FROM canadian_war_memorial
	WHERE day_date_of_death = day_date_of_birth
) y
CROSS JOIN
(
	SELECT COUNT(id) as total
	FROM canadian_war_memorial
) x;


- More questions to contemplate:

	-Audience segmented by gender
	-Audience plotted via age 