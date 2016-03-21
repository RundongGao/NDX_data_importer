import psycopg2
import pdb

conn = psycopg2.connect("dbname='ndx100' user='ndx_100_admin' host='ndx100.c3n3fi9grsp4.us-west-2.rds.amazonaws.com' password='EnihRGPD5TYPEIJ'")

cur = conn.cursor()
cur.execute("""SELECT * from companies limit 10""")
rows = cur.fetchall()

for row in rows:
	symbol = row[0]
	url = "http://ichart.finance.yahoo.com/table.csv?s=%s&a=0&b=1&c=2014&d=11&e=31&f=2014&g=d&ignore=.csv" % symbol
	response = requests.get(url)
	print response
