import psycopg2
import requests
import StringIO
import csv
import pdb

# scriping version of quote_importer
# need to do:
# 0. executemany
# 1. ood
# 2. upsert

conn = psycopg2.connect("dbname='nsd_100' user='nsd_100_admin' host='localhost' password='-<3Y%Y\"_xfr?nai'")
cur = conn.cursor()

cur.execute("""SELECT symbol from companies""")
rows = cur.fetchall()

for row in rows:
	symbol = row[0]
	url = "http://ichart.finance.yahoo.com/table.csv?s=%s&a=0&b=1&c=2014&d=11&e=31&f=2014&g=d&ignore=.csv" % symbol
	response = requests.get(url)
	if response.status_code == 404:
		continue
	f = StringIO.StringIO(response.text)
	reader = csv.reader(f, delimiter=',')
	
	for index, entry in enumerate(reader):
		if index == 0:
			continue
		try:
			clause =  "INSERT INTO quotes (symbol,date,open,close,high,low,volume,adj_close ) VALUES (%s, %s, %f, %f, %f, %f, %d, %f)" % ("'" + symbol + "'", "'" + entry[0]+ "'" , float(entry[1]),float(entry[2]), float(entry[3]), float(entry[4]),int(entry[5]), float(entry[6]))
			cur.execute(clause)
		except:
			print "Unexpected error:", sys.exc_info()[0]

conn.commit()
conn.close()

	


