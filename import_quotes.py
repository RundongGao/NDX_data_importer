import psycopg2
import requests
import StringIO
import csv
import pdb
import sys

# scriping version of quote_importer
# need to do:
# 0. executemany
# 1. class
# 2. upsert

def fetch_ndx_company_names():
	"Retrun all company name in database"
	conn = psycopg2.connect("dbname='nsd_100' user='nsd_100_admin' host='localhost' password='-<3Y%Y\"_xfr?nai'")
	cur = conn.cursor()

	cur.execute("""SELECT symbol from companies""")
	companies = cur.fetchall()

	conn.close()
	return companies


def fetch_quotes(company):
	"""
	Takes a company symbol string
	Return a csv reader of quotes for 2014 of the given company
	"""
	url_base = "http://ichart.finance.yahoo.com/table.csv?"
	flags = "s=%s&a=0&b=1&c=2014&d=11&e=31&f=2014&g=d&ignore=.csv" % company
	response = requests.get(url_base+flags)
	if response.status_code == 404:
		print "cann't get 2014 historical quotes for %s" % company
		return
	text = StringIO.StringIO(response.text)
	return csv.reader(text, delimiter=',')

def import_quotes(company, data):
	"""
	Take a csv reader with quotes data
	And insert quotes data into database
	"""
	conn = psycopg2.connect("dbname='nsd_100' user='nsd_100_admin' host='localhost' password='-<3Y%Y\"_xfr?nai'")
	cur = conn.cursor()

	for index, entry in enumerate(data):
		if index == 0:
			continue
		try:
			clause =  "INSERT INTO quotes (symbol,date,open,close,high,low,volume,adj_close ) VALUES (%s, %s, %f, %f, %f, %f, %d, %f)" % ("'" + company + "'", "'" + entry[0]+ "'" , float(entry[1]),float(entry[2]), float(entry[3]), float(entry[4]),int(entry[5]), float(entry[6]))
			cur.execute(clause)
		except:
			print "Unexpected error:", sys.exc_info()[0]
	conn.commit()
	conn.close()

def import_all_quotes():
	companies = fetch_ndx_company_names()
	for company in companies:
		symbol = company[0]
		data = fetch_quotes(symbol)
		if data:
			import_quotes(symbol,data)
		else:
			continue

#executing
import_all_quotes()


