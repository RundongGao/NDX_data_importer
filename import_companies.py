import psycopg2
import StringIO
import csv
import pdb

def calc_market_cap(s):
	"Convert a string ending with B(as biliion) of M(as million) to float(in million)"
	result = -1
	if s[-1] == 'B':
		result = float(s[1:-1]) * 1000
	elif s[-1] == 'M':
		result = float(s[1:-1])
	else:
		print 'unknow market_cap_m formate'
	return result

def read_all_companies():
	"Return a list of all companies with desired attributes."
	all_companies_file = open('companylist.csv', 'rb')
	all_companies_reader = csv.reader(all_companies_file)

	# skip header
	all_companies_reader.next()
	all_company_list = [] 
	for row in all_companies_reader:
		# remove unused columns
		del row[-1], row[7], row[4], row[2]
		all_company_list.append(row)
	return all_company_list


def read_ndx_companies():
	"Return a list of nadasqa 100's companies with desired attributes."
	nasdaq_file = open('nasdaq100.csv', 'rb')
	nasdaq_reader = csv.reader(nasdaq_file )

	nasdaq_reader.next()
	nasdaq_name_list = []
	for row in nasdaq_reader:
		nasdaq_name_list.append(row[0])

	all_company_list = read_all_companies()
	# add company form all company list to final list 
	# if that company is in the nasdaq_name_list
	companies = [c for c in all_company_list if c[0] in nasdaq_name_list]
	return companies

def import_ndx_companies(ndx_companies):
	"""
	Take a list of companies with desired attributes 
	and insert into database
	"""
	conn = psycopg2.connect("dbname='nsd_100' user='nsd_100_admin' host='localhost' password='-<3Y%Y\"_xfr?nai'")
	cur = conn.cursor()
	for comp in ndx_companies:
		clause =  "INSERT INTO companies (symbol,name,market_cap_m,sector,industory) VALUES (%s, %s, %d, %s, %s)" % ("'" + comp[0] + "'", "'" + comp[1]+ "'" , calc_market_cap(comp[2]), "'" + comp[3] + "'", "'" + comp[4] + "'")
		cur.execute(clause)

	conn.commit()
	conn.close()

# executing
import_ndx_companies(read_ndx_companies())
