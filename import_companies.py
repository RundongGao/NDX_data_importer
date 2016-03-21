import psycopg2
import StringIO
import csv
import pdb

def calc_market_cap(s):
	"this function convert a string ending with B(biliion) of M(million) to int (in million)"
	result = -1
	if s[-1] == 'B':
		result = float(s[1:-1]) * 1000
	elif s[-1] == 'M':
		result = float(s[1:-1])
	else:
		print 'unknow market_cap_m formate'
	return result


companies_file = open('companylist.csv', 'rb')
big_table = csv.reader(companies_file)

big_table.next()
all_company_list = [] 
for row in big_table:
	del row[-1], row[7], row[4], row[2]
	all_company_list.append(row)


nasdaq_file = open('nasdaq100.csv', 'rb')
small_table = csv.reader(nasdaq_file )

small_table.next()
nasdaq_list = []
for row in small_table:
	nasdaq_list.append(row[0])

companies = [comp for comp in all_company_list if comp[0] in nasdaq_list]

conn = psycopg2.connect("dbname='nsd_100' user='nsd_100_admin' host='localhost' password='-<3Y%Y\"_xfr?nai'")
cur = conn.cursor()

for comp in companies:
	clause =  "INSERT INTO companies (symbol,name,market_cap_m,sector,industory) VALUES (%s, %s, %d, %s, %s)" % ("'" + comp[0] + "'", "'" + comp[1]+ "'" , calc_market_cap(comp[2]), "'" + comp[3] + "'", "'" + comp[4] + "'")
	cur.execute(clause)

conn.commit()
conn.close()

#with open('companylist.csv', 'w') as big_table:

#	headers = big_table.next

	# with open('nasdaq100.csv','w') as small_table:
