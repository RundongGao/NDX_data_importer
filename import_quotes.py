from __future__ import print_function
import sys
sys.path.append('lib/')
from database_caller import DataBase
from yahoo_caller import YahooCaller

def usage():
	print("Usage:", file=sys.stderr)
	print("\tDEFAULT       ::  python import_quotes", file=sys.stderr)
	print("\tANY DATABASE  ::  python import_quotes dbname=DBNAME user=USER host=HOST password=PASSWORD", file=sys.stderr)

def main(argv):
	if len(argv) == 1:
		db = DataBase()
	elif len(argv) == 5:
		db = DataBase(argv[1], argv[2], argv[3], argv[4])
	else:
		usage()
		sys.exit(1)

	companies = db.fetch_ndx_company_names()
	for company in companies:
		symbol = company[0]
		data = YahooCaller.fetch_quotes(symbol)
		if data:
			db.import_quotes(symbol,data)
		else:
			continue
	db.close()


if __name__ == '__main__':
	main(sys.argv)


