from __future__ import print_function
import sys
sys.path.append('lib/')
from database_caller import DataBase
from file_reader import FileReader

def usage():
	print("Usage:", file=sys.stderr)
	print("\tDEFAULT       ::  python import_companies", file=sys.stderr)
	print("\tANY DATABASE  ::  python import_companies dbname=DBNAME user=USER host=HOST password=PASSWORD", file=sys.stderr)

def main(argv):
	if len(argv) == 1:
		db = DataBase()
	elif len(argv) == 5:
		db = DataBase(argv[1], argv[2], argv[3], argv[4])
	else:
		usage()
		sys.exit(1)

	data = FileReader().read_ndx_companies()
	db.import_ndx_companies(data)
	db.close()


if __name__ == '__main__':
    main(sys.argv)
