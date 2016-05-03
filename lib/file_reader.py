import StringIO
import csv

class FileReader(object):
	FILE_ALL_COMPANIES = 'data/companylist.csv'
	FILE_NDX100_COMPANIES = 'data/nasdaq100.csv'

	def __init__(self):
		self._all_companies = csv.reader(open(self.FILE_ALL_COMPANIES, 'rb'))
		self._ndx100_companies = csv.reader(open(self.FILE_NDX100_COMPANIES, 'rb'))

	def read_all_companies(self):
		"Return a list of all companies with desired attributes."

		# skip header
		self._all_companies.next()
		all_company_list = [] 
		for row in self._all_companies:
			# remove unused columns
			del row[-1], row[7], row[4], row[2]
			all_company_list.append(row)
		return all_company_list

	def read_ndx_companies(self):
		"Return a list of nadasqa 100's companies with desired attributes."

		# skip header
		self._ndx100_companies.next()
		nasdaq_name_list = []
		for row in self._ndx100_companies:
			nasdaq_name_list.append(row[0])

		all_company_list = self.read_all_companies()
		# add company form all company list to final list 
		# if that company is in the nasdaq_name_list
		companies = [c for c in all_company_list if c[0] in nasdaq_name_list]
		return companies
