import psycopg2

class DataBase(object):
	def database_call_wraper(func):
		def new_func(self, *args):
			self._cur = self._conn.cursor()
			result = func(self,*args)
			self._conn.commit()
			return result
		return new_func

	def __init__(self, dbname='nsd_100', user='nsd_100_admin', host='localhost', password='-<3Y%Y\"_xfr?nai'):
		self._conn = psycopg2.connect(("dbname=%s user=%s host=%s password=%s") % ( dbname, user, host ,password ))

	def close(self):
		self._conn.close()

	@database_call_wraper
	def fetch_ndx_company_names(self):
		"Retrun all company name in database"
		self._cur.execute("""SELECT symbol from companies""")
		companies = self._cur.fetchall()
		return companies

	@database_call_wraper
	def import_quotes(self,company, data):
		"""
		Take a csv reader with quotes data
		And insert quotes data into database
		"""
		for index, entry in enumerate(data):
			if index == 0:
				continue
			try:
				clause =  "INSERT INTO quotes (symbol,date,open,close,high,low,volume,adj_close ) VALUES (%s, %s, %f, %f, %f, %f, %d, %f)" % ("'" + company + "'", "'" + entry[0]+ "'" , float(entry[1]),float(entry[2]), float(entry[3]), float(entry[4]),int(entry[5]), float(entry[6]))
				self._cur.execute(clause)
			except:
				print "Unexpected error:", sys.exc_info()[0]

	@database_call_wraper
	def import_ndx_companies(self, ndx_companies):
		"""
		Take a list of companies with desired attributes 
		and insert into database
		"""
		for comp in ndx_companies:
			clause =  "INSERT INTO companies (symbol,name,market_cap_m,sector,industory) VALUES (%s, %s, %d, %s, %s)" % ("'" + comp[0] + "'", "'" + comp[1]+ "'" , self.calc_market_cap(comp[2]), "'" + comp[3] + "'", "'" + comp[4] + "'")
			self._cur.execute(clause)

	@staticmethod
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

