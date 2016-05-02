import requests
import StringIO
import csv

class YahooCaller(object):
	URL_BASE = "http://ichart.finance.yahoo.com/table.csv?"

	@classmethod
	def fetch_quotes(cls, company):
		"""
		Takes a company symbol string
		Return a csv reader of quotes for 2014 of the given company
		"""
		flags = "s=%s&a=0&b=1&c=2014&d=11&e=31&f=2014&g=d&ignore=.csv" % company
		response = requests.get(cls.URL_BASE + flags)
		if response.status_code == 404:
			print "cann't get 2014 historical quotes for %s" % company
			return
		text = StringIO.StringIO(response.text)
		return csv.reader(text, delimiter=',')

