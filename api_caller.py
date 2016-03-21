import requests
import pdb

url = "http://ichart.finance.yahoo.com/table.csv?s=AAPL&a=0&b=1&c=2014&d=11&e=31&f=2014&g=d&ignore=.csv"
response = requests.get(url)

print response.text
