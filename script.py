import psycopg2 as psc
from time import gmtime, strftime
import urllib2
from datetime import date, timedelta
response = urllib2.urlopen('https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml')
html = response.read()
##### Define function for nth occurance
def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start
def add_months(sourcedate,months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month / 12
    month = month % 12 + 1
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    return datetime.date(year,month,day)
######   define variables############
cur = 'Cube currency='
rate = 'rate='
date1 = 'Cube time='
total_count = html.count(cur)
strt = 1
sgd_rate=float(html[html.find('SGD')+len('SGD')+find_nth(html[html.find('SGD')+len('SGD'):],"'",2)+1:find_nth(html[html.find('SGD')+len('SGD'):],"'",3)+html.find('SGD')+len('SGD')])
##temp_date = (html[find_nth(html, date1, 1)+len(date1)+1:find_nth(html, date1,1)+len(date1)+1+find_nth(html[find_nth(html, date1, 1)+len(date1)+1:],"'",1)])
temp_date = (date.today() - timedelta(0)).strftime("%Y-%m-%d")
##print("done")
##temp_date = date(*map(int, date_string.split('-'))).strftime('%Y-%m-%d')
##print("done")
######## db connection #########3
#This method connects to the given database 
dbname = 'BusinesInteligence'
print "Connecting to DB"
dbcon = psc.connect(host="businessreporting.c8kroqbohqdf.us-west-2.rds.amazonaws.com", user="vagh_nod_167", password="salt**hjk878", port =5432, dbname=dbname)
print "Connected DB"
cursor = dbcon.cursor()
cur_timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
###########   start loops         ##########
while(strt<=total_count):
    temp_cur = html[find_nth(html, cur, strt)+len(cur)+1:find_nth(html, cur, strt)+len(cur)+1+find_nth(html[find_nth(html, cur, strt)+len(cur)+1:],"'",1)]
    print("done")
    temp_rate = float(html[find_nth(html, rate, strt)+len(rate)+1:find_nth(html, rate, strt)+len(rate)+1+find_nth(html[find_nth(html, rate, strt)+len(rate)+1:],"'",1)])
    actual_rate = round(temp_rate/sgd_rate,4)
    print("done")
    #temp_date = add_months(datetime.datetime(*[int(item) for item in x.split('-')]), 1).strftime("%Y-%m-%d")
    query = "insert into cur_conversion (currency, base_currency, rate, rate_date, created_at ) values ('%s','SGD', %s , '%s',TIMESTAMP '%s')" % (temp_cur,actual_rate,temp_date,cur_timestamp)
    print(temp_cur)
    print(actual_rate)
    print(temp_date)
    cursor.execute(query)
    print "Query Executed , Inserted result for " + str(strt) 
    strt = strt+1
dbcon.commit()
print "committed " +strftime("%Y-%m-%d %H:%M:%S", gmtime())
dbcon.close()
print "Conn Close Connection Closed " +strftime("%Y-%m-%d %H:%M:%S", gmtime())
