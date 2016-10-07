# check shingle demand databases on GS18 and Robin
# to see if reports have been imported over the past 10 days

import psycopg2
import ConfigParser

# config file for GS18
confile1=r"C:\Users\thowingt\Documents\Code\python\shingle_demand.cfg"

# config file for Robin
confile2=r"C:\Users\thowingt\Documents\Code\python\shingle_demand_robin.cfg"

def get_params(cf):
    config=ConfigParser.RawConfigParser()
    config.read(cf)
    dbn=config.get('database parameters','database')
    usn=config.get('database parameters','user')
    hn=config.get('database parameters', 'host')
    pw=config.get('database parameters', 'password')
    return [dbn, usn, pw, hn]


# connect to GS18
dbn, usn, pw, hn=get_params(confile1)
conn = psycopg2.connect("dbname={0} user={1} password={2} host={3}".format(dbn, usn, pw, hn))
cur=conn.cursor()

# z_reports is a stored procedue that performs a set of queries adn joins the results in a nice way.
q_line="select * from z_reports()"
cur.execute(q_line)

print "GS20 roof_hail"
print '{:10s}'.format("date"),'{:>5s}'.format("hail"),'{:>5s}'.format("wind"),'{:>5s}'.format("bts")
for record in cur:
	print record[0], '{:5,d}'.format(int(record[1])), \
		'{:5,d}'.format(int(record[2])),\
		'{:5,d}'.format(int(record[3]))
print "\n"
conn.close()


# connect to Robin
dbn, usn, pw, hn=get_params(confile2)
conn = psycopg2.connect("dbname={0} user={1} password={2} host={3}".format(dbn, usn, pw, hn))
cur=conn.cursor()

q_line="select * from z_reports()"
cur.execute(q_line)

print "Robin shingle_demand"
print '{:10s}'.format("date"),'{:>5s}'.format("hail"),'{:>5s}'.format("wind"),'{:>5s}'.format("bts")
for record in cur:
	print record[0], '{:5,d}'.format(int(record[1])), \
		'{:5,d}'.format(int(record[2])),\
		'{:5,d}'.format(int(record[3]))

conn.close()

raw_input()
