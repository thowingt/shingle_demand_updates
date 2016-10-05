import os, sys
import datetime
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase


# this script will check for any roof damage from the previous day
# if any damage was present, the top 5 rows and a summary will be emailed
# if any "events" are detected, the subject of the email is changed the the
# event columns are shaded orange


db = "dbname='shingle_demand' user='shingle_demand' password='b@dweather!' host='localhost'"

# determine the days until thursday (day of updating)
def days_until_thurs(yesterday):
    yesterday = datetime.datetime.strptime(yesterday,"%Y-%m-%d")
    # mon = 0, sun = 6
    day = yesterday.weekday()
    day_until_thurs = 3 - day
    return day_until_thurs

def determine_update_one(yesterday):
    days_until_thursday = days_until_thurs(yesterday)
    next_update = days_until_thursday + 7
    yesterday = datetime.datetime.strptime(yesterday,"%Y-%m-%d")
    thurs_delta=datetime.timedelta(days=next_update)
    event_update_one =(yesterday+thurs_delta).strftime("%Y-%m-%d")
    return event_update_one

def determine_update_two(yesterday):
    days_until_thursday = days_until_thurs(yesterday)
    next_update = days_until_thursday + 28
    yesterday = datetime.datetime.strptime(yesterday,"%Y-%m-%d")
    thurs_delta=datetime.timedelta(days=next_update)
    event_update_two =(yesterday+thurs_delta).strftime("%Y-%m-%d")
    return event_update_two

# query to pull the highest upper estimate
def determine_max_upper():
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""
    select location,state,upper from temp_eventss
    order by upper desc limit 1
    """
    cur.execute(q_line)
    return cur

# query to pull the highest expected estimate
def determine_max_expected():
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""
    select location,state,expected from temp_eventss
    order by expected desc limit 1
    """
    cur.execute(q_line)
    return cur

# query to pull the top (num) damage entries
def determine_top_damage(num):
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""
    select location,state,upper,expected,lower from
    temp_eventss order by upper desc limit """+str(num)+"""
    """
    cur.execute(q_line)
    conn.commit()
    return cur

# query to determine the total roofs expected to be damaged from the previous date
def determine_sum_expected():
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""
    (select sum(expected)
    from temp_eventss 
    )"""
    cur.execute(q_line)
    conn.commit()
    return cur

# insert the county damage query into a temp table
def temp_county_damage(date):
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""insert into temp_eventss
    (select distinct b.county, b.state, 
    sum(b.upper) over(partition by b.fips) as "SumUpper",
    sum(b.expected) over(partition by b.fips) as "SumExpected",
    sum(b.lower) over(partition by b.fips) as "SumLower"
    from damage b left join county_cbsa a 
    on a.county_id=b.fips 
    where (b.date1 = %s) 
    and a.msa_name is NULL)"""
    date_arg=[date]
    cur.execute(q_line, date_arg)
    conn.commit()

# insert the MSA damage query into a temp table
def temp_msa_damage(date):
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""insert into temp_eventss 
    select * from (
    (select distinct on (a.msa_name) a.msa_name, b.state,
    sum(b.upper) over(partition by a.msa_name) as "SumUpper",
    sum(b.expected) over(partition by a.msa_name) as "SumExpected",
    sum(b.lower) over(partition by a.msa_name) as "SumLower"
    from damage b left join county_cbsa a
    on a.county_id=b.fips
    where (b.date1 = %s)
    and a.msa_name != '')
    ) as q
    """
    date_arg=[date]
    cur.execute(q_line, date_arg)
    conn.commit()

# delete all entries from the temp damage table
def delete_temp_damage():
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""delete from temp_eventss
    """
    cur.execute(q_line)
    conn.commit()

# checks to see if any entries exceed the 10k threshold
def check_alert():
    conn = psycopg2.connect(db)
    cur = conn.cursor()
    q_line="""
    (select location,state,upper
    from temp_eventss 
    where upper > 10000 group by location, state, upper)"""
    cur.execute(q_line)
    totalEvents = cur.rowcount
    conn.commit()
    return totalEvents


# this function prepares the body of the email being sent
def create_body(date,max_upper,max_expected,sum_expected,top_5,updateOne,updateTwo):

    totalEntries = len(top_5)
    body = ''
    if updateOne != "na":
        body += '<p style="margin: 0; padding: 0;"><b>Significant hail event(s) were detected on '+date +'</b></p>'
        body += '<p style="margin: 0; padding: 0;"><b>We will provide the first revised damage estimates for the event(s) on ' + updateOne+'</b></p>'
        body += '<p style="margin: 0; padding: 0;"><b>The second revision will be provided on ' + updateTwo+'</b></p>'
        body += '<p style="margin: 0; padding: 0;">--------------------------------</p>'
        body += '<p style="margin: 0; padding: 0;">--------------------------------</p>'
        
    body += '<p style="margin: 0; padding: 0;">DAILY SUMMARY OF DAMAGE ESTIMATES FOR '+ date +'</p>'
    body += '<p style="margin: 0; padding: 0;">--------------------------------</p>'
    #Ex: 16.0 expected roofs damaged in the U.S.
    body += ('<p style="margin: 0; padding: 0;"><b>'+str(int(float(sum_expected[0])))
            +'</b> total expected roofs damaged in the U.S.</p>')
    #Ex: Max upper roofs damaged : Washington County, Alabama, with 96.0 total roofs.
    body += ('<p style="margin: 0; padding: 0;">Max upper roofs damaged : '+str(max_upper[0]) +
             ', ' + str(max_upper[1]) + ', with <b>' + str(int(float(max_upper[2]))) + '</b> total roofs.</p>')
    #Ex: Max expected roofs damaged : Washington County, Alabama, with 15.0 total roofs.
    body += ('<p style="margin: 0; padding: 0;">Max expected roofs damaged : '
             +str(max_expected[0]) + ', ' + str(max_expected[1]) + ', with <b>'
             + str(int(float(max_expected[2]))) + '</b> total roofs.</p>')
    #Ex: TOP 3 DAMAGE ESTIMATES ON 2015-03-22
    body += '<p style="margin: 0; padding: 0;">--------------------------------</p>'
    body += ('<p style="margin: 0; padding: 0;">TOP DAMAGE ESTIMATES ON '+ date+'</b></p>')
    # add table to email
    body += '<p style="margin: 0; padding: 0;">--------------------------------</p>'
    # create table headers
    body += ("""<table border = "1" style="width:75%;border: 1px solid;cellspacing="0";
             cellpadding="0"><tr><th>Location</th><th>State</th><th>Upper Damage
             </th><th>Expected Damage</th><th>Lower Damage</th></tr>""")
    # add each damage entry to the email table
    for top_5 in top_5:
        top_5 = top_5.split(";")
        for top_5 in top_5:
            top_5 = top_5.split(":")
            if float(top_5[2]) > 10000:
                # color the events orange
                body+=('<td bgcolor="#FF8C00">'+ str(top_5[0])+ '</td><td bgcolor="#FF8C00">'
                + str(top_5[1]) + '</td><td bgcolor="#FF8C00">'
                + str(int(float(top_5[2]))) + '</td><td bgcolor="#FF8C00">' + str(int(float(top_5[3]))) +
                '</td><td bgcolor="#FF8C00">' + str(int(float(top_5[4])))+'</td></tr>')
            else:
                body+=('<tr><td>'+ str(top_5[0])+ '</td><td>' + str(top_5[1]) + '</td><td>'
                + str(int(float(top_5[2]))) + '</td><td>' + str(int(float(top_5[3]))) + '</td><td>'
                + str(int(float(top_5[4])))+'</td></tr>')
            
    body+= '</cell></table>'
    return body

# create a sql query into something readable.
# create a delimited string of the returned rows.
# each row separated by a semicolon, each column by colon
def format_query(in_results):
    events = ""
    for record in in_results:
        for eachpiece in record:
            if eachpiece == '':
                eachpiece='0'
            events+=(str(eachpiece))
            events+=":"
        events += ";"
    return events


# email the alerts
def email_damage(alert,body,date):
    
    sender = 'tconnor@veriskclimate.com'
    #bcc = ['benchmark-info@veriskclimate.com', 'tnicholson@veriskclimate.com']
    bcc = ['gaf_demand_summary@aer.com','gafdemand@aer.com','srsdemand@veriskclimate.com','abcsupply@veriskclimate.com']
    recip = ['tlichtmann@veriskclimate.com','thowingt@veriskclimate.com', 'smassa@veriskclimate.com', 'tconnor@veriskclimate.com', 'tfarney@veriskclimate.com']
    #recip = ['tnichdolson@veriskclimate.com']
    msg = MIMEText(body, 'html')
    if body == '':
        msg['Subject'] = 'Shingle demand summary: no damage detected on ' + str(date)
    elif alert >0:
        msg['Subject'] = 'Alert - hail event(s) detected on ' + str(date)
    else:
        msg['Subject'] = 'Shingle demand summary for ' + str(date)

        
    msg['From'] = sender
    msg['To'] = ', '.join(recip)

    recips = recip + bcc
    s = smtplib.SMTP('smtp-gp.aer.com')
    s.sendmail(sender, recips, msg.as_string())
    s.quit()

def main(yesterday):
    # set the maximum number of rows to return for top damages
    max_num_entries = 5
    # variable to store the number of "alerted" regions
    alert = 0

    
    # load temp damage files for county and msa
    temp_county_damage(yesterday)
    temp_msa_damage(yesterday)

    # check to see if an event occurred
    alert = check_alert()
    if alert > 0:
        updateOne = determine_update_one(yesterday)
        updateTwo = determine_update_two(yesterday)
    else:
        updateOne = "na"
        updateTwo = "na"
        
    # determine the max upper found
    max_upper = (format_query(determine_max_upper())).split(":")
    # determine the max expected found
    max_expected = (format_query(determine_max_expected())).split(":")
    # determine the total expected
    sum_expected = (format_query(determine_sum_expected())).split(":")
    # grab the top damagge rows
    top_damage = ((format_query(determine_top_damage(max_num_entries))).split(";"))
    # last column is junk (artifact of string formatting)
    top_damage.pop()
    # delete the entries from the temp table
    delete_temp_damage() 

    # only create the email body and email if there was data from the previous day
    if len(top_damage)>1:
        body = create_body(yesterday,max_upper,max_expected,sum_expected,top_damage,updateOne,updateTwo)
    else:
        body = ''
    email_damage(alert, body,yesterday) 

#### really main portion ####

if __name__=="__main__":
    if len(sys.argv)==2:
        yesterday=sys.argv[1]
    else:
        today=datetime.datetime.today()
        yester_delta=datetime.timedelta(days=1)
        yesterday=(today-yester_delta).strftime("%Y-%m-%d")
    main(yesterday)
    #main('2015-04-10')
else:
    pass

