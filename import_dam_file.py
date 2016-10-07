import os, sys
import datetime
import psycopg2
import xlrd
import ConfigParser

# The goal is to change this so that it updates the database separately
# for each line of input directly from the xls without going thru the csv
# conversion.

# In hail dmage files:
# file date is beginning of period.  think about time zones.  it's 6am GMT day x to 6am GMT day y
# which equals midnight day x to midnight day y (CST)

######  CHANGE THIS TO GO LIVE  ##########
# don't need this anymore... working_directory=r"D:\Shingle_Demand_QC\input_reports"

confile=r"C:\Users\thowingt\Documents\Code\python\shingle_demand.cfg"

def get_params():
    config=ConfigParser.RawConfigParser()
    config.read(confile)
    dbn=config.get('database parameters','database')
    usn=config.get('database parameters','user')
    hn=config.get('database parameters', 'host')
    pw=config.get('database parameters', 'password')
    return [dbn, usn, pw, hn]

def get_dam_file(in_date):
    #assumes in_date is of the form YYYYMMDD
    year_dir=str(in_date)[0:4]
    filename="GAF_hail_county_"+str(in_date)+"_1day.xlsx"
    path_to_file=os.path.join(r"\\bluearc-evs1\vc_project\QC\perilDamage",str(in_date), filename)
    #path_to_file=os.path.join(filename)
    return path_to_file

def read_info(in_sheet, date_of_hail, conn, cur):
    for row_index in range(1, in_sheet.nrows):
        #row_elements=[]
        if in_sheet.cell_type(row_index, 4) not in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK)and in_sheet.cell_value(row_index, 4)>0:
            county="'"+str(in_sheet.cell_value(row_index, 0).replace("'","''").encode('UTF8'))+"'" # county
            fips="'"+in_sheet.cell_value(row_index, 1)+"'" # fips
            state="'"+in_sheet.cell_value(row_index, 2)+"'" # state
            date1="'"+date_of_hail+"'" # date1
            upper=str(int(in_sheet.cell_value(row_index, 4))) # upper
            expected=str(int(in_sheet.cell_value(row_index, 5))) # expected
            lower=str(int(in_sheet.cell_value(row_index, 6))) # lower
            exp_single=str(int(in_sheet.cell_value(row_index, 8))) # exp_single
            exp_duplex=str(int(in_sheet.cell_value(row_index, 9))) # exp_duplex
            exp_tripquad=str(int(in_sheet.cell_value(row_index, 10))) # exp_tripquad
            exp_mobile=str(int(in_sheet.cell_value(row_index, 11))) # exp_mobile

            update_database(conn, cur, county, fips, state, date1, upper,
                            expected, lower, exp_single, exp_duplex, exp_tripquad, exp_mobile)
             
def update_database(conn, cur, county, fips, state, date1, upper, expected, lower, exp_single, exp_duplex, exp_tripquad, exp_mobile):
    # first drop index from table?  right now it doesn't have one, but it should
    q_line="insert into damage values({0}, {1}, \
            {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, \
            {10})".format(county, fips, state, date1,
                          upper, expected,lower, exp_single,
                          exp_duplex, exp_tripquad, exp_mobile) 
    cur.execute(q_line)
    conn.commit()

def get_date_of_hail(yesterday):
    yesterday=datetime.datetime.strptime(yesterday, '%Y%m%d')
    yester_delta=datetime.timedelta(days=1)
    date_of_hail=(yesterday-yester_delta).strftime('%Y-%m-%d')
    return date_of_hail

def main_process(yesterday):
    # get yesterday's damage file, grab first sheet of workbook
    yesterday_dam_file=get_dam_file(yesterday)
    book=xlrd.open_workbook(yesterday_dam_file, encoding_override="unicode")
    sheet1=book.sheet_by_index(0)

    # read the sheet and write contents to database
    date_of_hail=get_date_of_hail(yesterday)

    dbn, usn, pw, hn=get_params()
    conn = psycopg2.connect("dbname={0} user={1} password={2} host={3}".format(dbn, usn, pw, hn))
    
    cur=conn.cursor()
    read_info(sheet1, date_of_hail, conn, cur)
    conn.close()
    print "finished dam import"

#### really main portion ####

if __name__=="__main__":
    if len(sys.argv)==2:
        today=sys.argv[1]
    else:
        today=datetime.datetime.today().strftime("%Y%m%d")
    main_process(today)
else:
    pass
