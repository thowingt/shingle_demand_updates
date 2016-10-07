import os, sys
import datetime
import psycopg2
import xlrd
import ConfigParser


# The date in the filename is the day after the "data" date.  It's 6am GMT day x to 6am GMT day y,
# which equals midnight day x to midnight day y (CST)

# When running this today (with no argument), you will get read the file that is labelled
# today in the filename, but contains yesterday's data.

# If you need to run it for a day in the past, e.g. 20160227, then run it with the argument 20160228.


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
    # assumes in_date is of the form YYYYMMDD
    year_dir=str(in_date)[0:4]
    filename="GAF_wind_county_"+str(in_date)+"_1day.xlsx"
    path_to_file=os.path.join(r"\\bluearc-evs1\vc_project\QC\perilDamage",str(in_date), filename)
    #path_to_file=os.path.join(filename)
    return path_to_file

def read_info(in_sheet, date_of_wind, conn, cur):
    for row_index in range(1, in_sheet.nrows):
        if (in_sheet.cell_type(row_index, 6) not in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK) and
        (in_sheet.cell_value(row_index, 6)+in_sheet.cell_value(row_index, 12)+in_sheet.cell_value(row_index, 19))>0):
            county="'"+str(in_sheet.cell_value(row_index, 0).replace("'","''").encode('UTF8'))+"'"
            fips="'"+in_sheet.cell_value(row_index, 1)+"'"
            state="'"+in_sheet.cell_value(row_index, 2)+"'"
            date1="'"+date_of_wind+"'"
            repair=in_sheet.cell_value(row_index, 6)
            replace=in_sheet.cell_value(row_index, 12)
            update_database(conn, cur, county, fips, state, date1, repair,
                            replace)

def update_database(conn, cur, county, fips, state, date1, repair, replace):
    q_line="insert into wind_damage values({0}, {1}, \
            {2}, {3}, {4}, {5}, {6})".format('default', county, fips, state, date1,
                          repair, replace)
    cur.execute(q_line)
    conn.commit()

def get_date_of_wind(yesterday):
    # calculates the date of the wind (rather than the date in the filename)
    yesterday=datetime.datetime.strptime(yesterday, '%Y%m%d')
    yester_delta=datetime.timedelta(days=1)
    date_of_wind=(yesterday-yester_delta).strftime('%Y-%m-%d')
    return date_of_wind

def wind_importer(today):
    yesterday_dam_file=get_dam_file(today)
    book=xlrd.open_workbook(yesterday_dam_file, encoding_override="unicode")
    sheet1=book.sheet_by_index(0)

    date_of_wind=get_date_of_wind(today)
    
    dbn, usn, pw, hn=get_params()
    conn = psycopg2.connect("dbname={0} user={1} password={2} host={3}".format(dbn, usn, pw, hn))
    cur=conn.cursor()
    read_info(sheet1, date_of_wind, conn, cur)
    conn.close()
    print "finished importing wind damage"

#### main portion ####
if __name__=="__main__":
    if len(sys.argv)==2:
        today=sys.argv[1]
    else:
        today=datetime.datetime.today().strftime("%Y%m%d")
    wind_importer(today)
else:
    pass
        

