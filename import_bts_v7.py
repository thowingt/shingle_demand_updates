# 20160725
# this version reads the xls file directly,
# without making any intermediate transformations.


import os, sys
import datetime
import psycopg2
import xlrd


def get_bts_file(in_date):
    # assumes in_date is of the form YYYYMMDD
    # returns path to target file
    year_dir=str(in_date)[0:4]
    filename="bts_daily_"+str(in_date)+".xls"
    path_to_file=os.path.join(r"\\Bluearc-evs1\BSD_Weather\BTS_Reports\successes",
                              year_dir,"daily",filename)
    #path_to_file=os.path.join(r"","bts_daily_20150515.xls")
    return path_to_file

def get_number_of_rows(in_sheet):
    # gets the number of rows that show some damage
    num_rows=0
    for row_index in range(1, in_sheet.nrows):
        if (in_sheet.cell_type(row_index, 1) not in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK)):
            num_rows=num_rows+1
    return num_rows

def read_line(in_sheet, row_num):
    # makes a string of values that will be inserted into wind_damage table
    row_elements=[]
    row_elements.append("'"+str(in_sheet.cell_value(row_num,0))+"'") # trans_date
    row_elements.append("'"+str(in_sheet.cell_value(row_num,1))+"'") # req_unique
    row_elements.append("'"+str(in_sheet.cell_value(row_num,5).replace("'","''"))+"'") # company
    row_elements.append("'"+str(in_sheet.cell_value(row_num,8))+"'") # cause_of_loss
    row_elements.append("'"+str(in_sheet.cell_value(row_num,9))+"'") # cause_of_loss_code
    row_elements.append("'"+str(in_sheet.cell_value(row_num,11))+"'") # date_of_loss
    row_elements.append(str(in_sheet.cell_value(row_num,14))) # geo_lat
    row_elements.append(str(in_sheet.cell_value(row_num,15))) # geo_lon
    row_elements.append("'"+str(in_sheet.cell_value(row_num,17).replace("'","''").replace("|",""))+"'") # city
    row_elements.append("'"+str(in_sheet.cell_value(row_num,18))+"'") # state
    return row_elements
            
def read_bts(in_file, cur, conn):
    book=xlrd.open_workbook(in_file,encoding_override="unicode")
    sheet1=book.sheet_by_index(0)

    number_of_rows=get_number_of_rows(sheet1)
    
    for i in range(1, number_of_rows+1):
        try:
            row_to_add=read_line(sheet1, i)
            if float(row_to_add[6]) and float(row_to_add[7]):
                string_to_add=','.join([row_to_add[0],
                                        row_to_add[1],
                                        row_to_add[2],
                                        row_to_add[3],
                                        row_to_add[4],
                                        row_to_add[5],
                                        row_to_add[6],
                                        row_to_add[7],
                                        row_to_add[8],
                                        row_to_add[9]])
                q_line="insert into bts_import values({this_row})".format(this_row=string_to_add)
                cur.execute(q_line)
                conn.commit()
        except:
            pass

def transform_bts_import_table(cur, conn):
    # This function adds and fills several columns to the bts_import
    # table in preparation for copying data to bts_rpts table.
    # Add locus col to temp_bts
    q_line="alter table bts_import add column locus geography(Point, 4326)"
    cur.execute(q_line)
    # Fill locus col
    q_line="""update bts_import set locus=ST_GeographyFromText('POINT(' || geocode_lon ||' ' || geocode_lat ||')')"""
    cur.execute(q_line)
    # Add fips
    q_line="alter table bts_import add column fips character varying"
    cur.execute(q_line)
    q_line="""update bts_import set fips=(select a.county_id from county_cbsa a \
            where st_contains(cast(a.geog as geometry), cast(bts_import.locus as geometry))='t')"""
    cur.execute(q_line)
    # Add msa_id
    q_line="alter table bts_import add column msa_id character varying"
    cur.execute(q_line)
    # Cast dates to date type
    q_line="alter table bts_import alter column trans_date type date using trans_date::date"
    cur.execute(q_line)
    q_line="alter table bts_import alter column date_of_loss type date using date_of_loss::date"
    cur.execute(q_line)
    q_line="""update bts_import set msa_id=(select a.msa_id from county_cbsa a \
            where st_contains(cast(a.geog as geometry), cast(bts_import.locus as geometry))='t')"""
    cur.execute(q_line)
    conn.commit()

def update_bts_rpts(cur, conn):
    # This function copies the bts_import table to the bts_rpts table.
    q_line="insert into bts_rpts select * from bts_import"
    cur.execute(q_line)
    conn.commit()

def cleanup_bts_import_table(cur, conn):
    # Returns bts_import table back to its original format.
    q_line="delete from bts_import"
    cur.execute(q_line)
    #put bts_import back into its original format
    q_line="alter table bts_import drop column fips; \
            alter table bts_import drop column msa_id; \
            alter table bts_import drop column locus; \
            alter table bts_import alter column trans_date type character varying; \
            alter table bts_import alter column date_of_loss type character varying;"
    cur.execute(q_line)
    conn.commit()

def main_process(yesterday):
    yesterday_bts_file=get_bts_file(yesterday)

    #conn = psycopg2.connect("dbname='shingle_demand' user='shingle_demand'password='b@dweather!' host='localhost'")
    conn = psycopg2.connect("dbname='roof_hail' user='postgres' host='localhost'")
    cur=conn.cursor()    
    read_bts(yesterday_bts_file, cur, conn)
    transform_bts_import_table(cur, conn)
    update_bts_rpts(cur, conn)
    cleanup_bts_import_table(cur, conn)
    conn.close()

######## main #######

# This portion determines the day of the bts report to process.
# If this is run fomr the command line, the only argument necessary
# is the date of the bts report desired in YYYYMMDD format
# If this is run without an argument, then yesterday's date is used.
if __name__=="__main__":
    if len(sys.argv)==2:
        yesterday=sys.argv[1]
        if str(yesterday[0:4])!=str(datetime.datetime.now().year):
            print "Do you really want",yesterday,"?"
            print "If so, comment out this line and try again."
            sys.exit()
    else:
        today=datetime.datetime.today()
        yester_delta=datetime.timedelta(days=1)
        yesterday=(today-yester_delta).strftime("%Y%m%d")
    main_process(yesterday)

else:
    pass
