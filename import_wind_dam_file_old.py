import os, sys
import datetime
import psycopg2
import xlrd


# The date in the filename is the day after the "data" date.  It's 6am GMT day x to 6am GMT day y,
# which equals midnight day x to midnight day y (CST)

# When running this today (with no argument), you will get read the file that is labelled
# today in the filename, but contains yesterday's data.

# If you need to run it for a day in the past, e.g. 20160227, then run it with the argument 20160228.



def get_dam_file(in_date):
    # assumes in_date is of the form YYYYMMDD
    year_dir=str(in_date)[0:4]
    filename="GAF_wind_county_"+str(in_date)+"_1day.xlsx"
    path_to_file=os.path.join(r"\\bluearc-evs1\vc_project\QC\perilDamage",str(in_date), filename)
    #path_to_file=os.path.join(filename)
    return path_to_file

def get_number_of_rows(in_sheet):
    # gets the number of rows that show some damage
    num_rows=0
    for row_index in range(1, in_sheet.nrows):
        if (in_sheet.cell_type(row_index, 6) not in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK) and
        (in_sheet.cell_value(row_index, 6)+in_sheet.cell_value(row_index, 12)+in_sheet.cell_value(row_index, 19))>0):
            num_rows=num_rows+1
    return num_rows


def read_line(in_sheet, row_num, date_of_wind):
    # makes a string of values that will be inserted into wind_damage table
    row_elements=",".join(['default',
    "'"+str((in_sheet.cell_value(row_num,0)))+"'",
    str((in_sheet.cell_value(row_num,1))),
    "'"+str((in_sheet.cell_value(row_num,2)))+"'",
                           "'"+date_of_wind+"'",
    str(int((in_sheet.cell_value(row_num,6)))),

    str(int((in_sheet.cell_value(row_num,12)))+int((in_sheet.cell_value(row_num,18))))])
    return row_elements
'''
def read_line(in_sheet, row_num, date_of_wind):
    # makes a string of values that will be inserted into wind_damage table
    row_elements=[]
    row_elements.append('default')
    row_elements.append(str((in_sheet.cell_value(row_num,0))))
    row_elements.append(str((in_sheet.cell_value(row_num,1))))
    row_elements.append(str((in_sheet.cell_value(row_num,2))))
    row_elements.append(date_of_wind)
    row_elements.append(str(int((in_sheet.cell_value(row_num,6)))))
    row_elements.append(str(int((in_sheet.cell_value(row_num,12)))))
    row_elements.append(int((in_sheet.cell_value(row_num,18))))
    return row_elements
'''
def get_date_of_wind(yesterday):
    # calculates the date of the wind (rahter than the date in the filename)
    yesterday=datetime.datetime.strptime(yesterday, '%Y%m%d')
    yester_delta=datetime.timedelta(days=1)
    date_of_wind=(yesterday-yester_delta).strftime('%Y-%m-%d')
    return date_of_wind

def wind_importer(today):
    yesterday_dam_file=get_dam_file(today)
    book=xlrd.open_workbook(yesterday_dam_file)
    sheet1=book.sheet_by_index(0)

    date_of_wind=get_date_of_wind(today)
    number_of_rows=get_number_of_rows(sheet1)
    conn = psycopg2.connect("dbname='roof_hail' user='postgres' host='localhost'")
    cur=conn.cursor()

    # skip the header row in the file (row 0), but get the proper number of entries
    for i in range(1, number_of_rows+1):
        row_to_add=read_line(sheet1, i, date_of_wind)
        #string_to_add=','.join(map(str,row_to_add))
        q_line="insert into wind_damage values({this_row})".format(this_row=row_to_add)
        cur.execute(q_line)
        conn.commit()
    conn.close()
    print "finished importing wind damage"


#### main portion ####
if __name__=="__main__":
    if len(sys.argv)==2:
        today=sys.argv[1]
    else:
        today=datetime.datetime.today().strftime("%Y%m%d")

    #for testing:
    #today='20160504'
    wind_importer(today)
else:
    pass
        

