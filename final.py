import requests,zipfile,io,datetime,os
import pandas as pd
def split(date):
    return date.split('/')
def mapped(lis):
    return list(map(int,lis))
def urlmodify(year,month,day):
    base_url='https://www1.nseindia.com/content/historical/EQUITIES/'
    base_suffix='bhav.csv.zip'
    month_dict={1:'JAN',2:'FEB',3:'MAR',4:'APR',5:'MAY',6:'JUN',7:'JUL',8:'AUG',9:'SEP',10:'OCT',11:'NOV',12:'DEC'}
    base_month=month_dict[month]
    day=bytes(str(day).zfill(2),encoding="utf-8")
    final_url=base_url+str(year)+'/'+base_month+'/'+"cm"+day.decode()+base_month+str(year)+base_suffix
    print(final_url)
    return final_url
def dlfile(file):
    r=requests.get(file)
    if r.status_code!=200:
        return 0
    zipf=zipfile.ZipFile(io.BytesIO(r.content))
    zipf.extractall()
    file=''.join(zipf.namelist())
    print(file)
    return file
def merge(curr,old):
    df1=pd.read_csv(curr)
    df2=pd.read_csv(old)
    join = pd.merge(df1, df2, on='ISIN')
    df_final = join.loc[:, ['SYMBOL_x', 'ISIN', 'CLOSE_x', 'CLOSE_y']]
    formula = ((df_final['CLOSE_x'] - df_final['CLOSE_y']) / df_final['CLOSE_y']) * 100
    df_final = df_final.assign(changed=formula.values)
    df_final = df_final.round(2)
    df_final.sort_values(by='changed', axis=0, ascending=False, inplace=True)
    file_name="report"+str(cd_day)+str(cd_month)+"_"+str(od_day)+str(od_month)+".csv"
    with open(file_name,'w') as file:
        df_final.to_csv(file_name)
    return 0
current_date=os.getenv("IP1")
old_date=os.getenv("IP2")
current_date=split(current_date)
current_date=mapped(current_date)
cd_year,cd_month,cd_day=current_date
old_date=split(old_date)
old_date=mapped(old_date)
od_year,od_month,od_day=old_date
cdo=datetime.datetime(cd_year,cd_month,cd_day)
odo=datetime.datetime(od_year,od_month,od_day)
if cdo.isoweekday() and odo.isoweekday() not in {6,7}:
    cu_url=urlmodify(cd_year,cd_month,cd_day)
    old_url = urlmodify(od_year,od_month,od_day)
    cu_file=dlfile(cu_url)
    od_file=dlfile(old_url)
    if cu_file and od_file:
        merge(cu_file,od_file)
    else:
        print("File not found. Retry")
else:
    if cdo.isoweekday() in {6,7}:
        print("Current date should be a weekday or trading day")
    else:
        print("Old date should be a weekday or trading day")