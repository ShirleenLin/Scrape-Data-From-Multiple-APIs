import tushare as ts
import time, datetime, sqlite3
from tqdm import tqdm
import pandas as pd

def downLoadData(pro):

    start_date = "2016-04-28"
    end_date="2023-01-09"

    conn = sqlite3.connect("test.sqlite")
    cursor = conn.cursor()

    #Financial Table
    financial = pd.DataFrame()
    codelist = pro.stock_basic()
    for i in tqdm(range(len(codelist["ts_code"]))):
        print(codelist["ts_code"][i])
        #API 1
        income = pro.income(ts_code=codelist["ts_code"][i],start_date=start_date, end_date=end_date)[['ts_code','ann_date','n_income','revenue']]
        #API 2
        cashflow = pro.cashflow(ts_code=codelist["ts_code"][i],start_date=start_date, end_date=end_date)[['ts_code','ann_date','c_inf_fr_operate_a']]
        #API 3
        fina_indicator = pro.fina_indicator(ts_code=codelist["ts_code"][i],start_date=start_date, end_date=end_date)[['ts_code','ann_date','roe']]
        #Merge dataframes from different APIs
        df_new = pd.merge(pd.merge(income,cashflow,on=['ts_code','ann_date']),fina_indicator,on=['ts_code','ann_date'])
        #Concat the new dataframe
        financial = pd.concat([financial,df_new])
    financial.drop_duplicates(inplace=True)
    print("Saving financial data into SQL.."")
    financial.to_sql(name='Financial', con=conn,if_exists="replace",index=False)
    #codelist.to_sql(name='Share_index', con=conn,index=False) --> Use this line if you want a Share_index Table
    print("Successfully saved financial data into SQL.."")
    conn.commit()
    conn.close()

#You need to get your own Tushare token from https://tushare.pro/document/1?doc_id=39
ts.set_token('')
pro = ts.pro_api()
downLoadData(pro)
