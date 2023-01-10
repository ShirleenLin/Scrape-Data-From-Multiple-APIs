import tushare as ts
import datetime, sqlite3
from tqdm import tqdm
import pandas as pd

def downLoadData(pro):

    start_date = "2019-01-09"
    end_date="2023-01-09"

    conn = sqlite3.connect("New_Stk_hist.sqlite")
    cursor = conn.cursor()

    #Stock History Table
    df_daily_basic = pd.DataFrame()
    df_daily = pd.DataFrame()
    #Iterate through a daterange
    date_range = pd.date_range(start=start_date, end=end_date)
    for date in tqdm(date_range):
        date= date.strftime('%Y%m%d')
        #API 1
        daily = pro.daily(trade_date = date)[['ts_code','trade_date','pct_chg']]  #If the date is not a trade_date, it doesn't return - which is what we want
        #API2
        daily_basic = pro.daily_basic(trade_date = date)[['ts_code','trade_date','turnover_rate','total_mv']]
        #Merge dataframes from different APIs
        df_daily = pd.concat([df_daily,daily])
        #Concat the new dataframe
        df_daily_basic = pd.concat([df_daily_basic,daily_basic])
        print("Scaped Stock Info on:",date)
    stk_hist = pd.merge(df_daily,df_daily_basic,on=['ts_code','trade_date'])
    stk_hist.to_sql(name='Stk_hist', con=conn,if_exists="replace",index=False)
    print("Successfully saved data into SOL table: Stk_hist")

    conn.commit()
    conn.close()

#You need to get your own Tushare token from https://tushare.pro/document/1?doc_id=39
ts.set_token('')
pro = ts.pro_api()
downLoadData(pro)
