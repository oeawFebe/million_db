import re
import pandas as pd
import pickle
import os
def _congregateDB(list_db,list_name):
    df_main=pd.DataFrame()
    for df,name in zip(list_db,list_name):
        df_main=df_main.join(df.add_suffix(name),how="outer")
    return df_main

def _write_into_pickle(df,filename):
    with open(filename,"wb") as p:
        pickle.dump(df,p)

def _now_collected():
    collected_tickers=[]
    pat=re.compile("^[A-Za-z._-]+")
    for f in os.listdir("."):
        if f.endswith("1min.p"):
            try:
                ticker=re.match(pat,f)[0]
                collected_tickers.append(ticker)
            except Exception:
                pass
    return collected_tickers

def _suffix_to_df(ticker):
    for f in os.listdir('.'):
        f.startswith(ticker) and f.endswith("1min.p")
        return f
    return None

def _read_p(filename):
    with open(filename,"rb") as p:
        return pickle.load(p)

def _suffix_list_to_df(ticker_list):
    filename_list = [_suffix_to_df(x) for x in ticker_list if _suffix_to_df(x) is not None]

    return [_read_p(f) for f in filename_list]

def df_congregate_50():
    collected_tickers=_now_collected()
    batch_max=(len(collected_tickers)+49)//50
    for batch in range(batch_max):
        target=collected_tickers[batch*50:batch*50+50]
        print(f"Going to collect {batch*50}~{batch*50+49} dfs: ",target)
        df=_congregateDB(_suffix_list_to_df(target),target)
        _write_into_pickle(df,f"_batch_{batch}_2020_09_09.p")
    return None
