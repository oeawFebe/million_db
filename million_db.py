import pandas as pd
def congregateDB(list_db,list_name=None):
    if len(list_db)>50:
        if list_name:
            df0=pd.DataFrame()
            for df,name in zip(list_db[:50],list_name[:50]):
                df0=df0.join(df,how="outer",rsuffix=name)
            return df.join(congregateDB(list_db[50:],list_name=list_name[50:]),how='outer')
        else:
            df0=pd.DataFrame()
            for df in list_db[:50]:
                df0=df0.join(df,how="outer")
            return df.join(congregateDB(list_db[50:]),how='outer')

    else:
        df0=pd.DataFrame()
        for df,name in zip(list_db,list_name):
            df0=df0.join(df,how="outer",rsuffix=name)
        return df0