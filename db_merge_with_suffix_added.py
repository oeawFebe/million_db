import pandas as pd
import pickle
def _congregateDB(list_db,list_name):
    if len(list_db)>50:
        df0=pd.DataFrame()
        for df,name in zip(list_db[:50],list_name[:50]):
            df0=df0.join(df.add_suffix(name),how="outer")
        return df0.join(_congregateDB(list_db[50:],list_name=list_name[50:]),how='outer')

    else:
        df0=pd.DataFrame()
        for df,name in zip(list_db,list_name):
            df0=df0.join(df.add_suffix(name),how="outer")
        return df0

# dfx=pd.DataFrame({"ma":[1,2,3],"mi":[2,3,4]})
# dfx=dfx.set_index("ma")
# a,b=[],[]
# for x in range(10000):
#     a.append(dfx);b.append("_"+str(x))
# print(_congregateDB(a,b)) # takes 19s

def _write_into_pickle(df,filename):
    with open(filename,"wb") as p:
        pickle.dump(df,p)
