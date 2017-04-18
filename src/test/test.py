import pandas as pd
import arrow

aa={'one':[1,2,3],'two':[2,3,4],'three':[3,4,5],'four':[8,8,9]}
bb=pd.DataFrame(aa)
print bb
print bb.loc[bb['two'].isin((3,4))]

outDF = pd.DataFrame()
outDF['total'] =bb.groupby('four')['one'].count()
#outDF.insert(0, "dt", outDF.index.values)
print outDF

end_dt = arrow.get("20170401", "YYYYMMDD")
start_dt = end_dt.replace(days=-9)
dates = ','.join(map(lambda x: x.strftime("%Y%m%d"),
                                 pd.date_range(start_dt.format("YYYY-MM-DD"), end_dt.format("YYYY-MM-DD"))))
print dates;