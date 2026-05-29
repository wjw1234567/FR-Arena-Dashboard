from datetime import date,datetime,timedelta
import pandas as pd




# print(datetime.now().strftime('%Y-%m-%d'))


# def generate_september_days(year=None,month=None):
#     """生成指定年份9月全量日期（默认当前年份）"""
#     target_year = year or date.today().year
#     start = date(target_year, 9, 1)
#     end = date(target_year, 9, 30)
#     return [start + timedelta(days=i) for i in range((end - start).days + 1)]
#
# # 生成2023年9月日期列表
# september_dates = generate_september_days(2023)
# print([d.strftime("%Y-%m-%d") for d in september_dates])


# date_List=pd.date_range("2025-07-01", "2025-09-30").strftime("%Y-%m-%d").tolist()

date_List=pd.date_range(datetime.now().strftime('%Y-%m-%d'),datetime.now().strftime('%Y-%m-%d')).strftime("%Y-%m-%d").tolist()

print(date_List)


df_condit=[{'user':'A','region':'zone1','diff':3,'z0_time':datetime.now()},
           {'user':'A','region':'zone1','diff':4,'z0_time':datetime.now()},
           {'user':'A','region':'zone2','diff':5,'z0_time':datetime.now()},
           {'user':'A','region':'zone2','diff':6,'z0_time':datetime.now()},
           {'user':'b','region':'zone1','diff':7,'z0_time':datetime.now()},
           {'user':'b','region':'zone1','diff':8,'z0_time':datetime.now()},
           {'user':'b','region':'zone2','diff':9,'z0_time':datetime.now()},
           {'user':'b','region':'zone2','diff':10,'z0_time':datetime.now()}
             ]

df=pd.DataFrame(df_condit)
df['z0_time'] = df['z0_time'].dt.floor("10min")
group_keys=["user","region","z0_time"]
df_group=df.groupby(group_keys)["diff"].idxmin()
df_result=df.loc[df_group]

print(df_result)

seven_days_ago = (datetime.now().date() - timedelta(days=7)).strftime('%Y-%m-%d')
print(f"seven_days_ago={seven_days_ago}")

dict1={}

if not dict1:
    print("dict1 不为空")
else:
    print("dict1 为空")




