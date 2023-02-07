from datetime import datetime
import pandas as pd
import glob
import os

path = "/tmp/interpark/modify/donggu.choi/auth-only/02.oauth.userinfo/ssw_oauth"
all_files = glob.glob(os.path.join(path, "*.log"))
df_log = pd.concat((pd.read_csv(f, sep=" ", 
                                names=["date", "time", "REQUEST_HEADER", "REQUEST_DATA", "RESPONSE_DATA", 
                                       "threat_no", "device_id", "mem_no", "mem_id", "name", "email", "phone"], 
                                encoding='utf-8', engine='python') for f in all_files), ignore_index=True)
# df_log = df_log[df_log["device_id"] == "None"]
df_log = df_log[df_log["device_id"] == "3be5ce5d2818b797f4e38a0cf845455d"]
df_log["datetime"] = pd.to_datetime(df_log["date"].str.cat(df_log["time"], sep=" "))
df_log = df_log.sort_values(by="datetime").reset_index(drop=True)

# 12.23 21:28 ~ 12.24 06:27
df_summary1 = df_log.loc[(df_log["datetime"] >= datetime(2022, 12, 23, 21, 28)) & (df_log["datetime"] < datetime(2022, 12, 24, 6, 28)), 
                         ["datetime", "device_id", "mem_id", "mem_no"]]
# 사용 계정수
# print(len(df_summary1["device_id"].unique()))
n_mem_id = len(df_summary1["mem_id"].unique())
print(n_mem_id)
# 로그인 시도수
n_login = len(df_summary1)
print(n_login)
print("")

# 12.24 11:26 ~ 12.24 16:14
df_summary2 = df_log.loc[(df_log["datetime"] >= datetime(2022, 12, 24, 11, 26)) & (df_log["datetime"] < datetime(2022, 12, 24, 16, 15)), 
                         ["datetime", "device_id", "mem_id", "mem_no"]]
# 사용 계정수
# print(len(df_summary1["device_id"].unique()))
n_mem_id = len(df_summary2["mem_id"].unique())
print(n_mem_id)
# 로그인 시도수
n_login = len(df_summary2)
print(n_login)
print("")

# 12.24 20:45 ~ 12.25 05:13
df_summary3 = df_log.loc[(df_log["datetime"] >= datetime(2022, 12, 24, 20, 45)) & (df_log["datetime"] < datetime(2022, 12, 25, 5, 14)), 
                         ["datetime", "device_id", "mem_id", "mem_no"]]
# 사용 계정수
# print(len(df_summary1["device_id"].unique()))
n_mem_id = len(df_summary3["mem_id"].unique())
print(n_mem_id)
# 로그인 시도수
n_login = len(df_summary3)
print(n_login)
print("")

"""
# 12.23 21:28 ~ 12.25 05:13
df_summary = df_log.loc[
    ((df_log["datetime"] >= datetime(2022, 12, 23, 21, 28)) & (df_log["datetime"] < datetime(2022, 12, 24, 6, 28))) | 
    ((df_log["datetime"] >= datetime(2022, 12, 24, 11, 26)) & (df_log["datetime"] < datetime(2022, 12, 24, 16, 15))) | 
    ((df_log["datetime"] >= datetime(2022, 12, 23, 21, 28)) & (df_log["datetime"] < datetime(2022, 12, 25, 5, 14))), 
    ["datetime", "device_id", "mem_id", "mem_no"]]
"""
# 사용 계정수
# print(len(df_log["device_id"].unique()))
n_mem_id = len(df_log["mem_id"].unique())
print(n_mem_id)
# 로그인 시도수
n_login = len(df_log)
print(n_login)

pd.DataFrame(df_log["mem_id"].unique()).to_csv("mem_id.csv", index=False, header=False, encoding="utf-8")
