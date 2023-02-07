from datetime import datetime
import pandas as pd
import glob
import sys
import os


# path = "/tmp/interpark/modify/donggu.choi/auth-only/01.api.external.login/ssw"

def main(argv):
    all_files = glob.glob(os.path.join(argv[1], "*."+argv[2]))
    df_log = pd.concat((pd.read_csv(f, sep=" ", 
                                    names=["date", "time", "REQUEST_HEADER", "REQUEST_DATA", "RESPONSE_DATA", 
                                           "threat_no", "device_id", "x_real_ip", "mem_id", 
                                           "OK", "code_200", "code_xxx"], 
                                    encoding='utf-8', engine='python') for f in all_files), ignore_index=True)
    df_log = df_log[df_log["device_id"] == "3be5ce5d2818b797f4e38a0cf845455d"]
    df_log["datetime"] = pd.to_datetime(df_log["date"].str.cat(df_log["time"], sep=" "))
    df_log = df_log.sort_values(by="datetime").reset_index(drop=True)
    # pd.DataFrame(df_log["mem_id"].unique()).to_csv("mem_id.csv", index=False, header=False, encoding="utf-8")
    df_log[["x_real_ip", "datetime", "mem_id", "OK"]].to_csv("ip_dt_enc_id.csv", index=False, header=False, encoding="utf-8")

    # 12.23 21:28 ~ 12.24 06:27
    df_summary1 = df_log.loc[(df_log["datetime"] >= datetime(2022, 12, 23, 21, 28)) & (df_log["datetime"] < datetime(2022, 12, 24, 6, 28)), 
                             ["datetime", "device_id", "mem_id", "OK"]]
    # 사용 계정수
    # print(len(df_summary1["device_id"].unique()))
    n_mem_id = len(df_summary1["mem_id"].unique())
    print(n_mem_id)
    # 로그인 시도수
    n_login = len(df_summary1)
    print(n_login)
    # 로그인 성공 횟수
    n_logged_in = len(df_summary1[df_summary1["OK"] == True])
    print(n_logged_in)
    # 로그인 성공 계정수
    n_logged_in_id = len(df_summary1.loc[df_summary1["OK"] == True, "mem_id"].unique())
    print(n_logged_in_id)
    # 로그인 성공 계정 비율
    if (n_mem_id != 0):
        print(n_logged_in_id/n_mem_id)
    else:
        print("0")
    print("")

    # 12.24 11:26 ~ 12.24 16:14
    df_summary2 = df_log.loc[(df_log["datetime"] >= datetime(2022, 12, 24, 11, 26)) & (df_log["datetime"] < datetime(2022, 12, 24, 16, 15)), 
                             ["datetime", "device_id", "mem_id", "OK"]]
    # 사용 계정수
    # print(len(df_summary1["device_id"].unique()))
    n_mem_id = len(df_summary2["mem_id"].unique())
    print(n_mem_id)
    # 로그인 시도수
    n_login = len(df_summary2)
    print(n_login)
    # 로그인 성공 횟수
    n_logged_in = len(df_summary2[df_summary2["OK"] == True])
    print(n_logged_in)
    # 로그인 성공 계정수
    n_logged_in_id = len(df_summary2.loc[df_summary2["OK"] == True, "mem_id"].unique())
    print(n_logged_in_id)
    # 로그인 성공 계정 비율
    if (n_mem_id != 0):
        print(n_logged_in_id/n_mem_id)
    else:
        print("0")
    print("")

    # 12.24 20:45 ~ 12.25 05:13
    df_summary3 = df_log.loc[(df_log["datetime"] >= datetime(2022, 12, 24, 20, 45)) & (df_log["datetime"] < datetime(2022, 12, 25, 5, 14)), 
                             ["datetime", "device_id", "mem_id", "OK"]]
    # 사용 계정수
    # print(len(df_summary1["device_id"].unique()))
    n_mem_id = len(df_summary3["mem_id"].unique())
    print(n_mem_id)
    # 로그인 시도수
    n_login = len(df_summary3)
    print(n_login)
    # 로그인 성공 횟수
    n_logged_in = len(df_summary3[df_summary3["OK"] == True])
    print(n_logged_in)
    # 로그인 성공 계정수
    n_logged_in_id = len(df_summary3.loc[df_summary3["OK"] == True, "mem_id"].unique())
    print(n_logged_in_id)
    # 로그인 성공 계정 비율
    if (n_mem_id != 0):
        print(n_logged_in_id/n_mem_id)
    else:
        print("0")
    print("")

    """
    # 12.23 21:28 ~ 12.25 05:13
    df_summary = df_log.loc[
        ((df_log["datetime"] >= datetime(2022, 12, 23, 21, 28)) & (df_log["datetime"] < datetime(2022, 12, 24, 6, 28))) | 
        ((df_log["datetime"] >= datetime(2022, 12, 24, 11, 26)) & (df_log["datetime"] < datetime(2022, 12, 24, 16, 15))) | 
        ((df_log["datetime"] >= datetime(2022, 12, 23, 21, 28)) & (df_log["datetime"] < datetime(2022, 12, 25, 5, 14))), 
        ["datetime", "device_id", "mem_id", "OK"]]
    """
    # 사용 계정수
    # print(len(df_log["device_id"].unique()))
    n_mem_id = len(df_log["mem_id"].unique())
    print(n_mem_id)
    # 로그인 시도수
    n_login = len(df_log)
    print(n_login)
    # 로그인 성공 횟수
    n_logged_in = len(df_log[df_log["OK"] == True])
    print(n_logged_in)
    # 로그인 성공 계정수
    n_logged_in_id = len(df_log.loc[df_log["OK"] == True, "mem_id"].unique())
    print(n_logged_in_id)
    # 로그인 성공 계정 비율
    if (n_mem_id != 0):
        print(n_logged_in_id/n_mem_id)
    else:
        print("0")
            
if __name__ == "__main__":
    main(sys.argv)
