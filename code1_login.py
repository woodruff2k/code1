from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import sys


def header2df(data):
    header = data.split(" ")
    if len(header) < 6 or header[4] != "INFO":
        # print("[error] - " + data)
        # result = pd.DataFrame([[None, None, None, None, None, None]], columns=["was_no", "date", "time", "thread", "level", "service"]).loc[0]
        result = pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan], 
                           index=["was_no", "date", "time", "thread", "level", "service"])
    else:
        del header[5]
        # result = pd.DataFrame([header], columns=["was_no", "date", "time", "thread", "level", "service"]).loc[0]
        result = pd.Series(header, index=["was_no", "date", "time", "thread", "level", "service"])
    return result


def REQUEST_HEADER(data):
    req_start  = data.find("REQUEST_HEADER")
    json_start = data.find("{", req_start+1)
    json_obj   = json.loads(data[json_start:])
    try:
        device_id  = json_obj["user-agent"].split(sep="|")[3]
        # return device_id
        x_real_ip  = json_obj["x-real-ip"]
        return pd.Series([device_id, x_real_ip], index=["device_id", "x_real_ip"])
    except Exception as e:
        print("[error] - " + data)
        # return None
        # return np.nan
        return pd.Series([np.nan, np.nan], index=["device_id", "x_real_ip"])


def REQUEST_DATA(data):
    req_start  = data.find("REQUEST_DATA")
    json_start = data.find("{", req_start+1)
    json_data = data[json_start:]
    json_obj = json.loads(json_data)
    try:
        return json_obj["memId"]
    except Exception as e:
        print("[error] - " + data)
        # return None
        return np.nan

    
def RESPONSE_DATA_200(data):
    key = "code="
    res_start  = data.find("RESPONSE_DATA")
    code_start = data.find(key, res_start+1)
    return data[code_start+len(key):code_start+len(key)+2]
    

def RESPONSE_DATA_XXX(data):
    res_start  = data.find("RESPONSE_DATA")
    json_start = data.find("{")
    json_obj   = json.loads(data[json_start:])
    try:
        return json_obj["code"]
    except Exception as e:
        print("[error] - " + data)
        # return None
        return np.nan
    
    
def main(argv):
    # df_log = pd.read_csv("result.log", sep=" - ", names=["header", "message"])
    df_log_org = pd.read_csv(argv[1], sep="1234567890!@#$%^&*()", names=["line"], 
                             encoding="utf-8", engine="python")
    df_log = df_log_org["line"].str.split(pat=" - ", n=1, expand=True)
    df_log.columns = ["header", "message"]
    df_log.index.name = "id"

    df_header = df_log["header"].apply(header2df)
    df_header = df_header.dropna(axis=0, how="any")
    df_header["thread_no"] = df_header["thread"].str.extract(r"(-\d+])")
    # df_header["thread_no"] = df_header["thread_no"].str[1:-1].astype(int)  # thread_no != nan
    df_header["thread_no"] = df_header["thread_no"].str[1:-1]  # thread_no != nan
    df_header["login"] = df_log["message"].str.contains("/api/external/login")
    df_header["login_token"] = df_log["message"].str.contains("/api/external/loginToken")
    df_header["login_otp"] = df_log["message"].str.contains("/api/external/loginOtp")
    """
    df_header = df_header.loc[(df_header["login"] == True) & (df_header["login_token"] == False), 
                              ["was_no", "date", "time", "thread_no", "login", "login_token", "login_otp"]]
    # df_header = df_header[df_header["login_otp"] == False]
    """
    df_header = df_header.loc[(df_header["login"] == True) & (df_header["login_token"] == False) & (df_header["login_otp"] == False), 
                              ["was_no", "date", "time", "thread_no", "login", "login_token", "login_otp"]]
    
    df_login_log = df_header.merge(df_log["message"].to_frame(), left_on='id', right_on='id', how="left")
    df_login_log["REQUEST_HEADER"] = df_login_log["message"].str.contains("REQUEST_HEADER")
    df_login_log[["device_id", "x_real_ip"]] = df_login_log[df_login_log["REQUEST_HEADER"] == True]["message"].apply(REQUEST_HEADER)
    df_login_log["REQUEST_DATA"] = df_login_log["message"].str.contains("REQUEST_DATA")
    df_login_log["mem_id"] = df_login_log[df_login_log["REQUEST_DATA"] == True]["message"].apply(REQUEST_DATA)
    df_login_log["RESPONSE_DATA"] = df_login_log["message"].str.contains("RESPONSE_DATA")
    df_login_log["OK"] = df_login_log["message"].str.contains("200 OK")
    df_login_log["code_200"] = df_login_log[(df_login_log["RESPONSE_DATA"] == True) & (df_login_log["OK"] == True)]["message"].apply(RESPONSE_DATA_200)
    df_login_log["code_xxx"] = df_login_log[(df_login_log["RESPONSE_DATA"] == True) & (df_login_log["OK"] == False)]["message"].apply(RESPONSE_DATA_XXX)

    n_lines = 0
    indexes = []
    last_index = len(df_login_log) - 1
    # for idx in df_login_log.index:
    for i, item in enumerate(df_login_log.itertuples()):
        # df_login_log.loc[idx, "was_no"]
        # print(item[0])  # index
        # print(item[1])  # was_no
        # (1) find REQUEST_HEADER
        if item[9] == True:
            current_date = datetime.strptime(item[2], "%Y-%m-%d").date()
            current_time = datetime.strptime(item[3], "%H:%M:%S.%f").time()
            current_datetime = datetime.combine(current_date, current_time)
            # print(df_login_log.index[i])
            # print(item[0])  # index
            # print(item[4])  # thread_no
            # (2) find REQUEST_DATA
            idx_REQUEST_DATA = None
            after = i + 1
            # after_date = datetime.strptime(df_login_log.loc[df_login_log.index[after], "date"], "%Y-%m-%d").date()
            # after_time = datetime.strptime(df_login_log.loc[df_login_log.index[after], "time"], "%H:%M:%S.%f").time()
            # after_datetime= datetime.combine(after_date, after_time)
            while after <= last_index:  # and after_datetime - current_datetime < timedelta(seconds=1):
                idx = df_login_log.index[after]
                if item[4] == df_login_log.loc[idx, "thread_no"] and df_login_log.loc[idx, "REQUEST_DATA"] == True:
                    idx_REQUEST_DATA = idx
                    break
                after = after + 1
                # if after <= last_index:
                #     after_date = datetime.strptime(df_login_log.loc[df_login_log.index[after], "date"], "%Y-%m-%d").date()
                #     after_time = datetime.strptime(df_login_log.loc[df_login_log.index[after], "time"], "%H:%M:%S.%f").time()
                #     after_datetime= datetime.combine(after_date, after_time)

            # (3) find RESPONSE_DATA
            idx_RESPONSE_DATA = None
            after = i + 1
            # after_date = datetime.strptime(df_login_log.loc[df_login_log.index[after], "date"], "%Y-%m-%d").date()
            # after_time = datetime.strptime(df_login_log.loc[df_login_log.index[after], "time"], "%H:%M:%S.%f").time()
            # after_datetime= datetime.combine(after_date, after_time)
            while after <= last_index:  # and after_datetime - current_datetime < timedelta(seconds=1):
                idx = df_login_log.index[after]
                if (item[4] == df_login_log.loc[idx, "thread_no"]) and (df_login_log.loc[idx, "RESPONSE_DATA"] == True):
                    idx_RESPONSE_DATA = idx
                    break
                after = after + 1
                # if after <= last_index:
                #     after_date = datetime.strptime(df_login_log.loc[df_login_log.index[after], "date"], "%Y-%m-%d").date()
                #     after_time = datetime.strptime(df_login_log.loc[df_login_log.index[after], "time"], "%H:%M:%S.%f").time()
                #     after_datetime= datetime.combine(after_date, after_time)

            if idx_REQUEST_DATA is None or idx_RESPONSE_DATA is None:
                if idx_REQUEST_DATA is None:
                    print(str(item[0]) + " has no REQUEST_DATA")
                if idx_RESPONSE_DATA is None:
                    print(str(item[0]) + " has no RESPONSE_DATA")
            elif df_login_log.loc[idx_REQUEST_DATA, "message"].find("/api/external/loginOtp") == -1:
                print(current_datetime, item[0], idx_REQUEST_DATA, idx_RESPONSE_DATA, 
                    df_login_log.loc[item[0], "thread_no"], 
                    df_login_log.loc[item[0], "device_id"], 
                    df_login_log.loc[item[0], "x_real_ip"],
                    df_login_log.loc[idx_REQUEST_DATA, "mem_id"], 
                    df_login_log.loc[idx_RESPONSE_DATA, "OK"],
                    df_login_log.loc[idx_RESPONSE_DATA, "code_200"],
                    df_login_log.loc[idx_RESPONSE_DATA, "code_xxx"])
                n_lines += 1
                """
                indexes.append(item[0])
                indexes.append(idx_REQUEST_DATA)
                indexes.append(idx_RESPONSE_DATA)
                """
                
    # print(n_lines)
    # df_log_org.loc[indexes, "line"].to_csv("login_org.csv", index=False, header=False, quoting=3, escapechar="\\", encoding="utf-8")

    
if __name__ == "__main__":
    main(sys.argv)
