from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import sys


def header2df(data):
    header = data.split(" ")
    if len(header) < 6 or header[4] != "INFO":
        # print("[error] - " + data)
        # result = pd.DataFrame([[None, None, None, None, None, None]], columns=["was_no", "date", "time", "threat", "level", "service"]).loc[0]
        result = pd.Series([None, None, None, None, None, None], index=["was_no", "date", "time", "threat", "level", "service"])
    else:
        del header[5]
        # result = pd.DataFrame([header], columns=["was_no", "date", "time", "threat", "level", "service"]).loc[0]
        result = pd.Series(header, index=["was_no", "date", "time", "threat", "level", "service"])
    return result


def REQUEST_HEADER(data):
    req_start  = data.find("REQUEST_HEADER")
    json_start = data.find("{", req_start+1)
    json_obj   = json.loads(data[json_start:])
    try:
        return json_obj["user-agent"].split(sep="|")[3]
    except Exception as e:
        # print("[error] - " + data)
        return None


def find_user_info(data, key, last):
    start = data.find(key)
    start += len(key)
    end = data.find(last, start+1)
    return data[start:end]


def RESPONSE_DATA(data):
    start = data.find("RESPONSE_DATA")
    memNo = find_user_info(data, "memNo=", ",")
    memId = find_user_info(data, "memId='", "'")
    return pd.Series([memNo, memId], index=["mem_no", "mem_id"])


def main(argv):
    # df_log = pd.read_csv("oauth_result.log", sep=" - ", names=["header", "message"])
    # df_log = pd.read_csv("oauth_sample.log", sep="1234567890!@#$%^&*()", names=["line"])
    df_log_org = pd.read_csv(argv[1], sep="1234567890!@#$%^&*()", names=["line"], encoding='utf-8', engine='python')
    df_log = df_log_org["line"].str.split(pat=" - ", n=1, expand=True)
    df_log.columns = ["header", "message"]
    df_log.index.name = "id"

    df_header = df_log["header"].apply(header2df)
    df_header = df_header.dropna(axis=0, how="any")
    df_header["threat_no"] = df_header["threat"].str.extract(r"(-\d+])")
    # df_header["threat_no"] = df_header["threat_no"].str[1:-1].astype(int)  # threat_no != nan
    df_header["threat_no"] = df_header["threat_no"].str[1:-1]  # threat_no != nan
    df_header["oauth"] = df_log["message"].str.contains("/oauth/userinfo")
    df_header = df_header.loc[df_header["oauth"] == True, ["was_no", "date", "time", "threat_no", "oauth"]]
    
    df_oauth_log = df_header.merge(df_log["message"].to_frame(), left_on='id', right_on='id', how="left")
    df_oauth_log["REQUEST_HEADER"] = df_oauth_log["message"].str.contains("REQUEST_HEADER")
    df_oauth_log["device_id"] = df_oauth_log[df_oauth_log["REQUEST_HEADER"] == True]["message"].apply(REQUEST_HEADER)
    df_oauth_log["REQUEST_DATA"] = df_oauth_log["message"].str.contains("REQUEST_DATA")
    df_oauth_log["RESPONSE_DATA"] = df_oauth_log["message"].str.contains("RESPONSE_DATA")
    df_user_info = df_oauth_log[df_oauth_log["RESPONSE_DATA"] == True]["message"].apply(RESPONSE_DATA)
    df_oauth_log = df_oauth_log.merge(df_user_info, left_on='id', right_on='id', how="left")

    n_lines = 0
    indexes = []
    last_index = len(df_oauth_log) - 1
    # for idx in df_oauth_log.index:
    for i, item in enumerate(df_oauth_log.itertuples()):
        # df_oauth_log.loc[idx, "was_no"]
        # print(item[0])  # index
        # print(item[1])  # was_no
        # (1) find REQUEST_HEADER
        if item[7] == True:
            current_date = datetime.strptime(item[2], "%Y-%m-%d").date()
            current_time = datetime.strptime(item[3], "%H:%M:%S.%f").time()
            current_datetime = datetime.combine(current_date, current_time)
            # print(df_oauth_log.index[i])
            # print(item[0])  # index
            # print(item[4])  # threat_no
            # (2) find REQUEST_DATA
            idx_REQUEST_DATA = None
            after = i + 1
            # after_date = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "date"], "%Y-%m-%d").date()
            # after_time = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "time"], "%H:%M:%S.%f").time()
            # after_datetime= datetime.combine(after_date, after_time)
            while after <= last_index:  # and after_datetime - current_datetime < timedelta(seconds=1):
                idx = df_oauth_log.index[after]
                if item[4] == df_oauth_log.loc[idx, "threat_no"] and df_oauth_log.loc[idx, "REQUEST_DATA"] == True:
                    idx_REQUEST_DATA = idx
                    break
                after = after + 1
                # if after <= last_index:
                #     after_date = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "date"], "%Y-%m-%d").date()
                #     after_time = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "time"], "%H:%M:%S.%f").time()
                #     after_datetime= datetime.combine(after_date, after_time)

            # (3) find RESPONSE_DATA
            idx_RESPONSE_DATA = None
            after = i + 1
            # after_date = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "date"], "%Y-%m-%d").date()
            # after_time = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "time"], "%H:%M:%S.%f").time()
            # after_datetime= datetime.combine(after_date, after_time)
            while after <= last_index:  # and after_datetime - current_datetime < timedelta(seconds=1):
                idx = df_oauth_log.index[after]
                if (item[4] == df_oauth_log.loc[idx, "threat_no"]) and (df_oauth_log.loc[idx, "RESPONSE_DATA"] == True):
                    idx_RESPONSE_DATA = idx
                    break
                after = after + 1
                # if after <= last_index:
                #     after_date = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "date"], "%Y-%m-%d").date()
                #     after_time = datetime.strptime(df_oauth_log.loc[df_oauth_log.index[after], "time"], "%H:%M:%S.%f").time()
                #     after_datetime= datetime.combine(after_date, after_time)

            if idx_REQUEST_DATA is None or idx_RESPONSE_DATA is None:
                if idx_REQUEST_DATA is None:
                    print(str(item[0]) + " has no REQUEST_DATA")
                if idx_RESPONSE_DATA is None:
                    print(str(item[0]) + " has no RESPONSE_DATA")
            else:
                print(current_datetime, item[0], idx_REQUEST_DATA, idx_RESPONSE_DATA, 
                    df_oauth_log.loc[item[0], "threat_no"], 
                    df_oauth_log.loc[item[0], "device_id"], 
                    df_oauth_log.loc[idx_RESPONSE_DATA, "mem_no"],
                    df_oauth_log.loc[idx_RESPONSE_DATA, "mem_id"])
                n_lines += 1
                """
                indexes.append(item[0])
                indexes.append(idx_REQUEST_DATA)
                indexes.append(idx_RESPONSE_DATA)
                """
    # print(n_lines)
    # df_log_org.loc[indexes, "line"].to_csv("oauth_org.csv", index=False, header=False, quoting=3, escapechar="\\", encoding="utf-8")

    
if __name__ == "__main__":
    main(sys.argv)
