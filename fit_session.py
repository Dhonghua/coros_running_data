import os
import pandas as pd
import fitdecode

def parse_fit_file_session(filepath):
    """使用 fitdecode 解析单个 .fit 文件并生成 session"""
    fit_data = []
    with fitdecode.FitReader(filepath, check_crc=False) as fit:
        for frame in fit:
            if isinstance(frame, fitdecode.FitDataMessage) and frame.name == "session": #data_type: lap/ session/ record
                data = {field.name: field.value for field in frame.fields if field.value is not None}
                fit_data.append(data)

    if not fit_data:
        return pd.DataFrame()

    df = pd.DataFrame(fit_data)

    # 替换列名
    df = df.rename(columns={"timestamp": "end_sport_time","start_time":"start_sport_time"})

    # === 将汇总数据添加到 DataFrame ===
    df["source_file"] = os.path.basename(filepath)
    
    df.drop(columns=["avg_stance_time","avg_stance_time_balance","avg_vertical_oscillation","avg_vertical_ratio","sport"], inplace=True) #删除非必要字段
    df.drop(columns=["enhanced_max_speed","enhanced_avg_speed"], inplace=True) #删除合并后多余数据
    return df


# if __name__ == "__main__":
    # data_types = ["session"]#"record" ,"lap","session"
#     for data_type in data_types:
        
#         OUTPUT_FILE = f"./dataFrame/fit_{data_type}_data111111111111.xlsx"
#         print("=== 开始批量解析 FIT 文件 ===")
#         df_all = process_fit_dataframe(parse_all_fit_files(FIT_FOLDER))

#         if not df_all.empty:
#             # 去掉 timestamp 时区
#             datetime_cols = ["start_time", "timestamp", "start_sport_time", "end_sport_time"]
#             for col in datetime_cols:
#                 if col in df_all.columns:
#                     df_all[col] = pd.to_datetime(df_all[col]).dt.tz_localize(None)

#             print(f"\n共解析 {len(df_all)} 条记录，包含字段数量：{len(df_all.columns)}")

#             # 字段替换为中文
#             df_all.rename(columns=CHINESE_COLUMNS, inplace=True)

#             if OUTPUT_FILE.endswith(".csv"):
#                 df_all.to_csv(OUTPUT_FILE, index=False)
#             else:
#                 df_all.to_excel(OUTPUT_FILE, index=False)

#             print(f"\n✅ 已导出到文件：{OUTPUT_FILE}")
#         else:
#             print("❗ 未生成任何数据。请检查 FIT 文件路径或内容。")
