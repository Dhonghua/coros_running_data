import os
import pandas as pd
import fitdecode

from process_columns import process_columns

def parse_fit_file_lap(filepath):
    """使用 fitdecode 解析单个 .fit 文件并生成 lap """
    fit_data = []
    with fitdecode.FitReader(filepath, check_crc=False) as fit:
        for frame in fit:
            if isinstance(frame, fitdecode.FitDataMessage) and frame.name == "lap": #data_type: lap/ session/ record
                data = {field.name: field.value for field in frame.fields if field.value is not None}
                fit_data.append(data)

    if not fit_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(fit_data)

    # === 将汇总数据添加到 DataFrame ===
    df["source_file"] = os.path.basename(filepath)
    
    start_sport_time = df.iloc[0].get("start_time")
    end_sport_time = df.iloc[-1].get("timestamp")
    # 添加文件源与起止时间
    df["start_sport_time"] = start_sport_time
    df["end_sport_time"] = end_sport_time

    #删除非必要字段
    cols_to_drop = ["timestamp","start_time","sport","avg_stance_time_percent",
                    "enhanced_avg_speed","enhanced_max_speed"]

    df = process_columns(df,cols_to_drop,"") #删除非必要字段

    # 字段重新排序
    cols_to_front = ["source_file","message_index","start_sport_time","end_sport_time","total_timer_time","total_elapsed_time","total_distance","avg_speed","Effort Pace","avg_heart_rate","avg_running_cadence","avg_step_length","avg_stance_time","avg_vertical_oscillation","avg_vertical_ratio","avg_power","max_speed","max_heart_rate","min_heart_rate","max_running_cadence","total_descent","total_ascent"] # 重新排序

    df = process_columns(df,"",cols_to_front) # 重新排序
    
    return df


# if __name__ == "__main__":
#     data_types = ["lap"]#"record" ,"lap","session"
#     FIT_FOLDER = "./fit_files"

#     for data_type in data_types:
#         OUTPUT_FILE = f"./dataFrame/fit_{data_type}_data.xlsx"
#         print("=== 开始批量解析 FIT 文件 ===")
#         df_all = process_fit_dataframe(parse_all_fit_files(FIT_FOLDER,data_type))
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
