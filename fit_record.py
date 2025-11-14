import os
import pandas as pd
import fitdecode

def parse_fit_file_record(filepath):
    """使用 fitdecode 解析单个 .fit 文件并生成 record 及汇总数据"""
    fit_data = []
    with fitdecode.FitReader(filepath, check_crc=False) as fit:
        for frame in fit:
            if isinstance(frame, fitdecode.FitDataMessage) and frame.name == "record": #data_type: lap/ session/ record
                data = {field.name: field.value for field in frame.fields if field.value is not None}
                fit_data.append(data)
    if not fit_data:
        return pd.DataFrame()

    df = pd.DataFrame(fit_data)

    summary = {}
    summary["source_file"] = os.path.basename(filepath)
    # === ① 提取开始/结束时间 ===
    # start_sport_time = df.iloc[0].get("timestamp")
    # end_sport_time = df.iloc[-1].get("timestamp")
    # 添加文件源与起止时间
    # summary["start_sport_time"] = start_sport_time
    # summary["end_sport_time"] = end_sport_time

    # === ② 数值汇总计算 ===
    sum_fields = [
        "total_timer_time", "total_elapsed_time", "total_distance",
        "total_calories", "total_descent", "total_ascent"
    ]
    mean_fields = [
        "avg_heart_rate", "avg_temperature", "enhanced_avg_speed", "avg_speed",
        "avg_running_cadence", "avg_step_length", "avg_power",
        "avg_stance_time", "avg_vertical_oscillation", "avg_vertical_ratio","speed","heart_rate","vertical_oscillation","stance_time","vertical_ratio","cadence","enhanced_speed","power","step_length","Effort Pace"
    ] 
    min_fields = ["min_heart_rate"]
    max_fields = [
        "max_heart_rate", "enhanced_max_speed", "max_speed", "max_running_cadence","altitude","distance","enhanced_altitude","accumulated_power"
    ]

    # 求和
    for f in sum_fields:
        if f in df.columns:
            summary[f] = df[f].sum()

    # 平均（忽略 0 和空值）
    for f in mean_fields:
        if f in df.columns:
            valid_values = df[f][(df[f].notna()) & (df[f] != 0)]
            summary[f] = f"{valid_values.mean():.0f}" if not valid_values.empty else None

    # 最小值
    for f in min_fields:
        if f in df.columns:
            summary[f] = df[f].min()

    # 最大值
    for f in max_fields:
        if f in df.columns:
            summary[f] = df[f].max()

    # === 将汇总数据添加到 DataFrame ===
    df_summary = pd.DataFrame([summary])
    # df_summary["message_index"] = "summary"  # 特殊标记

    # df_summary["数据类型"] = "recode"  # 特殊标记
    df_summary.drop(columns=["Effort Pace"], inplace=True) #删除非必要字段
    df_summary.drop(columns=["cadence","enhanced_speed","step_length","enhanced_altitude","speed","heart_rate","distance","power"], inplace=True) #删除合并后多余数据
    return df_summary

    # df_final = pd.concat([df, df_summary], ignore_index=True) # 原数据和汇总数据合并表格
    # return df


# if __name__ == "__main__":
#     data_types = ["record"]#"record" ,"lap","session"
#     FIT_FOLDER = "./fit_files"
#     for data_type in data_types:
        
#         OUTPUT_FILE = f"./dataFrame/fit_{data_type}_data1111111111111.xlsx"
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
