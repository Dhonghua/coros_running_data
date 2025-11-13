import os
import pandas as pd
import fitdecode

data_types = ["record"]#"record" ,"lap","session"

FIT_FOLDER = "./fit_files"
CHINESE_COLUMNS = {
    "message_index": "Lap 索引",
    "timestamp": "时间戳",
    "start_time": "开始时间",
    "total_timer_time": "运动时长(hms)",
    "total_elapsed_time": "总时长(hms)",
    "total_distance": "总距离(km)",
    "sport" : "运动类型",
    "total_calories": "总卡路里(kcal)",
    "max_heart_rate": "最大心率",
    "min_heart_rate": "最小心率",
    "avg_heart_rate": "平均心率",
    "avg_temperature": "平均温度(℃)",
    "enhanced_max_speed": "增强最大速度(min/km)",
    "max_speed": "最大速度(min/km)",
    "enhanced_avg_speed": "增强平均速度(min/km)",
    "avg_speed": "平均速度(min/km)",
    "avg_running_cadence": "平均步频",
    "avg_step_length": "平均步幅(cm)",
    "max_running_cadence": "最大步频",
    "total_descent": "总下降(m)",
    "total_ascent": "总上升(m)",
    "avg_power": "平均功率(W)",
    "avg_stance_time": "平均触地时间(ms)",
    "avg_vertical_oscillation": "平均垂直振幅",
    "avg_vertical_ratio": "平均垂直比",
    "Effort Pace": "等强配速(min/km)",
    "start_sport_time": "运动开始时间",
    "end_sport_time": "运动结束时间",
    "source_file": "源文件",
    "heart_rate": "心率(bpm)",
    "speed": "速度(切片平均值)(min/km)",
    "step_length": "步幅(cm)",
    "vertical_oscillation": "垂直振幅(cm)",
    "enhanced_altitude": "增强海拔(m)",
    "stance_time": "接地时间(ms)",
    "altitude": "海拔(m)",
    "vertical_ratio": "垂直比(%)",
    "distance": "距离(km)",
    "total_strides": "总步数",
    "avg_stance_time_balance": "支撑时间平衡(%)",
    "avg_stance_time_percent": "平均支撑时间百分比(%)",
    "cadence": "步频(spm)",
    "enhanced_speed": "增强速度(m/s)",
    "power": "功率(W)",
    "accumulated_power": "累计功率(W·s)",
    "position_lat": "纬度",
    "position_long": "经度"
}

def process_fit_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    "对 FIT 文件解析后的 DataFrame 进行单位和格式转换"

   # ====== 单位换算辅助函数 ======
    def seconds_to_hms(seconds: float) -> str:
        """
        秒 → 时:分:秒
        例：3661.5 → '1:01:01'
        """
        
        try:
            if pd.isna(seconds):
                return None
            seconds = int(seconds)
            h, m, s = seconds // 3600, (seconds % 3600) // 60, seconds % 60
            return f"{h:02}:{m:02}:{s:02}"
        except (TypeError, ValueError):
                return None

    def meters_to_km(meters: float, precision: int = 2) -> float:
        """
        米 → 公里（浮点数，默认保留2位小数）
        例：11592.81 → 11.59
        """
        try:
            return f"{round(meters / 1000, precision)}km" if pd.notna(meters) else None
        except (TypeError, ValueError):
                return None

    def ms_to_kmh(speed_m_s: float, precision: int = 2) -> float:
        """
        米/秒 → 公里/小时
        1 m/s = 3.6 km/h
        例：4.405 → 15.86 km/h
        """
        try:
            return f"{round(speed_m_s * 3.6, precision)}/h" if pd.notna(speed_m_s) else None
        except (TypeError, ValueError):
            return None

    def ms_to_minkm(speed_m_s: float) -> str:
        """
        米/秒 → 配速（分钟/公里），返回字符串格式 例 '5:15/km'
        例：4.405 → '3:47/km'
        """
        try:
        # 转 float
            speed_m_s = float(speed_m_s)
            if speed_m_s <= 0:
                return None
            pace_min_per_km = 1000 / speed_m_s / 60  # 分钟/公里
            minutes = int(pace_min_per_km)
            seconds = int(round((pace_min_per_km - minutes) * 60))
            return f"{minutes:02d}\'{seconds:02d}\""
        except (TypeError, ValueError):
            return None
        
    
    def mm_to_cm(speed_mm_cm: int) -> float:
        """
        毫米 -> 厘米
        """
        try:
            speed_mm_cm = float(speed_mm_cm)   # 转数字
            return round(speed_mm_cm / 10, 1)
        except (TypeError, ValueError):
            return None  # 或者返回 0
    
    def cadence2(cadence:int) -> int:
        """
        fit文件保存单脚步频，需要乘二处理
        """
        try:
            cadence = float(cadence)
            return cadence*2
        except (TypeError, ValueError):
            return None  # 或者返回 0

    # ====== 执行转换 ======
    seconds_to_hms_cols = ["total_timer_time", "total_elapsed_time"]
    meters_to_km_cols = ["total_distance","distance"]
    # ms_to_kmh_cols = [""]
    ms_to_minkm_cols = ["enhanced_max_speed","max_speed","enhanced_avg_speed","avg_speed","Effort Pace","enhanced_speed","speed"]
    mm_to_cm_cols = ["avg_step_length","step_length"]
    cadence_cols = ["max_running_cadence","cadence","avg_running_cadence"]

    for col in seconds_to_hms_cols:
        if col in df.columns:
            df[col] = df[col].apply(seconds_to_hms) # 秒 → 时:分:秒

    for col in meters_to_km_cols:
        if col in df.columns:
            df[col] = df[col].apply(meters_to_km) # 米 → 公里

    # for col in ms_to_kmh_cols:
    #     if col in df.columns:
    #         df[col] = df[col].apply(ms_to_kmh) #  米/秒 → 公里/小时
    
    for col in ms_to_minkm_cols:
        if col in df.columns:
            df[col] = df[col].apply(ms_to_minkm) # 米/秒 → 配速（分钟/公里）
    for col in mm_to_cm_cols:
        if col in df.columns:
            df[col] = df[col].apply(mm_to_cm) # 毫米 -> 厘米

    for col in cadence_cols:
        if col in df.columns:
            print(f"步频转化前{df[col]}")
            df[col] = df[col].apply(cadence2)# 步频乘二
            print(f"步频转化后{df[col]}")

    return df


def parse_fit_file(filepath,data_type):
    """使用 fitdecode 解析单个 .fit 文件并生成 record 及汇总数据"""
    fit_data = []
    with fitdecode.FitReader(filepath, check_crc=False) as fit:
        for frame in fit:
            if isinstance(frame, fitdecode.FitDataMessage) and frame.name == data_type: #data_type: lap/ session/ record
                data = {field.name: field.value for field in frame.fields if field.value is not None}
                fit_data.append(data)

    if not fit_data:
        return pd.DataFrame()

    df = pd.DataFrame(fit_data)

    summary = {}
    
    summary["source_file"] = os.path.basename(filepath)
    # === ① 提取开始/结束时间 ===
    start_sport_time = df.iloc[0].get("timestamp")
    end_sport_time = df.iloc[-1].get("timestamp")
    # 添加文件源与起止时间
    summary["start_sport_time"] = start_sport_time
    summary["end_sport_time"] = end_sport_time

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
    df_summary["message_index"] = "summary"  # 特殊标记
    
    
    df_summary["data_type"] = "recode"  # 特殊标记
    return df_summary

    # df_final = pd.concat([df, df_summary], ignore_index=True) # 原数据和汇总数据合并表格
    # return df

def parse_all_fit_files(folder,data_type):
    """批量解析 .fit 文件"""
    all_dfs = []
    for fname in os.listdir(folder):
        if fname.lower().endswith(".fit"):
            fpath = os.path.join(folder, fname)
            print(f"正在解析: {fname}")
            try:
                df = parse_fit_file(fpath,data_type)
                if not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                print(f"❌ 解析失败 {fname}: {e}")

    if not all_dfs:
        print("未发现可解析的 FIT 文件。")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    return combined




if __name__ == "__main__":
    for data_type in data_types:
        
        OUTPUT_FILE = f"./dataFrame/fit_{data_type}_data.xlsx"
        print("=== 开始批量解析 FIT 文件 ===")
        df_all = process_fit_dataframe(parse_all_fit_files(FIT_FOLDER,data_type))

        if not df_all.empty:
            # 去掉 timestamp 时区
            datetime_cols = ["start_time", "timestamp", "start_sport_time", "end_sport_time"]
            for col in datetime_cols:
                if col in df_all.columns:
                    df_all[col] = pd.to_datetime(df_all[col]).dt.tz_localize(None)

            print(f"\n共解析 {len(df_all)} 条记录，包含字段数量：{len(df_all.columns)}")

            # 字段替换为中文
            df_all.rename(columns=CHINESE_COLUMNS, inplace=True)

            if OUTPUT_FILE.endswith(".csv"):
                df_all.to_csv(OUTPUT_FILE, index=False)
            else:
                df_all.to_excel(OUTPUT_FILE, index=False)

            print(f"\n✅ 已导出到文件：{OUTPUT_FILE}")
        else:
            print("❗ 未生成任何数据。请检查 FIT 文件路径或内容。")
