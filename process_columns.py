import pandas as pd

def process_columns(df, cols_to_drop=None, cols_to_front=None):
    """
    对 DataFrame 的列进行智能处理：
    1. 删除指定列（如果存在）
    2. 将指定列移动到前面（如果存在）
    3. 自动跳过不存在的列或空列

    参数：
        df : pd.DataFrame
        cols_to_drop : list[str] 要删除的列
        cols_to_front : list[str] 要放到最前面的列

    返回：
        pd.DataFrame 处理后的 DataFrame
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df 必须是 pandas.DataFrame")

    # 去掉重复列
    df = df.loc[:, ~df.columns.duplicated()]

    # 删除列
    if cols_to_drop:
        cols_to_drop_existing = [c for c in cols_to_drop if c in df.columns]
        if cols_to_drop_existing:
            df = df.drop(columns=cols_to_drop_existing)
            print(f"已删除列: {cols_to_drop_existing}")

    # 移动列到前面
    if cols_to_front:
        cols_to_front_existing = [c for c in cols_to_front if c in df.columns]
        if cols_to_front_existing:
            cols = cols_to_front_existing + [c for c in df.columns if c not in cols_to_front_existing]
            df = df[cols]
            print(f"列顺序已调整，前置列: {cols_to_front_existing}")

    return df
