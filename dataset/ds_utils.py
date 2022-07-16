import pandas as pd


def compute_returns(close_price: pd.Series, delay_day:int = 1) -> pd.Series:
    """
    给定收盘价格，计算间隔delay_day天数的收益，如果没前delay_day的数据，则补填0

    Params:
    close_price: pd.Series, index 按照时间升序排列的收盘价格
    delay_day: 延迟的天数
    Return:
    返回一个名叫returns代表收益的Series
    """
    returns = close_price.shift(periods=delay_day, axis=0) / close_price - 1
    returns.fillna(0, inplace=True)
    returns.name = "returns"
    return returns