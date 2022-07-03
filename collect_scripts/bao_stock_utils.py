import os
import baostock as bs
import pandas as pd
from utils.logger import StockLogger
from collect_scripts.dump_stock import DumpStockBase, StockDumpType


class BaoStockUtils(DumpStockBase):
    """
    This class encapsulate helper functions of baostock api
    这个类封装了各种baostock库的易用接口

    Params:
    data_save_dir: str, 用于保存数据的根目录
    """
    data_save_dir: str = "/data"
    file_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(os.path.dirname(file_dir), "log")
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    logger = StockLogger(name="baostock", file_name=os.path.join(log_path, "baostock.log")).logger

    @staticmethod
    def find_all_stock_ids(date: str = "2021-07-01", save_dir: str = None):
        """
        查询某个日期的所有股票的代码，保存成stock_ids.csv

        Params:
        date: str, 股票查询日期，例如"2021-07-01"
        save_dir: str, 查询结果保存目录, 默认None，使用BaoStockUtils.data_save_dir
        """
        if save_dir is None:
            save_dir = BaoStockUtils.data_save_dir
            BaoStockUtils.logger.warn(f"save_dir is None, use {BaoStockUtils.data_save_dir} instead")
        if not os.path.exists(save_dir):
            BaoStockUtils.logger.info(f"save_dir:{save_dir}does not exist, create it")
            os.makedirs(save_dir)
        _ = bs.login()
        rs = bs.query_all_stock(day=date)
        data = rs.get_data()
        stock_id_file = os.path.join(save_dir, f"{date}-stock_ids.csv")
        data.to_csv(stock_id_file, index=False)
        BaoStockUtils.logger.info(f"Download stock ids on {date}, to {stock_id_file}!")
        bs.logout()

    @staticmethod
    def dump_stock_data(start_date: str = "2019-07-01",
                        end_date: str = "2021-07-01",
                        feature_names: list = ["date", "code", "open", "high", "low", "close", "preclose", "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST"],
                        stock_dump_mode: StockDumpType = StockDumpType.END_DATE, 
                        stock_ids_dir: str = None,
                        save_data_dir: str = None):
        """
        下载从start_date到end_date之间的所有之间存在的股票数据

        Params:
        start_date: 下载数据开始时间, 例如"2019-07-01"
        end_date: 下载数据结束时间，例如"2022-05-01"
        stock_ids_dir: 股票代码数据存储路径
        save_data_dir: 存储下载数据的目录，默认是None，使用BaoStock.data_save_dir
        """
        if stock_ids_dir is None:
            BaoStockUtils.logger.info(f"stock_ids_dir is None, download stock_ids.csv to {BaoStockUtils.data_save_dir}")
            BaoStockUtils.find_all_stock_ids(date=start_date, save_dir=BaoStockUtils.data_save_dir)
            BaoStockUtils.find_all_stock_ids(date=end_date, save_dir=BaoStockUtils.data_save_dir)
            stock_ids_dir = BaoStockUtils.data_save_dir

        # 这边可以用一个函数优化，不需要写两遍
        if stock_dump_mode != StockDumpType.END_DATE:
            try:
                start_df = pd.read_csv(os.path.join(stock_ids_dir, f"{start_date}-stock_ids.csv"))
            except FileNotFoundError:
                BaoStockUtils.logger.warn(f"Open stock ids with start date:{start_date} failed")
                BaoStockUtils.logger.info(f"System re-download stock ids with date:{start_date}")
                BaoStockUtils.find_all_stock_ids(date=start_date, save_dir=stock_ids_dir)
                start_df = pd.read_csv(os.path.join(stock_ids_dir, f"{start_date}-stock_ids.csv"))
            start_df = start_df.dropna(axis=0, how="any")
            start_ids = start_df[["code"]][~start_df["code_name"].str.contains("指数")]
            start_ids = list(start_ids["code"])

        if stock_dump_mode != StockDumpType.START_DATE:
            try:
                end_df = pd.read_csv(os.path.join(stock_ids_dir, f"{end_date}-stock_ids.csv"))
            except FileNotFoundError:
                BaoStockUtils.logger.warn(f"Open stock ids with end date:{end_date} failed")
                BaoStockUtils.logger.info(f"System re-download stock ids with date:{end_date}")
                BaoStockUtils.find_all_stock_ids(date=end_date, save_dir=stock_ids_dir)
                end_df = pd.read_csv(os.path.join(stock_ids_dir, f"{end_date}-stock_ids.csv"))
            end_df = end_df.dropna(axis=0, how="any")
            end_ids = end_df[["code"]][~end_df["code_name"].str.contains("指数")]
            end_ids = list(end_ids["code"])

        dump_stock_ids = []
        if stock_dump_mode == StockDumpType.START_DATE:
            dump_stock_ids = start_ids
        elif stock_dump_mode == StockDumpType.END_DATE:
            dump_stock_ids = end_ids
        elif stock_dump_mode == StockDumpType.INTERSECTION:
            dump_stock_ids = list(set(start_ids) & set(end_ids))
        elif stock_dump_mode == StockDumpType.UNION:
            dump_stock_ids = list(set(start_ids) | set(end_ids))

        _ = bs.login()
        if save_data_dir is None:
            BaoStockUtils.logger.info(f"save_data_dir is None, use {BaoStockUtils.data_save_dir} instead")
            save_data_dir = BaoStockUtils.data_save_dir
        if not os.path.exists(save_data_dir):
            BaoStockUtils.logger.info("save_data_dir does not exists, create it")
            os.makedirs(save_data_dir)
        for stock_id in dump_stock_ids:
            BaoStockUtils.logger.info(f"Download stock {stock_id} data")
            fields = BaoStockUtils.feature_names_to_fields(feature_names)
            rs = bs.query_history_k_data_plus(
                code=stock_id, fields=fields, start_date=start_date, end_date=end_date)
            data = rs.get_data()
            data.to_csv(os.path.join(save_data_dir, f"{stock_id}.csv"), index=False)
        bs.logout()

    @staticmethod
    def feature_names_to_fields(feature_names):
        fields = ""
        for feature in feature_names:
            fields += feature + ","
        return fields[:-1]
