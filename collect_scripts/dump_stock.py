from enum import IntEnum, unique


@unique
class StockDumpType(IntEnum):
    # 下载开始日期的所有股票
    START_DATE = 1
    # 下载结束日期的所有股票
    END_DATE = 2
    # 下载开始日期和结束日期股票的并集
    UNION = 3
    # 下载开始日期和结束日期股票的交集
    INTERSECTION = 4


class DumpStockBase(object):
    """
    所有股票下载数据的基类接口
    """

    @staticmethod
    def find_all_stock_ids(date: str, save_dir: str):
        """
        查询某个日期的所有股票的代码，保存成stock_ids.csv

        Params:
        date: str, 股票查询日期，例如"2021-07-01"
        save_dir: str, 查询结果保存目录 
        """
        raise NotImplementedError

    @staticmethod
    def dump_stock_data(start_date: str,
                        end_date: str,
                        feature_names: list,
                        stock_dump_mode: StockDumpType, 
                        stock_ids_dir: str,
                        save_data_dir: str):
        """
        下载从start_date到end_date之间的所有之间存在的股票数据

        Params:
        start_date: 下载数据开始时间, 例如"2019-07-01"
        end_date: 下载数据结束时间，例如"2022-05-01"
        feature_names: 所有特征的集合，例如["date", "code", "open"]
        stock_ids_dir: 股票代码数据存储路径
        save_data_dir: 存储下载数据的目录，默认是None，使用BaoStock.data_save_dir
        """
        raise NotImplementedError