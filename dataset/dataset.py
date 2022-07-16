import os
from abc import abstractmethod
import pandas as pd
from utils.logger import StockLogger


class DataSetBase(object):
    def __init__(self,
                 data: pd.DataFrame = None,
                 log_dir: str = None) -> None:
        self.data = data
        if log_dir is None:
            self.logger = StockLogger(name="dataset",
                                      file_name=os.path.join(os.getcwd(), "dataset.log"))
        else:
            self.logger = StockLogger(name="dataset", file_name=log_dir)

    @abstractmethod
    def prepare_dataset(self, source_dir: str, dest_file: str, dump: bool):
        raise NotImplementedError
    
    @abstractmethod
    def load_dataset(self, ds_file: str):
        raise NotImplementedError

    @abstractmethod
    def split_dataset(self, ratios: list):
        """
        将数据集拆分成train_ds, val_ds, test_ds, 数据比例按照ratios。
        """
        assert len(ratios) == 3
        self.train_ds = None
        self.val_ds = None
        self.test_ds = None