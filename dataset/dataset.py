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