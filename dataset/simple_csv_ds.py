import os
import errno
import pandas as pd
import numpy as np
from rich.progress import Progress

from dataset.dataset import DataSetBase


class SimpleCSVDataSet(DataSetBase):
    def __init__(self,
                 data: pd.DataFrame = None,
                 log_dir: str = None) -> None:
        super.__init__(data, log_dir)

    def prepare_dataset(self, source_dir: str, dest_file: str, dump: bool = True):
        """
        把以各个股票代号为单位下载的csv文件聚合成一个符合SimpleCSVDataset数据集格式的csv

        Params:
        source_dir: 各个股票文件下载目录
        dest_dir: 聚合之后文件
        dump: 是否保存
        """
        if not os.path.exists(source_dir):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), source_dir)

        if os.path.exists(dest_file):
            raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), dest_file) 

        total_task_num = len(os.listdir(source_dir))
        with Progress() as progress:
            concat_task = progress.add_task("concating file...", total=total_task_num)
            for inst_file in os.listdir(source_dir):
                inst_file_full = os.path.join(source_dir, inst_file)
                inst_df = pd.read_csv(inst_file_full)
                inst_df.to_csv(dest_file, mode="a")
                if not progress.finished:
                    progress.update(concat_task, advance=1)
        print("sorting by date...")
        dest_df = pd.read_csv(dest_file)
        dest_df.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)
        if dump:
            dest_df.to_csv(dest_file)
        else:
            os.rmdir(dest_file)
        self.data = dest_df
        return dest_df

    @staticmethod
    def compute_return(data: pd.DataFrame, delay_day: int = 1, is_single_inst: bool = True):
        """
        给定单个股票的数据，或者多个股票的数据，计算delay_day的收益

        Params:
        data: 待处理的数据
        delay_day: 收益延迟天数
        is_single_inst: 数据是否只包含单个股票的数据
        """
        if is_single_inst:
            null_num = np.sum(data.isnull().values)
            if null_num > 0:
                
            if data.isnull().values.any():
                data.fillna(method="ffil", inplace=True)