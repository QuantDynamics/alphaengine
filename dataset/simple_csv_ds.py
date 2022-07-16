import os
import errno
import pandas as pd
import numpy as np
from rich.progress import Progress

from dataset.dataset import DataSetBase
from dataset import ds_utils


class SimpleCSVDataSet(DataSetBase):
    def __init__(self,
                 data: pd.DataFrame = None,
                 log_dir: str = None,
                 use_multi_index: bool = True,
                 delay_day: int = 1,
                 ) -> None:
        super(SimpleCSVDataSet, self).__init__(data, log_dir)
        # 使用date和code同时作为index
        self.use_multi_index = use_multi_index
        self.delay_day = delay_day

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
        columns = None
        count = 0
        with Progress() as progress:
            concat_task = progress.add_task("concating file...", total=total_task_num)
            for inst_file in os.listdir(source_dir):
                inst_file_full = os.path.join(source_dir, inst_file)
                inst_df = pd.read_csv(inst_file_full, index_col=0)
                inst_df.sort_values(by="date", ascending=True, inplace=True)
                returns = ds_utils.compute_returns(inst_df["close"]) 
                inst_df = pd.concat([inst_df, returns], axis=1)
                if columns is None:
                    columns = inst_df.columns
                if (inst_df.columns != columns).any():
                    print(f"inst_file:{inst_file} columns is different, skip it")
                    if not progress.finished:
                        progress.update(concat_task, advance=1)
                    continue
                if count == 0:
                    inst_df.to_csv(dest_file, mode="a")
                else:
                    inst_df.to_csv(dest_file, mode="a", header=False)
                count += 1
                if not progress.finished:
                    progress.update(concat_task, advance=1)
        print("sorting by date...")
        dest_df = pd.read_csv(dest_file)
        dest_df.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)
        if dump:
            dest_df.to_csv(dest_file, index=False)
        else:
            os.rmdir(dest_file)
        self.data = dest_df
        if self.use_multi_index:
            self.data.set_index(["date", "code"], inplace=True)
        return self.data 

    def load_dataset(self, ds_file: str):
        """
        Params:
        ds_file: 数据文件
        """
        if not os.path.exists(ds_file):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENONET), ds_file)
        self.data = pd.read_csv(ds_file)
        if self.use_multi_index:
            self.data.set_index(["date", "code"], inplace=True)

    def compute_returns(self):
        """
        计算delay_day的收益
        """
        group_by_stock = self.data.groupby("code")
        returns_res = [] 
        for code, g in group_by_stock:
            returns = g["close"].shift(periods=self.delay_day, axis=0) / g["close"] - 1
            returns.fillna(0, inplace=True)
            returns.name = "returns"
            g_full: pd.DataFrame = pd.concat([g, returns], axis=1)
            returns_res.append(g_full)
        self.data = pd.concat(returns_res, axis=0)
        print(self.data)

    def split_dataset(self, ratios: list):
        super().split_dataset(ratios)
        
        