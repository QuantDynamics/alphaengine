import os
import argparse
from collect_scripts.bao_stock_utils import BaoStockUtils


def main(args):
    pass


if __name__ == "__main__":
    home_dir = os.environ["HOME"]
    parser = argparse.ArgumentParser(description="download A stock data using baostock")
    parser.add_argument("--stock_id_dir", type=str, default=os.path.join(home_dir, "stock_data/stock_ids"), help="股票代码存储目录")
    parser.add_argument("--stock_data_dir", type=str, default=os.path.join(home_dir, "stock_data/daily_data"), help="股票日间数据存储目录")
    parser.add_argument("--start_date", type=str, default="2000-01-04", help="股票开始时间")
    parser.add_argument("--end_date", type=str, default="2022-05-13", help="股票结束时间")
    args = parser.parse_args()
    BaoStockUtils.find_all_stock_ids(date=args.start_date, save_dir=args.stock_id_dir)
    BaoStockUtils.find_all_stock_ids(date=args.end_date, save_dir=args.stock_id_dir)
    BaoStockUtils.dump_stock_data(start_date=args.start_date, end_date=args.end_date, stock_ids_dir=args.stock_id_dir, save_data_dir=args.stock_data_dir)


    