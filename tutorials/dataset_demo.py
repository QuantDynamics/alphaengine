from operator import concat
from dataset.simple_csv_ds import SimpleCSVDataSet


def main():
    stock_data_dir = "/data/stock_data/daily_data"
    concat_data_file = "/data/stock_data/daily_data_full.csv"
    test_data_file = "/data/stock_data/daily_data_full.csv"
    # test_data_file = "/data/stock_data/test_data.csv"
    simple_dataset = SimpleCSVDataSet()
    simple_dataset.prepare_dataset(stock_data_dir, concat_data_file)
    # simple_dataset.load_dataset(test_data_file)
    # print(simple_dataset.data)
    # simple_dataset.compute_returns()


if __name__ == "__main__":
    main()
