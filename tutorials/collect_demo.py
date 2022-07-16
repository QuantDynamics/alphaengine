from collect_scripts.bao_stock_utils import BaoStockUtils


def main():
    BaoStockUtils.dump_index_ingredients(start_date="2001-01-05", end_date="2022-07-01", save_dir="/data/stock_data/sz50")

if __name__ == "__main__":
    main()