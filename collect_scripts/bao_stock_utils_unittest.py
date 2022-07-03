import os
import unittest
from bao_stock_utils import BaoStockUtils


class TestBaoStockUtils(unittest.TestCase):
    def test_find_all_stock_ids(self):
        tmp_dir = "./data/test_data"
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        if os.path.exists(os.path.join(tmp_dir, "stock_ids.csv")):
            os.remove(os.path.join(tmp_dir, "stock_ids.csv"))
        BaoStockUtils.find_all_stock_ids(date="2021-07-01", save_dir=tmp_dir)
        self.assertTrue(os.path.exists(os.path.join(tmp_dir, "2021-07-01-stock_ids.csv")))


if __name__ == "__main__":
    unittest.main()