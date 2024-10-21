
from product_data_collector_api import product_data_collector

def test_product_data_collector():
    product_data_collector("file:///home/takeo/product.txt","file:///home/takeo/parq/proudct_data.parquet")


if __name__ == '__main__':
    test_product_data_collector()
