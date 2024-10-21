from sales_data_collector_api import sales_data_collector_api



def test_sales_data_collectorr():
    sales_data_collector_api("file:///home/takeo/sales_data.txt")

if __name__ == '__main__':
    test_sales_data_collectorr()
