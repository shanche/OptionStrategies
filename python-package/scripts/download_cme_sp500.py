from qlib.web import CMESP500Scraper
from datetime import datetime


if __name__ == '__main__':
    run_date = datetime.today().strftime('%Y%m%d')

    scraper = CMESP500Scraper()
    scraper.load_all()

    data_dir = r'/home/yue/study/OptionStrategies/data/CMESP500'

    print("Loaded {} futures contracts".format(scraper.futures_data.shape[0]))
    scraper.futures_data.to_csv('{}/CMESP500Futures_{}.csv'.format(data_dir, run_date))

    print("Loaded {} options contracts".format(scraper.options_data.shape[0]))
    scraper.options_data.to_csv('{}/CMESP500Futures_{}.csv'.format(data_dir, run_date))

