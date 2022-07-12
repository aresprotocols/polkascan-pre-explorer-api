import logging
import time

from app.tasks.chain_data import ChainDataTask
from app.tasks.chart import AresChartTask
from app.tasks.reward import RequestRewardTask
from app.tasks.symbols import SymbolsPriceTask

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == '__main__':

    ares_chart = AresChartTask()
    request_reward = RequestRewardTask()
    symbols_price = SymbolsPriceTask()
    chain_data = ChainDataTask()

    print("RUN ares_chart")
    ares_chart.run()
    time.sleep(5)

    print("RUN request_reward")
    request_reward.run()
    time.sleep(5)

    print("RUN symbols_price")
    symbols_price.run()
    time.sleep(5)

    print("RUN chain_data")
    chain_data.run()
    time.sleep(5)

    print("Refresh done.")
