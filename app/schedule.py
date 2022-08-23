import logging
import time

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.resources.base import create_substrate
from app.tasks.chain_data import ChainDataTask
from app.tasks.chart import AresChartTask
from app.tasks.reward import RequestRewardTask
from app.tasks.symbols import SymbolsPriceTask
from app.settings import DB_CONNECTION, DEBUG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == '__main__':

    jobstores = {'default': SQLAlchemyJobStore(url='sqlite:///jobs.'+str(time.time())+'.sqlite')}
    executors = {'default': ThreadPoolExecutor(10), 'processpool': ProcessPoolExecutor(10)}
    job_defaults = {'coalesce': False, 'max_instances': 10}

    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.start()
    ares_chart = AresChartTask(DB_CONNECTION, DEBUG)
    request_reward = RequestRewardTask()
    symbols_price = SymbolsPriceTask(DB_CONNECTION, DEBUG)
    chain_data = ChainDataTask(DB_CONNECTION, DEBUG)
    substrate = create_substrate()


    def subscription_handler(new_block, update_nr, subscription_id):
        events = substrate.get_events(block_hash=new_block['header']['parentHash'])
        for event in events:
            event = event.value
            if event['module_id'] == 'OracleFinance' and event['event_id'] == 'PurchaseRewardToken':
                print("detect PurchaseRewardToken event..refresh cache")
                request_reward.run()
                break


    print("############## first run.")
    print('RUN - ares_chart ----------------------------')
    ares_chart.run()
    time.sleep(5)
    print('RUN - request_reward ----------------------------')
    request_reward.run()
    time.sleep(5)
    print('RUN - symbols_price ----------------------------')
    symbols_price.run()
    time.sleep(5)
    print('RUN - chain_data ----------------------------')
    chain_data.run()

    print("############## add schedule tasks.")

    time.sleep(2)
    scheduler.add_job(ares_chart.run, 'interval', minutes=60, id='ares_chart')
    time.sleep(2)
    scheduler.add_job(request_reward.run, 'interval', minutes=10, id='request_reward')
    time.sleep(2)
    scheduler.add_job(symbols_price.run, 'interval', minutes=10, id='symbols_price')
    time.sleep(2)
    scheduler.add_job(chain_data.run, 'interval', minutes=30, id='chain_data')
    time.sleep(2)

    # scheduler.add_job(
    #     ares_chart.run,
    #     trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="*/2", second="0"),
    #     # args=[],
    #     name="ares_chart2",
    # )
    # time.sleep(5)
    # scheduler.add_job(
    #     request_reward.run,
    #     trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="*/2", second="0"),
    #     # args=[],
    #     name="reqeust_reward",
    # )
    # time.sleep(5)
    # scheduler.add_job(
    #     symbols_price.run,
    #     trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="*/2", second="0"),
    #     # args=[],
    #     name="symbols price",
    # )
    # time.sleep(5)
    # scheduler.add_job(
    #     chain_data.run,
    #     trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="*/2", second="0"),
    #     # args=[],
    #     name="chain data",
    # )

    substrate.subscribe_block_headers(subscription_handler=subscription_handler, finalized_only=True,
                                      include_author=False)

    while True:
        print('----------- wait.')
        time.sleep(120)
