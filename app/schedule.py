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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    jobstores = {'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')}
    executors = {'default': ThreadPoolExecutor(20), 'processpool': ProcessPoolExecutor(5)}
    job_defaults = {'coalesce': False, 'max_instances': 5}

    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.start()
    ares_chart = AresChartTask()
    request_reward = RequestRewardTask()
    symbols_price = SymbolsPriceTask()
    chain_data = ChainDataTask()
    substrate = create_substrate()


    def subscription_handler(new_block, update_nr, subscription_id):
        events = substrate.get_events(block_hash=new_block['header']['parentHash'])
        for event in events:
            event = event.value
            if event['module_id'] == 'OracleFinance' and event['event_id'] == 'PurchaseRewardToken':
                print("detect PurchaseRewardToken event..refresh cache")
                request_reward.run()
                break


    ares_chart.run()
    request_reward.run()
    symbols_price.run()
    chain_data.run()
    scheduler.add_job(
        ares_chart.run,
        trigger=CronTrigger(year="*", month="*", day="*", hour="*/6", minute="50", second="0"),
        # args=[],
        name="ares_chart",
    )
    time.sleep(1)
    scheduler.add_job(
        request_reward.run,
        trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="10", second="0"),
        # args=[],
        name="reqeust_reward",
    )
    time.sleep(1)
    scheduler.add_job(
        symbols_price.run,
        trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="15", second="0"),
        # args=[],
        name="symbols price",
    )
    time.sleep(1)
    scheduler.add_job(
        chain_data.run,
        trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="30", second="0"),
        # args=[],
        name="chain data",
    )
    substrate.subscribe_block_headers(subscription_handler=subscription_handler, finalized_only=True,
                                      include_author=False)
    # while True:
    #     time.sleep(120)
