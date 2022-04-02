import logging
import time

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dogpile.cache import make_region

from app.settings import DOGPILE_CACHE_SETTINGS
from app.tasks.chart import AresChartTask
from app.tasks.reward import RequestRewardTask
from app.tasks.symbols import SymbolsPriceTask

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


if __name__ == '__main__':
    jobstores = {'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')}
    executors = {'default': ThreadPoolExecutor(20), 'processpool': ProcessPoolExecutor(5)}
    job_defaults = {'coalesce': False, 'max_instances': 5}

    cache_region = make_region().configure(
        'dogpile.cache.redis',
        arguments={
            'host': DOGPILE_CACHE_SETTINGS['host'],
            'port': DOGPILE_CACHE_SETTINGS['port'],
            'db': DOGPILE_CACHE_SETTINGS['db'],
            'redis_expiration_time': 60 * 60 * 2,  # 2 hours
            'distributed_lock': False
        }
    )
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.start()
    ares_chart = AresChartTask(cache_region=cache_region)
    request_reward = RequestRewardTask(cache_region=cache_region)
    symbols_chart = SymbolsPriceTask(cache_region=cache_region)
    while True:
        ares_chart.run()
        request_reward.run()
        symbols_chart.run()
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
            symbols_chart.run,
            trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="15", second="0"),
            # args=[],
            name="symbols price",
        )
        while True:
            time.sleep(120)
