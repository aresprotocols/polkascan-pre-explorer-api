import time
from datetime import datetime
import pytz

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor


def testTask():
    zone_of_shanghai = pytz.timezone("Asia/Shanghai")
    time_of_shanghai = datetime.now(zone_of_shanghai)
    print(" Hello ", time_of_shanghai.strftime("%Y-%m-%d %H:%M:%S"))

def testTask2():
    zone_of_shanghai = pytz.timezone("Asia/Shanghai")
    time_of_shanghai = datetime.now(zone_of_shanghai)
    print(" World ", time_of_shanghai.strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == '__main__':
    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:///jobs2.sqlite')
    }
    executors = {
        'default': ThreadPoolExecutor(20),
        'processpool': ProcessPoolExecutor(5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone='UTC')
    scheduler.start()

    print('add job with interval')
    scheduler.add_job(testTask, 'interval', minutes=1, id='my_job_id')
    print('done')

    print('add job with Trigger')
    scheduler.add_job(
        testTask2,
        trigger=CronTrigger(year="*", month="*", day="*", hour="*", minute="*/2", second="0"),
        # args=[],
        name="my_job_2",
    )

    while True:
        time.sleep(120)

