import logging

from dogpile.cache import make_region

from app.settings import DOGPILE_CACHE_SETTINGS

cache_region = make_region().configure(
    'dogpile.cache.redis',
    arguments={
        'host': DOGPILE_CACHE_SETTINGS['host'],
        'port': DOGPILE_CACHE_SETTINGS['port'],
        'db': DOGPILE_CACHE_SETTINGS['db'],
        'redis_expiration_time': 60 * 60 * 2,  # 2 hours
        'distributed_lock': True
    }
)


class BaseTask:

    def before(self):
        pass

    def after(self):
        pass

    def post(self):
        pass

    def run(self):
        try:
            logging.info("job start")
            self.before()
            self.post()
        except Exception as e:
            logging.error(e)
        finally:
            logging.info("job done")
            self.after()

    def cache_region(self):
        return cache_region
