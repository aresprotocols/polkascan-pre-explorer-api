import logging


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
