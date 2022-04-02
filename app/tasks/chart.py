import datetime

import sqlalchemy
from dogpile.cache import CacheRegion
from sqlalchemy.orm import scoped_session, Session

from app.main import session_factory
from app.models.data import BlockTotal
from app.tasks.base import BaseTask


class AresChartTask(BaseTask):
    session: 'Session'

    def before(self):
        _scoped_session = scoped_session(session_factory)
        self.session = _scoped_session()

    def after(self):
        self.session.close()
        self.session = None

    def post(self):
        limit = 14
        time_format = "%Y%m%d"
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)  # current date and time
        days = [(yesterday - datetime.timedelta(days=i)).strftime(time_format) for i in range(limit)]
        params = ",".join([':days{}'.format(i) for i, _ in enumerate(days)])
        values = {'days{}'.format(i): x for i, x in enumerate(days)}
        sql = sqlalchemy.text(
            "select min(id) as 'min', max(id) as 'max', full_day from data_block where full_day IN ({}) group by full_day".format(
                params))
        rows = self.session.execute(sql, params=values).all()
        blocks_per_day = {row[2]: {"min": row[0], "max": row[1]} for row in rows}

        params = []
        for row in rows:
            params.append(row[0])
            params.append(row[1])
        query = sqlalchemy.select([
            BlockTotal.id,
            BlockTotal.total_extrinsics_signed,
            BlockTotal.total_accounts_new,
            BlockTotal.total_blocktime,
        ], BlockTotal.id.in_(params))
        block_totals = [row._asdict() for row in self.session.execute(query).all()]

        charts = [
            {
                "type": "chart",
                "id": "utcday-accounts_new-sum-line-14",
                "attributes": {
                    "table": "utcday",
                    "metric": "accounts_new",
                    "metric_type": "sum",
                    "type": "line",
                    "limit": limit,
                    "column": "accounts_new_sum",
                    "title": "Total new accounts by day (UTC)",
                    "data": {
                        "x_axis_type": "datetime",
                        "series": [
                            {
                                "name": "Total new accounts by day (UTC)",
                                "type": "line",
                                "data": []
                            }
                        ]
                    },
                    "active": True
                }
            },
            {
                "type": "chart",
                "id": "utcday-blocktime-avg-line-14",
                "attributes": {
                    "table": "utcday",
                    "metric": "blocktime",
                    "metric_type": "avg",
                    "type": "line",
                    "limit": limit,
                    "column": "blocktime_avg",
                    "title": "Average blocktime by day (UTC)",
                    "data": {
                        "x_axis_type": "datetime",
                        "series": [
                            {
                                "name": "Average blocktime by day (UTC)",
                                "type": "line",
                                "data": []
                            }
                        ]
                    },
                    "active": True
                }
            },
            {
                "type": "chart",
                "id": "utcday-extrinsics_signed-sum-line-14",
                "attributes": {
                    "table": "utcday",
                    "metric": "extrinsics_signed",
                    "metric_type": "sum",
                    "type": "line",
                    "limit": limit,
                    "column": "extrinsics_signed_sum",
                    "title": "Total transactions by day (UTC)",
                    "data": {
                        "x_axis_type": "datetime",
                        "series": [
                            {
                                "name": "Total transactions by day (UTC)",
                                "type": "line",
                                "data": []
                            }
                        ]
                    },
                    "active": True
                }
            },
        ]

        for day in blocks_per_day:
            time = datetime.datetime.strptime(str(day), time_format).timestamp() * 1000
            time = int(time)

            max_total_extrinsics_signed = 0
            min_total_extrinsics_signed = 0
            max_total_accounts_new = 0
            min_total_accounts_new = 0
            max_total_blocktime = 0
            min_total_blocktime = 0

            blocks = blocks_per_day[day]
            for total in block_totals:
                if total["id"] == blocks['max']:
                    max_total_extrinsics_signed = total['total_extrinsics_signed']
                    max_total_accounts_new = total['total_accounts_new']
                    max_total_blocktime = total['total_blocktime']
                elif total["id"] == blocks['min']:
                    min_total_extrinsics_signed = total['total_extrinsics_signed']
                    min_total_accounts_new = total['total_accounts_new']
                    min_total_blocktime = total['total_blocktime']

            # accounts_new
            _data = charts[0]['attributes']['data']['series'][0]['data']
            _data.append([time, int(max_total_accounts_new - min_total_accounts_new)])

            # blocktime_avg
            _data = charts[1]['attributes']['data']['series'][0]['data']
            _data.append([time, int(max_total_blocktime - min_total_blocktime) / int((blocks['max'] - blocks['min']))])

            # extrinsics_signed
            _data = charts[2]['attributes']['data']['series'][0]['data']
            _data.append([time, int(max_total_extrinsics_signed - min_total_extrinsics_signed)])

        self.cache_region().set("ares_charts", charts)
