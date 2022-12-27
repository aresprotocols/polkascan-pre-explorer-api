import falcon
import logging
import time
from sqlalchemy import func
from app.models.data import EstimatesParticipants, EstimatesWinner, EstimatesDataList, DataReminder, \
    DataReminderLifecycle
from app.resources.base import JSONAPIResource, JSONAPIDetailResourceFilterWithDb


# class ReminderList(JSONAPIDetailResourceFilterWithDb):
#     cache_expiration_time = 6
#
#     def process_get_response(self, req, resp, **kwargs):
#         account = kwargs.get("acc")
#         item = self.get_item(account)
#         response = {
#             'status': falcon.HTTP_200,
#             'media': self.get_jsonapi_response(
#                 data=self.serialize_item(item),
#                 relationships={},
#                 meta=self.get_meta()
#             ),
#             'cacheable': True
#         }
#         return response
#
#     def get_item(self, account):
#
#         symbol_prices: [] = self.session.query(self.session). \
#             join(DataReminderLifecycle, DataReminderLifecycle.reminder_id == DataReminder.reminder_id). \
#             filter(owner_ss58=account). \
#             order_by(DataReminder.block_id.desc()).limit().all()
#
#         # tmp = self.session.query(EstimatesParticipants.estimate_type).filter_by(
#         #     symbol=symbol,
#         #     estimate_id=estimate_id
#         # ).first()
#         # if tmp and tmp.estimate_type == 'deviation':
#         #     results = EstimatesParticipants.query(self.session).filter_by(symbol=symbol, estimate_id=estimate_id).all()
#         #     items = [row for row in results]
#         #     items.sort(key=lambda r: r.price)
#         #     sum_price = 0
#         #     for item in items:
#         #         sum_price += item.price
#         #
#         #     return {
#         #         "total": len(items),
#         #         "avg": str(sum_price / len(items)),
#         #         "median": str(items[len(items) // 2].price)
#         #     }
#         # if tmp and tmp.estimate_type == 'range':
#         #     results = self.session.query(EstimatesParticipants.option_index,
#         #                                  func.count(EstimatesParticipants.option_index)). \
#         #         filter_by(symbol=symbol, estimate_id=estimate_id). \
#         #         group_by(EstimatesParticipants.option_index).all()
#         #     return [{"index": row[0], "count": row[1]} for row in results]

class ReminderListByAccount(JSONAPIDetailResourceFilterWithDb):
    cache_expiration_time = 6
    acc_id = ''

    def get_item_url_name(self):
        return 'acc'

    def get_meta(self):
        if self.acc_id == '':
            return {'total_count': 0}
        else:

            results = self.session.query(func.count(DataReminder.id)). \
                join(DataReminderLifecycle, DataReminderLifecycle.reminder_id == DataReminder.reminder_id). \
                filter(DataReminder.owner_ss58 == self.acc_id, DataReminderLifecycle.is_released.is_(None)).first()

            if results is None:
                return {'total_count': 0}
            else:
                return {'total_count': int(results[0])}


    def get_item(self, acc_id, offset, size_num):
        print("item_id", acc_id, "offset", offset, "size_num", size_num)
        self.acc_id = acc_id

        # for c, i in self.session.query(DataReminder, DataReminderLifecycle).filter(Customer.id == Invoice.custid).all():
        #     print("ID: {} Name: {} Invoice No: {} Amount: {}".format(c.id, c.name, i.invno, i.amount))

        reminder_list = self.session.query(
            DataReminder.reminder_id,
            DataReminder.block_id,
            DataReminder.owner,
            DataReminder.owner_ss58,
            DataReminder.interval_bn,
            DataReminder.repeat_count,
            DataReminder.create_bn,
            DataReminder.price_snapshot,
            DataReminder.trigger_condition_type,
            DataReminder.trigger_condition_price_key,
            DataReminder.anchor_price,
            DataReminder.trigger_receiver_type,
            DataReminder.trigger_receiver_url,
            DataReminder.trigger_receiver_sign,
            DataReminder.update_bn,
            DataReminder.tip,
            DataReminderLifecycle.points,
            DataReminderLifecycle.is_released
        ). \
            join(DataReminderLifecycle, DataReminderLifecycle.reminder_id == DataReminder.reminder_id). \
            filter(DataReminder.owner_ss58 == acc_id, DataReminderLifecycle.is_released.is_(None)).order_by(DataReminder.block_id.desc()).offset(offset).limit(size_num)[:size_num]

        # print('reminder_list = 55 ', reminder_list[0])

        # for key,val in reminder_list[0]:
        #     print('row --3 ', key,val)
        #     # for inv in row.invoices:
        #     #     print(row.id, row.name, inv.invno, inv.amount)

        data = {
            'name': 'reminder',
            'type': 'line',
            'data': [
                {
                    'reminder_id': reminder_data.reminder_id,
                    'block_id': reminder_data.block_id,
                    'owner': reminder_data.owner,
                    'owner_ss58': reminder_data.owner_ss58,
                    'interval_bn': reminder_data.interval_bn,
                    'repeat_count': reminder_data.repeat_count,
                    'create_bn': str(reminder_data.create_bn),
                    'price_snapshot': float(reminder_data.price_snapshot),
                    'trigger_condition_type': reminder_data.trigger_condition_type,
                    'trigger_condition_price_key': reminder_data.trigger_condition_price_key,
                    'anchor_price': float(reminder_data.anchor_price),
                    'trigger_receiver_type': reminder_data.trigger_receiver_type,
                    'trigger_receiver_url': reminder_data.trigger_receiver_url,
                    'trigger_receiver_sign': reminder_data.trigger_receiver_sign,
                    'update_bn': str(reminder_data.update_bn),
                    'tip': reminder_data.tip,
                    'points': reminder_data.points,
                    'is_released': reminder_data.is_released,
                }
                for reminder_data in reminder_list
            ]
        }
        return data
