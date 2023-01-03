import falcon
import logging
import time
from sqlalchemy import func
from app.models.data import EstimatesParticipants, EstimatesWinner, EstimatesDataList, DataReminder, \
    DataReminderLifecycle, DataReminderMsg
from app.resources.base import JSONAPIResource, JSONAPIDetailResourceFilterWithDb


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
            DataReminder.datetime,
            DataReminderLifecycle.points,
            DataReminderLifecycle.is_released
        ). \
            join(DataReminderLifecycle, DataReminderLifecycle.reminder_id == DataReminder.reminder_id). \
            filter(DataReminder.owner_ss58 == acc_id, DataReminderLifecycle.is_released.is_(None)).order_by(DataReminder.block_id.desc()).offset(offset).limit(size_num)[:size_num]

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
                    'datetime': reminder_data.datetime.timestamp(),
                }
                for reminder_data in reminder_list
            ]
        }
        return data


class ReminderMsgByAccount(JSONAPIDetailResourceFilterWithDb):
    cache_expiration_time = 6
    acc_id = ''

    def get_item_url_name(self):
        return 'acc'

    def get_meta(self):
        if self.acc_id == '':
            return {'total_count': 0}
        else:

            results = self.session.query(func.count(DataReminderMsg.id)). \
                join(DataReminder, DataReminderMsg.reminder_id == DataReminder.reminder_id). \
                filter(DataReminder.owner_ss58 == self.acc_id).first()

            if results is None:
                return {'total_count': 0}
            else:
                return {'total_count': int(results[0])}


    def get_item(self, acc_id, offset, size_num):
        print("item_id", acc_id, "offset", offset, "size_num", size_num)
        self.acc_id = acc_id

        msg_list = self.session.query(
            DataReminderMsg.id,
            DataReminderMsg.submitter,
            DataReminderMsg.datetime,
            DataReminderMsg.reminder_id,
            DataReminderMsg.block_id,
            DataReminder.owner,
            DataReminder.owner_ss58,
            DataReminder.interval_bn,
            DataReminder.repeat_count,
            # DataReminder.create_bn,
            DataReminder.price_snapshot,
            DataReminder.trigger_condition_type,
            DataReminder.trigger_condition_price_key,
            DataReminder.anchor_price,
            DataReminder.trigger_receiver_type,
            DataReminder.trigger_receiver_url,
            DataReminder.trigger_receiver_sign,
            # DataReminder.update_bn,
            DataReminder.tip,
        ). \
            join(DataReminder, DataReminderMsg.reminder_id == DataReminder.reminder_id). \
            filter(DataReminder.owner_ss58 == acc_id).order_by(DataReminderMsg.block_id.desc()).offset(offset).limit(size_num)[:size_num]

        data = {
            'name': 'reminder',
            'type': 'line',
            'data': [
                {
                    'reminder_id': msg_data.reminder_id,
                    'block_id': msg_data.block_id,
                    'owner': msg_data.owner,
                    'owner_ss58': msg_data.owner_ss58,
                    # 'interval_bn': reminder_data.interval_bn,
                    # 'repeat_count': reminder_data.repeat_count,
                    # 'create_bn': str(msg_data.create_bn),
                    # 'price_snapshot': float(reminder_data.price_snapshot),
                    'trigger_condition_type': msg_data.trigger_condition_type,
                    'trigger_condition_price_key': msg_data.trigger_condition_price_key,
                    'anchor_price': float(msg_data.anchor_price),
                    'trigger_receiver_type': msg_data.trigger_receiver_type,
                    'trigger_receiver_url': msg_data.trigger_receiver_url,
                    'trigger_receiver_sign': msg_data.trigger_receiver_sign,
                    # 'update_bn': str(reminder_data.update_bn),
                    'tip': msg_data.tip,
                    'datetime': msg_data.datetime.timestamp(),
                }
                for msg_data in msg_list
            ]
        }
        return data
