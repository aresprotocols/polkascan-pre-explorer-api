import falcon
import logging
from sqlalchemy import func
from app.models.data import EstimatesParticipants, EstimatesWinner
from app.resources.base import JSONAPIResource, JSONAPIDetailResourceFilterWithDb


class StatisticsEstimate(JSONAPIResource):
    cache_expiration_time = 60 * 10

    def process_get_response(self, req, resp, **kwargs):
        symbol = kwargs.get("symbol")
        estimate_id = kwargs.get("id")
        item = self.get_item(symbol, estimate_id)
        response = {
            'status': falcon.HTTP_200,
            'media': self.get_jsonapi_response(
                data=self.serialize_item(item),
                relationships={},
                meta=self.get_meta()
            ),
            'cacheable': True
        }
        return response

    def get_item(self, symbol, estimate_id):
        tmp = self.session.query(EstimatesParticipants.estimate_type).filter_by(
            symbol=symbol,
            estimate_id=estimate_id
        ).first()
        if tmp and tmp.estimate_type == 'price':
            results = EstimatesParticipants.query(self.session).filter_by(symbol=symbol, estimate_id=estimate_id).all()
            items = [row for row in results]
            items.sort(key=lambda r: r.price)
            sum_price = 0
            for item in items:
                sum_price += item.price

            return {
                "total": len(items),
                "avg": str(sum_price / len(items)),
                "median": str(items[len(items) // 2].price)
            }
        if tmp and tmp.estimate_type == 'range':
            results = self.session.query(EstimatesParticipants.option_index,
                                         func.count(EstimatesParticipants.option_index)). \
                filter_by(symbol=symbol, estimate_id=estimate_id). \
                group_by(EstimatesParticipants.option_index).all()
            return [{"index": row[0], "count": row[1]} for row in results]


class EstimatesParticipantsList(JSONAPIDetailResourceFilterWithDb):
    cache_expiration_time = 0
    item_id = ''

    def get_meta(self):
        if self.item_id == '':
            return {'total_deposit': 0}
        else:
            results = self.session.query(EstimatesParticipants.ss58_address,
                                         func.sum(EstimatesParticipants.deposit)). \
                filter_by(ss58_address=self.item_id). \
                group_by(EstimatesParticipants.ss58_address).first()
            return {'total_deposit': str(results[1])}

    def get_item_url_name(self):
        return 'ss58'

    def get_item(self, item_id, offset, size_num):
        print("item_id", item_id, "offset", item_id, "size_num", size_num)
        self.item_id = item_id

        estimate_list: [EstimatesParticipants] = EstimatesParticipants.query(self.session).filter_by(
            ss58_address=item_id).order_by(EstimatesParticipants.block_id.desc()).offset(offset).limit(size_num)[
                                                 :size_num]

        # print("###########1")
        # print(estimate_list[1].block_id, estimate_list[1].symbol, estimate_list[1].price)
        # print("###########2")
        data = {
            'name': 'Price',
            'type': 'line',
            'data': [
                {
                    'block_id': estimate_item.block_id,
                    'symbol': estimate_item.symbol,
                    'estimate_id': estimate_item.estimate_id,
                    'estimate_type': estimate_item.estimate_type,
                    'participant': estimate_item.participant,
                    'ss58_address': estimate_item.ss58_address,
                    'price': None if estimate_item.price is None else str(estimate_item.price),
                    'deposit': None if estimate_item.deposit is None else str(estimate_item.deposit),
                    'option_index': estimate_item.option_index,
                    'created_at': str(estimate_item.created_at)
                }
                for estimate_item in estimate_list
            ]
        }
        return data


class EstimatesWinnerList(JSONAPIDetailResourceFilterWithDb):
    cache_expiration_time = 0
    item_id = ''

    def get_item_url_name(self):
        return 'ss58'

    def get_meta(self):
        if self.item_id == '':
            return {'total_reward': 0}
        else:
            results = self.session.query(EstimatesWinner.ss58_address,
                                         func.sum(EstimatesWinner.reward)). \
                filter_by(ss58_address=self.item_id). \
                group_by(EstimatesWinner.ss58_address).first()
            return {'total_reward': str(results[1])}

    def get_item(self, item_id, offset, size_num):
        print("item_id", item_id, "offset", item_id, "size_num", size_num)
        self.item_id = item_id
        # query = session.query(
        #     (User.first_name + ' ' + User.last_name).label('seller'),
        #     sa.func.count(OrderItem.id).label('unique_items'),
        #     sa.func.sum(OrderItem.qty).label('items_total'),
        #     sa.func.sum(OrderItem.qty * Product.price).label('order_amount'),
        # ).join(OrderItem).join(Product).group_by(User.id).order_by('items_total',
        #                                                            'order_amount')

        winner_list: [EstimatesWinner] = EstimatesWinner.query(self.session).filter_by(
            ss58_address=item_id).order_by(EstimatesWinner.block_id.desc()).offset(offset).limit(size_num)[:size_num]

        # winner_list: [EstimatesWinner] = []
        data = {
            'name': 'Price',
            'type': 'line',
            'data': [
                {
                    'block_id': winner_item.block_id,
                    'symbol': winner_item.symbol,
                    'estimate_id': winner_item.estimate_id,
                    'estimate_type': winner_item.estimate_type,
                    'public_key': winner_item.public_key,
                    'ss58_address': winner_item.ss58_address,
                    'reward': str(winner_item.reward),
                    'created_at': str(winner_item.created_at)
                }
                for winner_item in winner_list
            ]
        }
        return data
