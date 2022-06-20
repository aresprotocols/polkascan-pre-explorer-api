import falcon

from app.models.data import EstimatesParticipants
from app.resources.base import JSONAPIResource


class StatisticsEstimate(JSONAPIResource):
    cache_expiration_time = 0

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
        results = EstimatesParticipants.query(self.session).filter_by(symbol=symbol, estimate_id=estimate_id).all()
        items = [row for row in results]
        items.sort(key=lambda r: r.price)
        sum_price = 0
        for item in items:
            sum_price += item.price
        return {"total": len(items), "avg": str(sum_price / len(items)), "median": str(items[len(items) // 2].price)}
