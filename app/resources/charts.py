from app.resources.base import JSONAPIDetailResource


class ExtrinsicSigned(JSONAPIDetailResource):
    cache_expiration_time = 3600 * 5

    def get_item(self, item_id):
        return self.cache_region.get("ares_charts", self.cache_expiration_time)
