from sqlalchemy.orm import load_only
from substrateinterface import SubstrateInterface

from app import utils
from app.models.data import SymbolSnapshot, PriceRequest, EraPriceRequest
from app.resources.base import JSONAPIDetailResource, JSONAPIListResource, create_substrate
from app.settings import SUBSTRATE_ADDRESS_TYPE
from app.utils.ss58 import ss58_encode


class SymbolListResource(JSONAPIListResource):
    cache_expiration_time = 3600 + 300

    def get_query(self):
        return self.cache_region.get("ares_symbols", self.cache_expiration_time)


class OracleRequestListResource(JSONAPIListResource):
    def get_query(self):
        price_requests: [PriceRequest] = self.session.query(PriceRequest).options(
            load_only("order_id", "created_by", "symbols", "status", "prepayment", "payment", "created_at",
                      "ended_at")).order_by(PriceRequest.created_at.desc())
        return price_requests


class OracleEraRequests(JSONAPIListResource):
    cache_region = 3600

    def get_query(self):
        era_price_requests: [EraPriceRequest] = self.session.query(EraPriceRequest).options().order_by(
            EraPriceRequest.era.desc())
        return era_price_requests


class OracleDetailResource(JSONAPIDetailResource):
    cache_expiration_time = 0

    def get_item_url_name(self):
        return 'symbol'

    def get_item(self, item_id):
        symbol_prices: [SymbolSnapshot] = SymbolSnapshot.query(self.session).filter_by(
            symbol=item_id).order_by(SymbolSnapshot.block_id.desc()).limit(1000)[:1000]
        data = {
            'name': 'Price',
            'type': 'line',
            'data': [
                # TODO fix price
                [price.block_id, price.price]
                for price in symbol_prices
            ]
        }
        return data


class OracleRequestsReward(JSONAPIDetailResource):
    cache_expiration_time = 3600 + 300

    def get_item(self, item_id):
        return self.cache_region.get("ares_request_reward", self.cache_expiration_time)


class OraclePreCheckTaskListResource(JSONAPIListResource):
    cache_expiration_time = 300
    substrate: SubstrateInterface = None

    def __init__(self, substrate: SubstrateInterface = None):
        self.substrate = substrate

    def get_query(self):
        if self.substrate is None:
            self.substrate = create_substrate()
        substrate = self.substrate
        # block: Block = Block.query(self.session).filter_by(
        #     id=self.session.query(func.max(Block.id)).one()[0]).first()
        # block_hash = block.hash
        block_hash = substrate.get_chain_finalised_head()

        substrate.init_runtime(block_hash=block_hash)
        tasks = utils.query_storage(pallet_name='AresOracle', storage_name='PreCheckTaskList',
                                    substrate=substrate, block_hash=block_hash)
        result = []
        all_final_results = utils.query_all_storage(pallet_name='AresOracle', storage_name='FinalPerCheckResult',
                                                    substrate=substrate, block_hash=block_hash)

        for (validator, ares_authority, block_number) in tasks.value:
            obj = {
                "validator": ss58_encode(validator.replace('0x', ''), SUBSTRATE_ADDRESS_TYPE),
                "ares_authority": ares_authority,
                "block_number": block_number,
                "status": None
            }
            if validator in all_final_results:
                obj['status'] = all_final_results[validator].value[1]
            result.append(obj)
        return result

    def process_get_response(self, req, resp, **kwargs):
        if self.substrate:
            self.substrate.close()
        return super().process_get_response(req, resp, **kwargs)
