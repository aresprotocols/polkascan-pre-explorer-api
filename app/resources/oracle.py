import scalecodec.utils.ss58
from scalecodec import GenericPalletMetadata, GenericStorageEntryMetadata, ScaleBytes
from sqlalchemy import func
from sqlalchemy.orm import load_only
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
from sqlalchemy import and_, or_
from app import utils
from app.models.data import SymbolSnapshot, Block, PriceRequest, EraPriceRequest
from app.resources.base import JSONAPIDetailResource, JSONAPIListResource, create_substrate
from app.settings import SUBSTRATE_ADDRESS_TYPE


class SymbolListResource(JSONAPIListResource):
    cache_expiration_time = 3600
    substrate: SubstrateInterface = None

    def __init__(self, substrate: SubstrateInterface = None):
        self.substrate = substrate

    def get_query(self):
        if self.substrate is None:
            self.substrate = create_substrate()
        block: Block = Block.query(self.session).filter_by(
            id=self.session.query(func.max(Block.id)).one()[0]).first()
        substrate = self.substrate
        block_hash = block.hash
        # block_hash = substrate.get_chain_finalised_head()

        substrate.init_runtime(block_hash=block_hash)

        symbols = utils.query_storage(pallet_name='AresOracle', storage_name='PricesRequests',
                                      substrate=substrate,
                                      block_hash=block_hash)
        symbol_keys = [symbol.value[0] for symbol in symbols]

        # symbol_prices = self.session.query(SymbolSnapshot).filter(
        #     and_(User.group_id.__eq__(chat.input_entity.channel_id), User.deleted_at.__eq__(None),
        #          and_(User.created_at.__eq__(None), User.updated_at.__eq__(None)))).all()
        results = []
        for symbol in symbols:
            key = symbol.value[0]
            symbol_price: [] = self.session.query(SymbolSnapshot.symbol, SymbolSnapshot.price, SymbolSnapshot.block_id,
                                                  Block.datetime). \
                join(Block, Block.id == SymbolSnapshot.block_id). \
                filter(and_(SymbolSnapshot.symbol.__eq__(key))). \
                order_by(SymbolSnapshot.block_id.desc()).first()
            if symbol_price:
                results.append({
                    "symbol": key,
                    "precision": symbol.value[3],
                    "interval": symbol.value[4] * 6,
                    "price": symbol_price[1],
                    "block_id": symbol_price[2],
                    "created_at": symbol_price[3].strftime('%Y-%m-%d %H:%M:%S'),
                })
        substrate.close()
        return results

    def process_get_response(self, req, resp, **kwargs):
        if self.substrate:
            self.substrate.close()
        return super().process_get_response(req, resp, **kwargs)


class OracleRequestListResource(JSONAPIListResource):
    def get_query(self):
        price_requests: [PriceRequest] = self.session.query(PriceRequest).options(
            load_only("order_id", "created_by", "symbols", "status", "prepayment", "payment", "created_at",
                      "ended_at")).order_by(PriceRequest.created_at.desc())
        return price_requests


class OracleEraRequests(JSONAPIListResource):
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
    cache_expiration_time = 3600
    substrate: SubstrateInterface = None

    def __init__(self, substrate: SubstrateInterface = None):
        self.substrate = substrate

    def get_item(self, item_id):
        return self.cache_region.get("ares_request_reward", self.cache_expiration_time)

    def process_get_response(self, req, resp, **kwargs):
        if self.substrate:
            self.substrate.close()
        return super().process_get_response(req, resp, **kwargs)


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
                "validator": validator,
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
