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
    cache_expiration_time = 60
    substrate: SubstrateInterface = None

    # def __init__(self, substrate: SubstrateInterface):
    #     self.substrate = substrate

    def get_query(self):
        substrate = create_substrate()
        self.substrate = substrate

        block: Block = Block.query(self.session).filter_by(
            id=self.session.query(func.max(Block.id)).one()[0]).first()
        substrate = self.substrate
        block_hash = block.hash
        # block_hash = substrate.get_chain_finalised_head()

        substrate.init_runtime(block_hash=block_hash)

        # symbol_prices: [SymbolSnapshot] = SymbolSnapshot.query(self.session).filter_by(
        #     symbol=item_id).order_by(SymbolSnapshot.block_id.desc()).limit(1000)[:1000]
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

            results.append({
                "symbol": key,
                "precision": symbol.value[3],
                "interval": symbol.value[4] * 6,
                "price": symbol_price[1],
                "block_id": symbol_price[2],
                "created_at": symbol_price[3].strftime('%Y-%m-%d %H:%M:%S'),
            })
            # break
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
    cache_expiration_time = 60
    substrate: SubstrateInterface = None

    # def __init__(self, substrate: SubstrateInterface):
    #     self.substrate = substrate

    def get_item(self, item_id):
        substrate = create_substrate()
        self.substrate = substrate
        block_hash = substrate.get_chain_finalised_head()

        storage_key_prefix = substrate.generate_storage_hash(storage_module="OracleFinance",
                                                             storage_function="RewardEra")  # 未领取
        rpc_result = substrate.rpc_request("state_getKeys", [storage_key_prefix, block_hash]).get("result")

        substrate.init_runtime(block_hash=block_hash)
        accounts = [storage_key[-64:] for storage_key in rpc_result if len(storage_key) == 162]
        eras = {}
        for account in accounts:
            account = "0x{}".format(account)
            rewards = utils.query_storage(pallet_name="OracleFinance", storage_name="RewardEra",
                                          substrate=substrate, block_hash=block_hash, params=[account])
            for reward in rewards:
                era = str(reward[0])
                if era in eras:
                    era_reward = eras[era]
                    if account in era_reward:
                        era_reward[account] += reward[1].value
                    else:
                        era_reward[account] = reward[1].value
                else:
                    # TODO remove hardcode type_string & hashers
                    era_key = substrate.runtime_config.create_scale_object(type_string="U32")
                    era_key.encode(int(era))
                    storage_key_prefix = substrate.generate_storage_hash(storage_module="OracleFinance",
                                                                         storage_function="AskEraPoint",
                                                                         params=[era_key],
                                                                         hashers=["Blake2_128Concat"])
                    keys = substrate.rpc_request("state_getKeys", [storage_key_prefix, block_hash]).get(
                        "result")
                    points = substrate.rpc_request("state_queryStorageAt", [keys, block_hash]).get("result")
                    total_points = 0
                    for point in points[0]['changes']:
                        item_value = substrate.runtime_config.create_scale_object(type_string='U32',
                                                                                  data=ScaleBytes(point[1]))
                        item_value.decode()
                        total_points += item_value.value
                    eras[era] = {account: reward[1].value, 'total_points': total_points}

        module: GenericPalletMetadata = substrate.metadata_decoder.get_metadata_pallet("OracleFinance")
        storage_func: GenericStorageEntryMetadata = module.get_storage_function("AskEraPayment")
        param_types = storage_func.get_params_type_string()
        param_hashers = storage_func.get_param_hashers()
        value_type = storage_func.get_value_type_string()

        results = []
        total_reward = 0
        for era in eras:
            era_total_points = eras[era]['total_points']
            era_total_reward = 0
            era_key = substrate.runtime_config.create_scale_object(type_string=param_types[0])
            era_key.encode(int(era))
            storage_key_prefix = substrate.generate_storage_hash(storage_module="OracleFinance",
                                                                 storage_function="AskEraPayment",
                                                                 params=[era_key],
                                                                 hashers=param_hashers[0:1])
            keys = substrate.rpc_request("state_getKeys", [storage_key_prefix, block_hash]).get("result")
            response = substrate.rpc_request(method="state_queryStorageAt", params=[keys, block_hash])
            if 'error' in response:
                raise SubstrateRequestException(response['error']['message'])

            for result_group in response['result']:
                for item in result_group['changes']:
                    item_value = substrate.runtime_config.create_scale_object(type_string=value_type,
                                                                              data=ScaleBytes(item[1]))
                    item_value.decode()
                    era_total_reward += item_value.value

            total_reward += era_total_reward
            for account in eras[era]:
                if 'total_points' == account: continue

                results.append({
                    'era': era,
                    'account': scalecodec.ss58_encode(account.replace('0x', ''), SUBSTRATE_ADDRESS_TYPE),
                    'reward': eras[era][account] / era_total_points * era_total_reward,
                })

        # storage_key_prefix = substrate.generate_storage_hash(storage_module="OracleFinance",
        #                                                      storage_function="RewardTrace")
        # keys = substrate.rpc_request("state_getKeys", [storage_key_prefix, block_hash]).get("result")
        # module: GenericPalletMetadata = substrate.metadata_decoder.get_metadata_pallet("OracleFinance")
        # storage_func: GenericStorageEntryMetadata = module.get_storage_function("RewardTrace")
        # param_types = storage_func.get_params_type_string()
        # param_hashers = storage_func.get_param_hashers()
        #
        # def claim_exist(era: int, account: str, keys: [str]):
        #     era_key = substrate.runtime_config.create_scale_object(type_string=param_types[0])
        #     account_key = substrate.runtime_config.create_scale_object(type_string=param_types[1])
        #     era_key.encode(era)
        #     account_key.encode(account)
        #     hash_key = substrate.generate_storage_hash(storage_module="OracleFinance",
        #                                                storage_function="RewardTrace",
        #                                                params=[era_key, account_key],
        #                                                hashers=param_hashers)
        #     return hash_key in keys
        #
        # for result in results:
        #     # print(result)
        #     era = result['era']
        #     account = result['account']
        #     print(era, account)
        #     print(claim_exist(int(era), account, keys))
        # result['account'] = scalecodec.ss58_encode(account.replace('0x', ''), 34)
        results.sort(key=lambda r: (r['era'], r['account']))
        return {"total_reward": total_reward, "data": results}

    def process_get_response(self, req, resp, **kwargs):
        if self.substrate:
            self.substrate.close()
        return super().process_get_response(req, resp, **kwargs)
