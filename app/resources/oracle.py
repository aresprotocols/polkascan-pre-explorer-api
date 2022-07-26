import falcon
from sqlalchemy.orm import load_only
from substrateinterface import SubstrateInterface

from app import utils
from app.models.data import SymbolSnapshot, PriceRequest, EraPriceRequest
from app.resources.base import JSONAPIDetailResource, JSONAPIListResource, create_substrate, JSONAPIResource
from app.settings import SUBSTRATE_ADDRESS_TYPE
from app.utils.ss58 import ss58_encode


class SymbolListResource(JSONAPIListResource):
    cache_expiration_time = 0

    def get_query(self):
        return self.cache_region.get("ares_symbols")

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
                [
                    price.block_id,
                    price.price,
                    price.fraction,
                    price.created_at.timestamp(),
                    [[ss58_encode(auth[0].replace('0x', ''), SUBSTRATE_ADDRESS_TYPE), auth[1]] for auth in price.auth]
                ]
                for price in symbol_prices
            ]
        }
        return data


class OracleRequestsReward(JSONAPIDetailResource):
    cache_expiration_time = 0

    def get_item(self, item_id):
        return self.cache_region.get("ares_request_reward")


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
        # tasks = utils.query_storage(pallet_name='AresOracle', storage_name='PreCheckTaskList',
        #                             substrate=substrate, block_hash=block_hash)
        result = []
        all_final_results = utils.query_all_storage(pallet_name='AresOracle', storage_name='FinalPerCheckResult',
                                                    substrate=substrate, block_hash=block_hash)
        for key in all_final_results:
            task_result = all_final_results[key].value
            obj = {
                "validator": ss58_encode(key.replace('0x', ''), SUBSTRATE_ADDRESS_TYPE),
                "ares_authority": task_result[3],
                "block_number": task_result[0],
                "status": task_result[1]
            }
            result.append(obj)
        # print("==========")
        # if tasks is not None:
        #     for (validator, ares_authority, block_number) in tasks.value:
        #         obj = {
        #             "validator": ss58_encode(validator.replace('0x', ''), SUBSTRATE_ADDRESS_TYPE),
        #             "ares_authority": ares_authority,
        #             "block_number": block_number,
        #             "status": None
        #         }
        #         if validator in all_final_results:
        #             obj['status'] = all_final_results[validator].value[1]
        #         result.append(obj)
        return result

    def process_get_response(self, req, resp, **kwargs):
        if self.substrate:
            self.substrate.close()
        return super().process_get_response(req, resp, **kwargs)


class OracleAresAuthorityResource(JSONAPIResource):
    cache_expiration_time = 3600
    substrate: SubstrateInterface = None

    def __init__(self, substrate: SubstrateInterface = None):
        self.substrate = substrate

    def process_get_response(self, req, resp, **kwargs):
        host_key_param = kwargs.get("key")
        authority_key = kwargs.get("auth")
        item = self.get_item(host_key_param, authority_key)
        response = {
            'status': falcon.HTTP_200,
            'media': self.get_jsonapi_response(
                data=self.serialize_item(item),
                relationships={},
                meta=self.get_meta()
            ),
            'cacheable': True
        }
        if self.substrate:
            self.substrate.close()
        return response
        # return super().process_get_response(req, resp, **kwargs)

    def get_item(self, host_key, authority_key):
        if self.substrate is None:
            self.substrate = create_substrate()
        substrate = self.substrate
        # block: Block = Block.query(self.session).filter_by(
        #     id=self.session.query(func.max(Block.id)).one()[0]).first()
        # block_hash = block.hash
        block_hash = substrate.get_chain_finalised_head()
        block_number = substrate.get_block_number(block_hash=block_hash)
        key = int(host_key, 0)
        substrate.init_runtime(block_hash=block_hash)

        # 查看对应的 search_authority 是否已经是验证人，如果是则不需要之后的判断
        validators = utils.query_storage(pallet_name='AresOracle', storage_name='Authorities',
                                         substrate=substrate, block_hash=block_hash)
        validators = validators.value
        if authority_key in validators:
            return "OK"

        x_ray = utils.query_storage(pallet_name='AresOracle', storage_name='LocalXRay',
                                    substrate=substrate, block_hash=block_hash, params=[key])

        # host_key exist
        if x_ray and len(x_ray.value) > 2:
            authority_keys = x_ray.value[2]
            created_at = x_ray.value[0]
            warehouse = x_ray.value[1]
            is_validator = x_ray.value[3]

            if authority_key not in authority_keys:
                return "Does not match on-chain setting"

            tasks = utils.query_storage(pallet_name='AresOracle', storage_name='PreCheckTaskList',
                                        substrate=substrate, block_hash=block_hash)

            session_length = 0
            for module_idx, module in enumerate(substrate.metadata_decoder.pallets):
                if 'Babe' == module.name and module.constants:
                    for constant in module.constants:
                        if 'epochDuration' == constant.value['name']:
                            session_length = constant
            session_length = 600

            match_data = None
            for task_data in tasks.value:
                if task_data[1] == authority_key:
                    match_data.stash_id = task_data[0]
                    match_data.authority_id = task_data[1]
                    match_data.submit_bn = task_data[2]

                    diff_bn = block_number - match_data.submit_bn

                    sessions_per_era = utils.query_storage(pallet_name='Staking', storage_name='SessionsPerEra',
                                                           substrate=substrate, block_hash=block_hash)
                    sessions_per_era = sessions_per_era.value
                    ear_length = sessions_per_era * session_length

                    cross_era = diff_bn / ear_length

                    pre_check_result = utils.query_storage(pallet_name='AresOracle', storage_name='FinalPerCheckResult',
                                                           substrate=substrate, block_hash=block_hash,
                                                           params=[match_data.stash_id])
                    pre_check_result = pre_check_result.value
                    if pre_check_result is None:

                        if cross_era < 2:
                            return "The set time did not exceed 1 era, please wait."
                        else:
                            return "Please check the warehouse configuration."
                    else:
                        check_result_status = pre_check_result[1]
                        if check_result_status == "Pass":
                            return "OK"
                        elif check_result_status == "Prohibit":
                            return "Please check the warehouse configuration"
                        else:
                            if cross_era < 2:
                                return "The set time did not exceed 1 era, please wait."
                            else:
                                return "Submit feedback to the project party."

        # if match_data == None:
        #     Exception("Impossible, but it can be checked")
        return "Unknown Exception"
