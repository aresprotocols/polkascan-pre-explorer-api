import falcon
from sqlalchemy.orm import load_only
from substrateinterface import SubstrateInterface

from app import utils
from app.models.data import SymbolSnapshot, PriceRequest, EraPriceRequest, ValidatorAuditFromChain
from app.resources.base import JSONAPIDetailResource, JSONAPIListResource, create_substrate, JSONAPIResource, \
    JSONAPIDetailResourceFilterWithDb
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


class OracleDetailResource(JSONAPIDetailResourceFilterWithDb):
    cache_expiration_time = 0

    def get_item_url_name(self):
        return 'symbol'

    def get_item(self, item_id, offset, size_num):
        symbol_prices: [SymbolSnapshot] = SymbolSnapshot.query(self.session).filter_by(
            symbol=item_id).order_by(SymbolSnapshot.block_id.desc()).offset(offset).limit(size_num)[:size_num]

        data = {
            'name': 'Price',
            'type': 'line',
            'data': [
                [
                    price.block_id,
                    price.price,
                    price.fraction,
                    price.created_at.timestamp(),
                    [[ss58_encode(auth[0].replace('0x', ''), SUBSTRATE_ADDRESS_TYPE), auth[1]] if isinstance(auth, list)
                     else [ss58_encode(auth.replace('0x', ''), SUBSTRATE_ADDRESS_TYPE), 0] for auth in price.auth]
                ]
                for price in symbol_prices
            ]
        }
        return data


class OracleRequestsReward(JSONAPIListResource):
    cache_expiration_time = 0

    def get_meta(self):
        ares_request_reward = self.cache_region.get("ares_request_reward")
        return {'total_reward': ares_request_reward['total_reward']}

    def get_query(self):
        ares_request_reward = self.cache_region.get("ares_request_reward")
        return ares_request_reward['data']


class OraclePreCheckTaskListResource(JSONAPIListResource):
    cache_expiration_time = 300
    substrate: SubstrateInterface = None
    audit_list = []

    def __init__(self, substrate: SubstrateInterface = None):
        self.substrate = substrate

    def get_meta(self):
        # ares_request_reward = self.cache_region.get("ares_request_reward")
        return {'total_audit_count': len(self.audit_list)}

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

        # id = sa.Column(sa.Integer(), primary_key=True)
        # validator = sa.Column(sa.String(100), nullable=False)
        # ares_authority = sa.Column(sa.String(100), nullable=False)
        # block_number = sa.Column(sa.Integer(), nullable=False)
        # status = sa.Column(sa.String(20), nullable=False)

        # Get db data
        validatorAuditDbList: [ValidatorAuditFromChain] = ValidatorAuditFromChain.query(
            self.session
        ).order_by(
            ValidatorAuditFromChain.block_number.desc()
        ).limit(5000)[:5000]

        self.audit_list = []

        for validatorAuditObj in validatorAuditDbList:
            obj = {
                "validator": validatorAuditObj.validator,
                "ares_authority": validatorAuditObj.ares_authority,
                "block_number": validatorAuditObj.block_number,
                "status": validatorAuditObj.status
            }
            self.audit_list.append(obj)


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

            is_found = False
            for idx, val in enumerate(self.audit_list):
                if val['validator'] == obj['validator'] and val['ares_authority'] == obj['ares_authority'] :
                    # update new data
                    is_found = True
                    self.audit_list[idx]["block_number"] = obj['block_number']
                    self.audit_list[idx]["status"] = obj['status']

            if not is_found:
                # To add new
                self.audit_list.append(obj)

        return self.audit_list

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
            # 获取session长度
            # TODO read from constant(babe::epochDuration)
            session_length = 0
            for module_idx, module in enumerate(substrate.metadata_decoder.pallets):
                if 'Babe' == module.name and module.constants:
                    for constant in module.constants:
                        if 'epochDuration' == constant.value['name']:
                            session_length = constant
            session_length = 600

            # 尝试获取对应匹配的数据
            match_data = None
            for task_data in tasks.value:
                # task_data 存储每一条任务记录，查找对应authority的数据 [0]=StashId,[1]=AuthorityId,[2]任务提交区块
                if task_data[1] == authority_key:
                    match_data.stash_id = task_data[0]
                    match_data.authority_id = task_data[1]
                    match_data.submit_bn = task_data[2]
                    # 获取链上最后的区块
                    # 获取任务提交到当前区块的时间
                    diff_bn = block_number - match_data.submit_bn
                    # 获取era长度
                    sessions_per_era = utils.query_storage(pallet_name='Staking', storage_name='SessionsPerEra',
                                                           substrate=substrate, block_hash=block_hash)
                    sessions_per_era = sessions_per_era.value
                    ear_length = sessions_per_era * session_length
                    # 计算从任务提交到当前位置跨越了几个era
                    cross_era = diff_bn / ear_length
                    # 获取检查结果数据
                    pre_check_result = utils.query_storage(pallet_name='AresOracle', storage_name='FinalPerCheckResult',
                                                           substrate=substrate, block_hash=block_hash,
                                                           params=[match_data.stash_id])
                    pre_check_result = pre_check_result.value
                    if pre_check_result is None:
                        # 如果没有找到结果
                        if cross_era < 2:
                            # 可能还没有到提交的区块，需要稍等一下。
                            return "The set time did not exceed 1 era, please wait."
                        else:
                            # 这种情况是因为该ares-authority没能成功发起offchain请求，原因不明。
                            return "Please check the warehouse configuration."
                    else:
                        # 提取数据
                        check_result_status = pre_check_result[1]
                        # 比对结果
                        if check_result_status == "Pass":
                            return "OK"
                        elif check_result_status == "Prohibit":
                            # 这种情况也可以给出一个新的提示“请检查 werahouse配置”
                            return "Please check the warehouse configuration"
                        else:
                            # 这种有结果的，判断返回值的类型
                            if cross_era < 2:
                                # 这种情况需要等待下一次选举后才能让其成为验证人。
                                return "The set time did not exceed 1 era, please wait."
                            else:
                                return "Submit feedback to the project party."

        # if match_data == None:
        #     Exception("Impossible, but it can be checked")
        return "Unknown Exception"

    # 参数
    # host_key = web.request.host_key
    # search_authority = web.request.search_authority
    # def get_data(self, host_key, search_authority):
    #
    #     # 通过 host_key 获取查询人对应本地的Ares-authorities列表。
    #     xray_data = chain.aresOracle.localXRay.get(host_key)
    #
    #     # 数据提交的区块。
    #     _xray_submit_bn = xray_data[0]
    #     # 数据提交的 weavehouse值。
    #     _xray_submit_werahouse = xray_data[1]
    #     # 这是一个数组，存储xray-key对应的所有本地验证人。
    #     xray_authorities = xray_data[2]
    #
    #     # 检查链上验证人是否与本地用户数据匹配
    #
    #     if False == xray_authorities.include(search_authority):
    #         # 表示第一种错误
    #         return "Does not match on - chain settings"
    #
    #     # 获取预检查任务列表
    #     pre_check_task_list = chain.aresOracle.preCheckTaskList()
    #     # 尝试获取对应匹配的数据
    #     match_data = None
    #     for task_data in pre_check_task_list:
    #         # task_data 存储每一条任务记录，查找对应authority的数据 [0]=StashId,[1]=AuthorityId,[2]任务提交区块
    #         if task_data[1] == search_authority:
    #             match_data.stash_id = task_data[0]
    #             match_data.authority_id = task_data[1]
    #             match_data.submit_bn = task_data[2]
    #             break  # 退出查找
    #
    #     if match_data == None:
    #         Exception("Impossible, but it can be checked")
    #
    #     # 获取链上最后的区块
    #     lastest_bn = chain.rpc.chain.getHeader()
    #
    #     # 获取任务提交到当前区块的时间
    #     diff_bn = lastest_bn - match_data.submit_bn
    #
    #     # 获取session长度
    #     session_length = 600
    #
    #     # 获取era长度
    #     ear_length = chain.staking.sessionsPerEra() * session_length
    #
    #     # 计算从任务提交到当前位置跨越了几个era
    #     cross_era = diff_bn / ear_length
    #
    #     # 获取检查结果数据
    #     pre_check_result = chain.resOracle.finalPerCheckResult(match_data.stash_id)
    #
    #     if pre_check_result == None:
    #         # 如果没有找到结果
    #         if cross_era < 2:
    #             # 可能还没有到提交的区块，需要稍等一下。
    #             return "The set time did not exceed 1 era, please wait."
    #         else:
    #             # 这种情况是因为该ares-authority没能成功发起offchain请求，原因不明。
    #             return "Submit feedback to the project party."
    #     else:
    #         # 提取数据
    #         check_result_createbn = pre_check_result[0]
    #         check_result_status = pre_check_result[1]
    #         # 比对结果
    #         if check_result_status == "Prohibit":
    #             # 这种情况也可以给出一个新的提示“请检查 werahouse配置”
    #             return "Please check the werahouse configuration"
    #         else:
    #             # 这种有结果的，判断返回值的类型
    #             if cross_era < 2:
    #                 # 这种情况需要等待下一次选举后才能让其成为验证人。
    #                 return "The set time did not exceed 1 era, please wait."
    #             else:
    #                 return "Submit feedback to the project party."
