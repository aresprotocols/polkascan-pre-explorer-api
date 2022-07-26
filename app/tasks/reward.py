import scalecodec
from scalecodec import ScaleBytes, GenericPalletMetadata, GenericStorageEntryMetadata
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

from app import utils
from app.resources.base import create_substrate
from app.settings import SUBSTRATE_ADDRESS_TYPE
from app.tasks.base import BaseTask


class RequestRewardTask(BaseTask):
    substrate: 'SubstrateInterface'

    def before(self):
        self.substrate = create_substrate()

    def after(self):
        # print("RUN 3")
        if self.substrate:
            self.substrate.close()
        if self.session:
            self.session.close()
        self.session = None
        self.substrate = None

    def post(self):
        substrate = self.substrate
        block_hash = substrate.get_chain_finalised_head()

        storage_key_prefix = substrate.generate_storage_hash(storage_module="OracleFinance",
                                                             storage_function="RewardEra")

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
                    print(f"request AskEraPoint {storage_key_prefix} {block_hash}")
                    keys = substrate.rpc_request("state_getKeys", [storage_key_prefix, block_hash]).get("result")
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

        results.sort(key=lambda r: (r['era'], r['account']))
        self.cache_region().set("ares_request_reward", {"total_reward": total_reward, "data": results})
