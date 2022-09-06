from scalecodec.base import ScaleBytes
from scalecodec.types import GenericMetadataVersioned, GenericPalletMetadata, GenericStorageEntryMetadata
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException


def query(substrate: SubstrateInterface, pallet_name: str, storage_name: str, param_types: list, param_hashers: list,
          params: list, value_type: str, block_hash):
    # Encode parameters
    for idx, param in enumerate(params):
        # param = substrate.convert_storage_parameter(param_types[idx], param)
        if type(param) is bytes:
            param = f'0x{param.hex()}'
        param_obj = substrate.runtime_config.create_scale_object(type_string=param_types[idx])
        params[idx] = param_obj.encode(param)

    storage_hash = substrate.generate_storage_hash(
        storage_module=pallet_name,
        storage_function=storage_name,
        params=params,
        hashers=param_hashers
    )
    query_value = substrate.get_storage_by_key(block_hash, storage_hash)
    if query_value is None:
        return None
    scale_class = substrate.runtime_config.create_scale_object(value_type,
                                                               data=ScaleBytes(query_value))
    scale_class.decode()
    return scale_class


def query_storage(pallet_name: str, storage_name: str, substrate: SubstrateInterface, block_hash,
                  params: list = None):
    if params is None:
        params = []

    if substrate.metadata_decoder is None:
        metadata: GenericMetadataVersioned = substrate.get_block_metadata(block_hash)
        substrate.metadata_decoder = metadata
    else:
        metadata: GenericMetadataVersioned = substrate.metadata_decoder

    module: GenericPalletMetadata = metadata.get_metadata_pallet(pallet_name)
    storage_func: GenericStorageEntryMetadata = module.get_storage_function(storage_name)
    param_types = storage_func.get_params_type_string()
    value_type = storage_func.get_value_type_string()

    return query(substrate=substrate, pallet_name=module.value['storage']['prefix'], storage_name=storage_name,
                 param_types=param_types, param_hashers=storage_func.get_param_hashers(), params=params,
                 value_type=value_type, block_hash=block_hash)


def query_all_storage(pallet_name: str, storage_name: str, substrate: SubstrateInterface, block_hash) -> {}:
    module: GenericPalletMetadata = substrate.metadata_decoder.get_metadata_pallet(pallet_name)
    storage_func: GenericStorageEntryMetadata = module.get_storage_function(storage_name)
    param_types = storage_func.get_params_type_string()
    param_hashers = storage_func.get_param_hashers()
    value_type = storage_func.get_value_type_string()
    storage_key_prefix = substrate.generate_storage_hash(storage_module=pallet_name, storage_function=storage_name)
    keys = substrate.rpc_request("state_getKeys", [storage_key_prefix, block_hash]).get("result")
    response = substrate.rpc_request(method="state_queryStorageAt", params=[keys, block_hash])
    if 'error' in response:
        raise SubstrateRequestException(response['error']['message'])

    result = {}
    for result_group in response['result']:
        for item in result_group['changes']:
            item_value = substrate.runtime_config.create_scale_object(type_string=value_type,
                                                                      data=ScaleBytes(item[1]))
            # TODO support more hash type & double map
            a = bytes.fromhex(item[0].replace(storage_key_prefix, ""))[16:]  # ['Blake2_128Concat']
            item_key = substrate.runtime_config.create_scale_object(type_string=param_types[0], data=ScaleBytes(a))
            item_key.decode()
            item_value.decode()
            result[item_key.value] = item_value
    return result

def getChainDataValue(data) :
    if isinstance(data, (float, int, str, list, dict, tuple)) :
        return data
    else:
        return data.value