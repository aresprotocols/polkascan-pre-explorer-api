from dogpile.cache import CacheRegion
from sqlalchemy import func
from sqlalchemy.orm import Session
from substrateinterface import SubstrateInterface
from sqlalchemy.orm import scoped_session, Session

from app import utils
from app.main import session_factory
from app.models.data import BlockTotal, Block, PriceRequest
from app.resources.base import create_substrate
from app.tasks.base import BaseTask
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, Session, sessionmaker


class ChainDataTask(BaseTask):
    session: 'Session'
    substrate: 'SubstrateInterface'

    def __init__(self, db_setting, is_debug=False):
        self.db_setting = db_setting
        self.is_debug = is_debug

    def before(self):
        print("Schedule call - ChainDataTask BEFORE")
        engine = create_engine(self.db_setting, echo=self.is_debug, isolation_level="READ_UNCOMMITTED", pool_pre_ping=True)
        session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        self.session = scoped_session(session_factory)
        self.substrate = create_substrate()

    # def before(self):
    #     print("Schedule call - ChainDataTask BEFORE")
    #     _scoped_session = scoped_session(session_factory)
    #     self.session = _scoped_session()
    #     self.substrate = create_substrate()

    def after(self):
        self.substrate.close()
        self.session.close()
        self.substrate = None
        self.session = None

    def post(self):
        print("Schedule call - ChainDataTask POST")
        block_total: BlockTotal = BlockTotal.query(self.session).filter_by(
            id=self.session.query(func.max(BlockTotal.id)).one()[0]).first()
        block: Block = Block.query(self.session).filter_by(id=block_total.id).first()
        block_hash = block.hash
        substrate = self.substrate
        # block_hash = substrate.get_chain_finalised_head()
        substrate.init_runtime(block_hash=block_hash)
        substrate.runtime_config.update_type_registry_types(
            {"EraIndex": "u32", "BalanceOf": "Balance", "ValidatorId": "AccountId"})

        finalized_block = substrate.get_block_number(block_hash)
        total_issuance = utils.query_storage(pallet_name="Balances", storage_name="TotalIssuance", substrate=substrate,
                                             block_hash=block_hash)
        if total_issuance is None:
            total_issuance = 0
        else:
            total_issuance = total_issuance.value

        total_validators = utils.query_storage(pallet_name='Session', storage_name='Validators',
                                               substrate=substrate,
                                               block_hash=block_hash)
        if total_validators is None:
            total_validators = 0
        else:
            total_validators = len(total_validators.value)

        active_era = utils.query_storage(pallet_name='Staking', storage_name='ActiveEra',
                                          substrate=substrate,
                                          block_hash=block_hash)

        print("KAMI-DEBUG, active_era.value['index'] = ", active_era.value['index'])
        total_stake = None
        if total_validators is not None:
            total_stake = utils.query_storage(pallet_name='Staking', storage_name='ErasTotalStake',
                                              substrate=substrate, block_hash=block_hash,
                                              params=[active_era.value['index']])

        if total_stake is not None:
            total_stake = total_stake.value
        else:
            total_stake = 0

        # calculate inflation
        # TODO read num_auctions from onchain data // const numAuctions = api.query.auctions ? auctionCounter : BN_ZERO
        num_auctions = 0

        # inflation_params
        auction_adjust = 0
        auction_max = 0
        falloff = 0.05
        max_inflation = 0.1
        min_inflation = 0.025
        stake_target = 0.5

        staked_fraction = 0 if total_issuance == 0 or total_issuance == 0 else total_stake * 1000000 / total_issuance / 1000000
        # staked_Fraction = total_issuance == 0 || totalIssuance.isZero()? 0 : totalStaked.mul(BN_MILLION).div(totalIssuance).toNumber() / BN_MILLION.toNumber();
        ideal_stake = stake_target - (min(auction_max, num_auctions) * auction_adjust)
        ideal_interest = max_inflation / ideal_stake
        if staked_fraction <= ideal_stake:
            tmp = staked_fraction * (ideal_interest - (min_inflation / ideal_stake))
        else:
            tmp = (ideal_interest * ideal_stake - min_inflation) * (2 ** ((ideal_stake - staked_fraction) / falloff))
        inflation = 100 * (min_inflation + tmp)

        # print("staked_fraction:{}".format(staked_fraction))
        # print("ideal_stake:{}".format(ideal_stake))
        # print("ideal_interest:{}".format(ideal_interest))
        # print("inflation:{}".format(inflation))
        # print("test:{}".format(2 ** ((ideal_stake - staked_fraction) / falloff)))

        # validator_keys = substrate.rpc_request('state_getKeys', [storage_key_prefix, block.hash]).get('result')
        # validators = [storage_key[-64:] for storage_key in rpc_result if len(storage_key) == 146]
        # total_validators = len(validator_keys)
        if block_total is None:
            block_total = BlockTotal()
            block_total.total_extrinsics_signed = 0
            block_total.total_events_transfer = 0
            block_total.total_accounts = 0
            block_total.total_treasury_burn = 0

        symbols = utils.query_storage(pallet_name='AresOracle', storage_name='PricesRequests',
                                      substrate=substrate,
                                      block_hash=block_hash)
        total_price_requests = self.session.query(func.count(PriceRequest.order_id)).scalar()

        resp = {
            'total_extrinsics_signed': int(block_total.total_extrinsics_signed),
            'total_events_transfer': int(block_total.total_events_transfer),
            'total_account': int(block_total.total_accounts),
            'total_treasury_burn': int(block_total.total_treasury_burn),
            'total_issuance': str(total_issuance),
            'finalized_block': finalized_block,
            'total_validators': int(total_validators),
            'total_stake': str(total_stake),
            'inflation': inflation,
            'total_symbols': len(symbols),
            'total_price_requests': total_price_requests,
            # 'remaining_rewards_of_purchase': 0 # not need.
        }
        self.cache_region().set("ares_chain_data", resp)
