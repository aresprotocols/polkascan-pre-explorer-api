from dogpile.cache import CacheRegion
from sqlalchemy import and_
from sqlalchemy.orm import scoped_session, Session, load_only
from substrateinterface import SubstrateInterface

from app import utils
from app.models.data import SymbolSnapshot, Block
from app.resources.base import create_substrate
from app.settings import SUBSTRATE_ADDRESS_TYPE
from app.tasks.base import BaseTask
from app.utils.ss58 import ss58_encode
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, Session, sessionmaker


class SymbolsPriceTask(BaseTask):
    substrate: 'SubstrateInterface'
    session: 'Session'

    def __init__(self, db_setting, is_debug=False):
        self.db_setting = db_setting
        self.is_debug = is_debug

    def before(self):
        print("Schedule call - SymbolsPriceTask BEFORE")
        engine = create_engine(self.db_setting, echo=self.is_debug, isolation_level="READ_UNCOMMITTED", pool_pre_ping=True)
        session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        self.session = scoped_session(session_factory)
        self.substrate = create_substrate()

    # def before(self):
    #     # print("RUN 1")
    #     self.substrate = create_substrate()
    #     _scoped_session = scoped_session(session_factory)
    #     self.session = _scoped_session()

    def after(self):
        self.substrate.close()
        self.session.close()
        self.session = None
        self.substrate = None

    def post(self):
        print("Schedule call - SymbolsPriceTask POST")
        substrate = self.substrate
        block_hash = substrate.get_chain_finalised_head()
        substrate.init_runtime(block_hash=block_hash)

        symbols = utils.query_storage(pallet_name='AresOracle', storage_name='PricesRequests',
                                      substrate=substrate,
                                      block_hash=block_hash)
        results = []
        for symbol in symbols:
            key = symbol.value[0]
            symbol_prices: [] = self.session.query(SymbolSnapshot.symbol, SymbolSnapshot.price, SymbolSnapshot.block_id,
                                                   Block.datetime, SymbolSnapshot.auth). \
                join(Block, Block.id == SymbolSnapshot.block_id). \
                filter(and_(SymbolSnapshot.symbol.__eq__(key))). \
                order_by(SymbolSnapshot.block_id.desc()).limit(2).all()
            if len(symbol_prices) == 0:
                continue

            # print('KAMI DEBUG - symbol_prices[0][4]] = ', symbol_prices[0][4])

            # Add block height to auth attribute.
            auths = [[ss58_encode(auth_items[0].replace('0x', ''), SUBSTRATE_ADDRESS_TYPE), auth_items[1]]
                     if isinstance(auth_items, list) else [ss58_encode(auth_items.replace('0x', ''), SUBSTRATE_ADDRESS_TYPE), 0]
                     for auth_items in symbol_prices[0][4]]

            # auths = [[ss58_encode(auth[0].replace('0x', ''), SUBSTRATE_ADDRESS_TYPE), auth[1]] for auth in symbol_prices[0][4]]
            if symbol_prices:
                if len(symbol_prices) > 1:
                    results.append({
                        "symbol": key,
                        "precision": symbol.value[3],
                        "interval": (symbol_prices[0][3] - symbol_prices[1][3]).total_seconds(),
                        "price": symbol_prices[0][1],
                        "block_id": symbol_prices[0][2],
                        "created_at": symbol_prices[0][3].strftime('%Y-%m-%d %H:%M:%S'),
                        "auth": auths
                    })
                else:
                    results.append({
                        "symbol": key,
                        "precision": symbol.value[3],
                        "interval": None,
                        "price": symbol_prices[0][1],
                        "block_id": symbol_prices[0][2],
                        "created_at": symbol_prices[0][3].strftime('%Y-%m-%d %H:%M:%S'),
                        "auth": auths
                    })
        results.sort(key=lambda r: r['interval'])
        self.cache_region().set("ares_symbols", results)
