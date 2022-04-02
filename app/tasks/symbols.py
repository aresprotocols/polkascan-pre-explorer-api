from dogpile.cache import CacheRegion
from sqlalchemy import and_
from sqlalchemy.orm import scoped_session, Session
from substrateinterface import SubstrateInterface

from app import utils
from app.main import session_factory
from app.models.data import SymbolSnapshot, Block
from app.resources.base import create_substrate
from app.tasks.base import BaseTask


class SymbolsPriceTask(BaseTask):
    substrate: 'SubstrateInterface'
    session: 'Session'
    cache_region: CacheRegion

    def __init__(self, cache_region: CacheRegion):
        self.cache_region = cache_region

    def before(self):
        self.substrate = create_substrate()
        _scoped_session = scoped_session(session_factory)
        self.session = _scoped_session()

    def after(self):
        self.substrate.close()
        self.session.close()

    def post(self):
        substrate = self.substrate
        block_hash = substrate.get_chain_finalised_head()
        substrate.init_runtime(block_hash=block_hash)

        symbols = utils.query_storage(pallet_name='AresOracle', storage_name='PricesRequests',
                                      substrate=substrate,
                                      block_hash=block_hash)
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
        self.cache_region.set("ares_symbols", results)
