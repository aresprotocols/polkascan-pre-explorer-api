#  Polkascan PRE Explorer API
#
#  Copyright 2018-2020 openAware BV (NL).
#  This file is part of Polkascan.
#
#  Polkascan is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Polkascan is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Polkascan. If not, see <http://www.gnu.org/licenses/>.
#
#  main.py

import logging

import falcon
from dogpile.cache import make_region
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.middleware.cache import CacheMiddleware
from app.middleware.context import ContextMiddleware
from app.middleware.sessionmanager import SQLAlchemySessionManager
from app.resources import polkascan, charts, oracle, estimates
from app.settings import DB_CONNECTION, DEBUG, DOGPILE_CACHE_SETTINGS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Database connection
engine = create_engine(DB_CONNECTION, echo=DEBUG, isolation_level="READ_UNCOMMITTED", pool_pre_ping=True)
session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Define cache region
cache_region = make_region().configure(
    'dogpile.cache.redis',
    arguments={
        'host': DOGPILE_CACHE_SETTINGS['host'],
        'port': DOGPILE_CACHE_SETTINGS['port'],
        'db': DOGPILE_CACHE_SETTINGS['db'],
        'redis_expiration_time': 60 * 60 * 6,  # 6 hours
        'distributed_lock': True
    }
)

# Define application
app = falcon.API(middleware=[
    ContextMiddleware(),
    SQLAlchemySessionManager(session_factory),
    CacheMiddleware(cache_region)
])
# substrate = SubstrateInterface(url=settings.SUBSTRATE_RPC_URL, type_registry_preset=settings.TYPE_REGISTRY)
# Application routes
### Created by aresprotocal
app.add_route('/charts', charts.ExtrinsicSigned())
app.add_route('/chain', polkascan.ChainDataResource())
app.add_route('/chain/latest', polkascan.LatestBlockResource())
app.add_route('/oracle/symbols', oracle.SymbolListResource())
app.add_route('/oracle/symbol/{symbol}', oracle.OracleDetailResource())
app.add_route('/oracle/requests', oracle.OracleRequestListResource())
app.add_route('/oracle/era_requests', oracle.OracleEraRequests())
app.add_route('/oracle/pre_check_tasks', oracle.OraclePreCheckTaskListResource())
app.add_route('/oracle/ares/author/{key}/{auth}', oracle.OracleAresAuthorityResource())
app.add_route('/oracle/reward', oracle.OracleRequestsReward())
app.add_route('/estimate/statistics/{symbol}/{id}', estimates.StatisticsEstimate())

app.add_route('/block', polkascan.BlockListResource())
app.add_route('/block/{block_id}', polkascan.BlockDetailsResource())
app.add_route('/block-total', polkascan.BlockTotalListResource())
app.add_route('/block-total/{item_id}', polkascan.BlockTotalDetailsResource())
app.add_route('/extrinsic', polkascan.ExtrinsicListResource())
app.add_route('/extrinsic/{extrinsic_id}', polkascan.ExtrinsicDetailResource())
app.add_route('/event', polkascan.EventsListResource())
app.add_route('/event/{event_id}', polkascan.EventDetailResource())
app.add_route('/runtime', polkascan.RuntimeListResource())
app.add_route('/runtime/{item_id}', polkascan.RuntimeDetailResource())
app.add_route('/runtime-call', polkascan.RuntimeCallListResource())
app.add_route('/runtime-call/{runtime_call_id}', polkascan.RuntimeCallDetailResource())
app.add_route('/runtime-event', polkascan.RuntimeEventListResource())
app.add_route('/runtime-event/{runtime_event_id}', polkascan.RuntimeEventDetailResource())
app.add_route('/runtime-module', polkascan.RuntimeModuleListResource())
app.add_route('/runtime-module/{item_id}', polkascan.RuntimeModuleDetailResource())
app.add_route('/runtime-storage/{item_id}', polkascan.RuntimeStorageDetailResource())
app.add_route('/runtime-constant', polkascan.RuntimeConstantListResource())
app.add_route('/runtime-constant/{item_id}', polkascan.RuntimeConstantDetailResource())
app.add_route('/runtime-type', polkascan.RuntimeTypeListResource())
app.add_route('/networkstats/{network_id}', polkascan.NetworkStatisticsResource())
app.add_route('/balances/transfer', polkascan.BalanceTransferListResource())
app.add_route('/balances/transfer/{item_id}', polkascan.BalanceTransferDetailResource())
app.add_route('/account', polkascan.AccountResource())
app.add_route('/account/{item_id}', polkascan.AccountDetailResource())
app.add_route('/accountindex', polkascan.AccountIndexListResource())
app.add_route('/accountindex/{item_id}', polkascan.AccountIndexDetailResource())
app.add_route('/log', polkascan.LogListResource())
app.add_route('/log/{item_id}', polkascan.LogDetailResource())
app.add_route('/session/session', polkascan.SessionListResource())
app.add_route('/session/session/{item_id}', polkascan.SessionDetailResource())
app.add_route('/session/validator', polkascan.SessionValidatorListResource())
app.add_route('/session/nominator', polkascan.SessionNominatorListResource())
app.add_route('/session/validator/{item_id}', polkascan.SessionValidatorDetailResource())
app.add_route('/contract/contract', polkascan.ContractListResource())
app.add_route('/contract/contract/{item_id}', polkascan.ContractDetailResource())
