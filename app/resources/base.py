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
#  base.py
from abc import ABC, abstractmethod

import falcon
from dogpile.cache import CacheRegion
from dogpile.cache.api import NO_VALUE
from sqlalchemy.orm import Session
from substrateinterface import SubstrateInterface

from app import settings, resources
from app.settings import MAX_RESOURCE_PAGE_SIZE, DOGPILE_CACHE_SETTINGS

metadata_store = {}


class BaseResource(object):
    session: Session
    cache_region: CacheRegion


class JSONAPIResource(BaseResource):
    cache_expiration_time = None

    def apply_filters(self, query, params):
        return query

    def get_meta(self):
        return {}

    def serialize_item(self, item):
        if hasattr(item, 'serialize'):
            return item.serialize()
        else:
            return item

    def process_get_response(self, req, resp, **kwargs):
        return {
            'status': falcon.HTTP_200,
            'media': self.get_jsonapi_response(data=None),
            'cacheable': False
        }

    def get_jsonapi_response(self, data, meta=None, errors=None, links=None, relationships=None, included=None):

        result = {
            'meta': {
                "authors": [
                    "WEB3SCAN",
                    "POLKASCAN",
                    "openAware BV"
                ]
            },
            'errors': [],
            "data": data,
            "links": {}
        }

        if meta:
            result['meta'].update(meta)

        if errors:
            result['errors'] = errors

        if links:
            result['links'] = links

        if included:
            result['included'] = included

        if relationships:
            result['data']['relationships'] = {}

            if 'included' not in result:
                result['included'] = []

            for key, objects in relationships.items():
                result['data']['relationships'][key] = {
                    'data': [{'type': obj.serialize_type, 'id': obj.serialize_id()} for obj in objects]}
                result['included'] += [obj.serialize() for obj in objects]

        return result

    def on_get(self, req, resp, **kwargs):

        cache_key = '{}-{}'.format(req.method, req.url)

        if self.cache_expiration_time:
            # Try to retrieve request from cache
            cache_response = self.cache_region.get(cache_key, self.cache_expiration_time)

            if cache_response is not NO_VALUE:
                resp.set_header('X-Cache', 'HIT')

            else:
                # Process request
                cache_response = self.process_get_response(req, resp, **kwargs)

                if cache_response.get('cacheable'):
                    # Store result in cache
                    self.cache_region.set(cache_key, cache_response)
                    resp.set_header('X-Cache', 'MISS')
        else:
            cache_response = self.process_get_response(req, resp, **kwargs)

        resp.status = cache_response.get('status')
        resp.media = cache_response.get('media')


class JSONAPIListResource(JSONAPIResource, ABC):
    cache_expiration_time = DOGPILE_CACHE_SETTINGS['default_list_cache_expiration_time']

    def get_included_items(self, items):
        return []

    @abstractmethod
    def get_query(self):
        raise NotImplementedError()

    def apply_paging(self, query, params):
        page = int(params.get('page[number]', 1)) - 1
        page_size = min(int(params.get('page[size]', 25)), MAX_RESOURCE_PAGE_SIZE)
        return query[page * page_size: page * page_size + page_size]

    def serialize_items(self, items):
        return [self.serialize_item(item) for item in items]

    def process_get_response(self, req, resp, **kwargs):
        items = self.get_query()
        items = self.apply_filters(items, req.params)
        items = self.apply_paging(items, req.params)

        return {
            'status': falcon.HTTP_200,
            'media': self.get_jsonapi_response(
                data=self.serialize_items(items),
                meta=self.get_meta(),
                included=self.get_included_items(items)
            ),
            'cacheable': True
        }


def make_response(resource, item, req):
    if not item:
        response = {
            'status': falcon.HTTP_404,
            'media': None,
            'cacheable': False
        }
    else:
        response = {
            'status': falcon.HTTP_200,
            'media': resource.get_jsonapi_response(
                data=resource.serialize_item(item),
                relationships=resource.get_relationships(req.params.get('include', []), item),
                meta=resource.get_meta()
            ),
            'cacheable': True
        }
    return response


class JSONAPIDetailResource(JSONAPIResource, ABC):
    cache_expiration_time = DOGPILE_CACHE_SETTINGS['default_detail_cache_expiration_time']

    def get_item_url_name(self):
        return 'item_id'

    @abstractmethod
    def get_item(self, item_id):
        raise NotImplementedError()

    def get_relationships(self, include_list, item):
        return {}

    def process_get_response(self, req, resp, **kwargs):
        item = self.get_item(kwargs.get(self.get_item_url_name()))
        return make_response(self, item, req)


class JSONAPIDetailResourceFilterWithDb(JSONAPIResource, ABC):
    cache_expiration_time = DOGPILE_CACHE_SETTINGS['default_detail_cache_expiration_time']

    filter_list = {}

    def __init__(self, filter_list={}):
        self.filter_list = filter_list

    def get_filter_list(self):
        return self.filter_list

    def get_item_url_name(self):
        return 'item_id'

    @abstractmethod
    def get_item(self, item_id, offset, size_num):
        raise NotImplementedError()

    def get_relationships(self, include_list, item):
        return {}

    def process_get_response(self, req, resp, **kwargs):
        params = req.params
        start_num = max(int(params.get('page[number]', 1)) - 1, 0)
        size_num = min(int(params.get('page[size]', 25)), MAX_RESOURCE_PAGE_SIZE)

        # if isinstance(self.get_item_url_name(), list):
        #     params = []
        # else:
        #     item = self.get_item(kwargs.get(self.get_item_url_name()), start_num * size_num, size_num)

        item = self.get_item(kwargs.get(self.get_item_url_name()), start_num * size_num, size_num)
        return make_response(self, item, req)


class AresSubstrateInterface(SubstrateInterface):

    def init_runtime(self, block_hash=None, block_id=None):
        super().init_runtime(block_hash=block_hash, block_id=block_id)
        self.ss58_format = None
        for key in self.metadata_cache:
            if key not in resources.metadata_store:
                resources.metadata_store[key] = self.metadata_cache[key]


def create_substrate() -> SubstrateInterface:
    a = AresSubstrateInterface(url=settings.SUBSTRATE_RPC_URL, type_registry_preset=settings.TYPE_REGISTRY,
                               cache_region=None)
    a.metadata_cache = resources.metadata_store
    return a
