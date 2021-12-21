import uuid
import logging
import requests
from requests.auth import HTTPBasicAuth
from core.service.job_properties_service import JobPropertiesService
from core.service.load_config_service import LoadConfigService
from datetime import datetime
from core.models.BaseAPIProduct import *
import json
import time

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

RETRY_COUNT = 5
RETRY_STATUSES = [502]

"""
Product Service for calling the product apis
"""


class ProductService:
	def __init__(self, job_properties: JobPropertiesService, load_config_service: LoadConfigService, service_type: str):
		self.job_properties = job_properties
		self.load_config_service = load_config_service
		self.api_endpoint = f'http://{job_properties.api_host}/product'
		self.basic_auth = HTTPBasicAuth(self.job_properties.api_username,
										self.job_properties.api_password)
		self.service_type = service_type

	def header(self):
		"""
		Get the API Header
		:return:
		"""
		return {'Merchant-Id': str(self.load_config_service.get_merchant_id()),
				'Correlation-Id': str(uuid.uuid1()), 'Content-Type': 'application/json'}

	def list_products(self, **kwargs):
		"""
		Make the call to the product API to fetch the product
		:param kwargs:
		:kwarg options [backordered, brand, catalog, createdTs, id, mpnStripped, onBackorderUntilTs, skuStripped, type, updatedTs, limit, offset]
		:return list of product objects:
		"""
		cnt = 1

		api_endpoint = self.api_endpoint

		if kwargs:
			api_endpoint = api_endpoint + '?'

			# base params
			filter = 'filters='
			limit = 'limit='
			offset = 'offset='

			# get filters from kwargs
			if 'backordered' in kwargs:
				filter = filter + 'backordered=' + str(kwargs.get('backordered')) + str("<AND>")
			if 'brand' in kwargs:
				filter = filter + 'brand=' + str(kwargs.get('brand')) + str("<AND>")
			if 'catalog' in kwargs:
				filter = filter + 'catalog=' + str(kwargs.get('catalog')) + str("<AND>")
			if 'createdTs' in kwargs:
				filter = filter + 'createdTs=' + str(kwargs.get('createdTs')) + str("<AND>")
			if 'id' in kwargs:
				filter = filter + 'id=' + str(kwargs.get('id')) + str("<AND>")
			if 'mpnStripped' in kwargs:
				mpn_stripped = str(kwargs.get('mpnStripped')).replace('-','').replace(' ','').replace('.','')
				mpn_stripped = [x for x in mpn_stripped]
				for x in range(len(mpn_stripped)):
					mpn_stripped[x] = mpn_stripped[x].lower()
				mpn_stripped = ''.join(mpn_stripped)
				filter = filter + 'mpnStripped=' + mpn_stripped + str("<AND>")
			if 'onBackorderUntilTs' in kwargs:
				filter = filter + 'onBackorderUntilTs=' + str(kwargs.get('onBackorderUntilTs')) + str("<AND>")
			if 'skuStripped' in kwargs:
				sku_stripped = str(kwargs.get('skuStripped')).replace('-','').replace(' ','').replace('.','')
				sku_stripped = [x for x in sku_stripped]
				for x in range(len(sku_stripped)):
					sku_stripped[x] = sku_stripped[x].lower()
				sku_stripped = ''.join(sku_stripped)
				filter = filter + 'skuStripped=' + sku_stripped + str("<AND>")
			if 'type' in kwargs:
				filter = filter + 'type=' + str(kwargs.get('type')) + str("<AND>")
			if 'updatedTs' in kwargs:
				filter = filter + 'type=' + str(kwargs.get('updatedTs')) + str("<AND>")

			# strip trailing <AND>
			filter = filter.rstrip('<AND>')

			# get limit and offset from kwargs
			if 'limit' in kwargs:
				limit = limit + str(kwargs.get('limit'))

			if 'offset' in kwargs:
				offset = offset + str(kwargs.get('offset'))

			# append kwargs to endpoint
			if filter != 'filters=':
				api_endpoint = api_endpoint + filter + '&'
			if limit != 'limit=':
				api_endpoint = api_endpoint + limit + '&'
			if offset != 'offset=':
				api_endpoint = api_endpoint + offset

			# remove trailing &
			api_endpoint = api_endpoint.rstrip('&')

		while cnt <= RETRY_COUNT:
			response = requests.get(api_endpoint, auth=self.basic_auth, headers=self.header())

			if (response.status_code in RETRY_STATUSES):
				print(f'Got status {response.status_code}. Retry attempt {cnt}')
				cnt += 1
			else:
				if response.status_code == 200:
					product_list = response.json()
					items = product_list['items']
					product_list = []
					for item in items:
						product_list.append(product_from_db(item))
					return product_list
				else:
					self.log_result(response, self.service_type, item_payload=filter, uri='list')
					return response.status_code

	def create_product(self, product: object):
		"""
		Make the call to the product API to load the product
		:param product object:
		:return product_id:
		"""
		cnt = 1

		while cnt <= RETRY_COUNT:
			response = requests.post(self.api_endpoint, auth=self.basic_auth, headers=self.header(), json=vars(product))

			if (response.status_code in RETRY_STATUSES):
				print(f'Got status {response.status_code}. Retry attempt {cnt}')
				cnt += 1
			else:
				self.log_result(response, self.service_type, item_payload=vars(product), uri='create')
				if response.status_code == 201:
					return self.__get_product_id(response)
				else:
					return False
	
	def get_product(self, product_id: int):
		"""
		Make the call to the product API to get the product
		:param product_id:
		:return product object:
		"""
		cnt = 1

		api_endpoint = self.api_endpoint + '/' + str(product_id)

		while cnt <= RETRY_COUNT:
			response = requests.get(api_endpoint, auth=self.basic_auth, headers=self.header())

			if (response.status_code in RETRY_STATUSES):
				print(f'Got status {response.status_code}. Retry attempt {cnt}')
				cnt += 1
			else:
				if response.status_code == 200:
					product = response.json()
					product_object = product_from_db(product)
					return product_object
				else:
					self.log_result(response, self.service_type, item_payload=None, uri='get')
					return response.status_code
	
	def get_or_create_product(self, product: object, brand: dict):
		"""
		Make the call to the Product API to get a product, or create the product if it does not exist
		:param product:
		:return (product: object, created: bool):
		"""
		# create a stripped sku
		sku_stripped = product.sku.replace('-','').replace(' ','').replace('.','')
		sku_stripped = [x for x in sku_stripped]
		for x in range(len(sku_stripped)):
			sku_stripped[x] = sku_stripped[x].lower()
		sku_stripped = ''.join(sku_stripped)

		# list products with skuStripped as search filter
		product_list = self.list_products(skuStripped=sku_stripped,brand=brand['name'])
		# if brands, get brand
		if product_list:
			if len(product_list) == 1:
				product_obj = self.get_product(product_list[0].id)
				product_obj = (product_obj, False)
			else:
				log.error(f'More than one product with the sku: {sku_stripped} found for brand: {brand}')
		# else, create product
		else:
			new_product = self.create_product(product)
			product_obj = self.get_product(new_product)
			log.info(f"Product SKU: {product_obj.sku}. Product ID: {product_obj.id}")
			product_obj = (product_obj, True)

		return product_obj

	def get_product_by_sku(self, sku, brand):
		# create a stripped sku
		sku_stripped = sku.replace('-','').replace(' ','').replace('.','')
		sku_stripped = [x for x in sku_stripped]
		for x in range(len(sku_stripped)):
			sku_stripped[x] = sku_stripped[x].lower()
		sku_stripped = ''.join(sku_stripped)

		product_list = self.list_products(skuStripped=sku_stripped,brand=brand['name'])
		# if brands, get brand
		if product_list:
			if len(product_list) == 1:
				product_obj = self.get_product(product_list[0].id)
				return product_obj
			else:
				log.error(f'More than one product with the sku: {sku_stripped} found for brand: {brand}')
				return None
		else:
			return None

	def update_product(self, product_id: int, product: object):
		"""
		Make the call to the product API to load the product
		:param product_id:
		:param item_payload:
		:return response:
		"""
		cnt = 1

		api_endpoint = self.api_endpoint + '/' + str(product_id)

		while cnt <= RETRY_COUNT:
			response = requests.put(api_endpoint, auth=self.basic_auth, headers=self.header(), data=json.dumps(vars(product)))

			if (response.status_code in RETRY_STATUSES):
				print(f'Got status {response.status_code}. Retry attempt {cnt}')
				cnt += 1
			else:
				if response.status_code == 204:
					return response.status_code
				else:
					self.log_result(response, self.service_type, item_payload=vars(product), uri='update')
					return response.status_code

	def delete_product(self, product: object):
		"""
		Make the call to the product API to delete the product
		:param product_id:
		:return response:
		"""
		cnt = 1

		api_endpoint = self.api_endpoint + '/' + str(product.id)

		while cnt <= RETRY_COUNT:
			response = requests.delete(api_endpoint, auth=self.basic_auth, headers=self.header())

			if (response.status_code in RETRY_STATUSES):
				print(f'Got status {response.status_code}. Retry attempt {cnt}')
				cnt += 1
			else:
				if response.status_code == 204:
					return response.status_code
				else:
					self.log_result(response, self.service_type, item_payload=None, uri='delete')
					return response.status_code

	def __get_product_id(self, response):
		"""
		Get the product id returned by the product API
		:param response:
		:return:
		"""
		location = response.headers['location']
		return location.split('/')[-1]

	def log_result(self, response, log_type: str, item_payload: dict, uri: str, **kwargs):
		"""
		Log the result of the API call
		:param response:
		:param log_type:
		:param item_payload:
		:return:
		"""

		if response.status_code == 201:
			product_id = self.__get_product_id(response)
			api_response = None
		elif response.status_code == 200:
			api_response = response.text
			product_id = None
		elif response.status_code == 204:
			api_response = response.text
			product_id = None
		elif response.status_code == 500:
			product_id = None
			api_response = response.text
			log.error(f'Got status: {response.status_code}, error: {json.dumps(api_response)}, payload: {item_payload}')
		else:
			api_response = response.json()
			log.error(f'Got status: {response.status_code} trying to {uri} a product. {item_payload}. error: {json.dumps(api_response)}')
			product_id = None

		# internal service
		ingest_dao = IngestDao(id=None, ingest_type=log_type,
									uri=uri, brand=19,
									product_id=product_id,
									mpn=product_id,
									status=response.status_code,
									catalog=self.load_config_service.get_catalog(),
									cid=self.load_config_service.get_cid(),
									ingest_data=None,
									product_data=item_payload,
									api_response=api_response,
									ingest_time=datetime.now())

