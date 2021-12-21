import pandas as pd
from core.models.BaseAPIPrice import *
from core.models.BaseAPIProduct import *
from core.models.BaseAPIOption import *
from core.models.BaseAPIBrand import *
from core.service.job_properties_service import JobPropertiesService
from core.service.load_config_service import LoadConfigService
from core.service.option_service import OptionService
from core.service.product_service import ProductService
from core.service.brand_service import BrandService
from core.service.price_service import PriceService
from core.service.price_list_service import PriceListService
import urllib
from pathlib import Path
from PIL import Image
import zipfile
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class ProcessCatalog:

	def __init__(self, job_properties_service: JobPropertiesService, load_config_service: LoadConfigService, temp_directory: str):
		self.job_properties_service = job_properties_service
		self.load_config_service = load_config_service
		self.temp_directory = temp_directory
		self.option_service = OptionService(job_properties=job_properties_service, load_config_service=load_config_service, service_type='OE')
		self.product_service = ProductService(job_properties=job_properties_service, load_config_service=load_config_service, service_type='OE')
		self.brand_service = BrandService(job_properties=job_properties_service, load_config_service=load_config_service, service_type='OE')
		self.price_service = PriceService(job_properties=job_properties_service, load_config_service=load_config_service, service_type='OE')
		self.price_list_service = PriceListService(job_properties=job_properties_service, load_config_service=load_config_service, service_type='OE')
		self.brand = self.brand_service.get_or_create_brand(Brand('BMW'),return_type='product')

	def test_delete_option(self):

		size_options = []
		sizes = ['sm','md','xl']
		for size in sizes:
			size_options.append(vars(Option(size)))
		if size_options:
			size_option = BaseOption('Size',size_options)

		# create master variant
		sku = 'test-kh'
		mpn = 'test-kh'
		brand = self.brand
		catalog_type = 'ACCESSORIES'
		subtype = 'MERCHANDISE'
		product = CoreProduct(sku, mpn, brand, catalog_type, subtype)
		product.catalog = 'custom'
		master_variant = MasterVariant(product)
		db_master, created_master = self.product_service.get_or_create_product(master_variant, brand)

		# create options on the master variant
		if not created_master:
			db_master.mpn = master_variant.mpn
			db_master.brand = master_variant.brand
			db_master.catalogType = master_variant.catalogType
			db_master.catalogSubtype = master_variant.catalogSubtype
			db_master.name = master_variant.name
			db_master.description = master_variant.description
			db_master.colors = master_variant.colors
			db_master.illustrationCode = master_variant.illustrationCode
			db_master.group = master_variant.group
			db_master.subgroup = master_variant.subgroup
			db_master.urlGroup = master_variant.urlGroup
			db_master.urlSubgroup = master_variant.urlSubgroup
			self.product_service.update_product(db_master.id,db_master)
			# get options
			option_list = self.option_service.list_options(db_master.id)
			for option in option_list:
				option_obj = self.option_service.get_option(db_master.id,option.id)
				if option_obj.name == 'Size':
					self.update_option(db_master.id, option_obj, sizes)

		return None

	def test_delete_products(self):

		new_variants = ['test-kh-variant-2']

		# get existing variant skus into a list
		existing_skus = self.product_service.list_products(brand='kh-test', mpnStripped='testkh')
		# get existing variants skus into a list
		existing_variants = [x for x in existing_skus if x.type() == 'Variant']
		# compare list
		deleted_skus =  [x for x in existing_variants if x.sku not in new_variants]
		# delete any sku not in new list
		for sku in deleted_skus:
			self.product_service.delete_product(sku)

		return None

	def test_options(self):
		
		size_options = []
		sizes = ['sm','md','lg','xl']
		for size in sizes:
			size_options.append(vars(Option(size)))
		if size_options:
			size_option = BaseOption('Size',size_options)

		# create master variant
		sku = 'test-kh'
		mpn = 'test-kh'
		brand = self.brand
		catalog_type = 'ACCESSORIES'
		subtype = 'MERCHANDISE'
		product = CoreProduct(sku, mpn, brand, catalog_type, subtype)
		product.catalog = 'custom'
		master_variant = MasterVariant(product)
		db_master, created_master = self.product_service.get_or_create_product(master_variant, brand)

		# create options on the master variant
		if not created_master:
			db_master.mpn = master_variant.mpn
			db_master.brand = master_variant.brand
			db_master.catalogType = master_variant.catalogType
			db_master.catalogSubtype = master_variant.catalogSubtype
			db_master.name = master_variant.name
			db_master.description = master_variant.description
			db_master.colors = master_variant.colors
			db_master.illustrationCode = master_variant.illustrationCode
			db_master.group = master_variant.group
			db_master.subgroup = master_variant.subgroup
			db_master.urlGroup = master_variant.urlGroup
			db_master.urlSubgroup = master_variant.urlSubgroup
			self.product_service.update_product(db_master.id,db_master)
			# get options
			option_list = self.option_service.list_options(db_master.id)
			for option in option_list:
				option_obj = self.option_service.get_option(db_master.id,option.id)
				if option_obj.name == 'Size':
					self.update_option(db_master.id, option_obj, sizes)
		else:
			if len(size_options) > 1:
				self.option_service.create_option(db_master.id,vars(size_option))

		# create variants with those options
		sku = 'test-kh-variant'
		mpn = 'test-kh'
		brand = self.brand
		catalog_type = 'ACCESSORIES'
		subtype = 'MERCHANDISE'
		product = CoreProduct(sku, mpn, brand, catalog_type, subtype)
		product.catalog = 'custom'
		option_list = self.option_service.list_options(db_master.id)
		new_options = []
		if len(size_options) > 1:
			option_value = 'md'
			if option_value:
				size_option_id = None
				for item in option_list:
					if item.name == 'Size':
						size_option_id = item.id
				if size_option_id:
					size_option = self.option_service.get_option(db_master.id,size_option_id)
					size_option_list = size_option.options
					option = next((item for item in size_option_list if item["name"] == option_value), None)
					new_options.append({'id':option['id']})
		master_variant = {'id':db_master.id}

		variant = Variant(product, master_variant)
		db_variant, created_variant = self.product_service.get_or_create_product(variant, brand)
		if new_options:
			db_variant.variantOptions = new_options
			self.product_service.update_product(db_variant.id,db_variant)
		
		# create variants with those options
		sku = 'test-kh-variant-2'
		mpn = 'test-kh'
		brand = self.brand
		catalog_type = 'ACCESSORIES'
		subtype = 'MERCHANDISE'
		product = CoreProduct(sku, mpn, brand, catalog_type, subtype)		
		product.catalog = 'custom'
		option_list = self.option_service.list_options(db_master.id)
		new_options = []
		if len(size_options) > 1:
			option_value = 'sm'
			if option_value:
				size_option_id = None
				for item in option_list:
					if item.name == 'Size':
						size_option_id = item.id
				if size_option_id:
					size_option = self.option_service.get_option(db_master.id,size_option_id)
					size_option_list = size_option.options
					option = next((item for item in size_option_list if item["name"] == option_value), None)
					new_options.append({'id':option['id']})
		master_variant = {'id':db_master.id}

		variant = Variant(product, master_variant)
		db_variant, created_variant = self.product_service.get_or_create_product(variant, brand)
		if new_options:
			db_variant.variantOptions = new_options
			self.product_service.update_product(db_variant.id,db_variant)

		return None

	def delete_catalog(self):

		brand_products = self.product_service.list_products(brand='BMW')
		log.info(brand_products)
		while brand_products:
			for product in brand_products:
				self.product_service.delete_product(product)
			brand_products = self.product_service.list_products(brand='BMW')

	def process_catalog(self, file_name):
		"""
		Start the parsing process for the given file
		:param catalog_file:
		:return:
		"""

		with zipfile.ZipFile(file_name, 'r') as zip_ref:
			zip_ref.extractall(self.temp_directory)

		catalog_files = []

		for file in Path(self.temp_directory).iterdir():
			if file.is_file() and file.suffix == '.xlsx':
				catalog_files.append(file)

		for catalog_file in catalog_files:

			if 'bicycle' in catalog_file.name:

				# read file into pandas dataframe
				catalog = pd.read_excel(catalog_file, sheet_name='Data', engine='openpyxl',converters={'partNumber':str})

				# fill null values with value for grouping
				catalog['parentPartNumber'].fillna('empty',inplace=True)

				# group the lifestyle catalog by part part numbers
				lifestyle_grouped = catalog.groupby('parentPartNumber')

				# loop over groups
				for name, group in lifestyle_grouped:
					# empty are products, not variants
					if name == 'empty':
						self.process_products(name, group)
					elif not name.startswith('SUIT02'):
						self.process_variants(name, group)
				# process bundles
				for name, group in lifestyle_grouped:
					if name.startswith('SUIT02') and name not in ['SUIT022116','SUIT022113','SUIT022114','SUIT022111']:
						self.process_bundles(name, group)

	def process_products(self, name, group):
		for index, row in group.iterrows():
			# required fields
			sku = self.get_text_field(row['partNumber'])
			mpn = sku
			brand = self.brand
			catalog_type = 'ACCESSORIES'
			subtype = 'MERCHANDISE'
			# initialize product
			product = CoreProduct(sku, mpn, brand, catalog_type, subtype)
			# optional fields
			product.catalog = 'custom'
			product.name = self.get_text_field(row['marketingName'])
			product.description = self.format_description(self.get_text_field(row['description']))
			if self.get_text_field(row['colorOption']):
				product.colors = [self.get_text_field(row['colorOption'])]
			product.illustrationCode = self.get_text_field(row['Primary Image'])
			product.group = self.get_text_field(row['Category1_Level1'])
			product.subgroup = self.get_text_field(row['Subcategory1_Level2'])
			# process url group/subgroup
			product.urlGroup = urllib.parse.quote(product.group)
			product.urlSubgroup = urllib.parse.quote(product.subgroup)
			# set isUniversal = True on all products
			product.isUniversal = True
			# pricing info
			msrp = float(self.get_float_field(row['productMSRP ($)']))*100
			cost = float(self.get_float_field(row['productCost ($)']))*100
			price = Price(msrp,cost,0,0)
			# update or create the product
			db_product, created_product = self.product_service.get_or_create_product(product, brand)
			if not created_product:
				db_product.mpn = product.mpn
				db_product.brand = product.brand
				db_product.catalogType = product.catalogType
				db_product.catalogSubtype = product.catalogSubtype
				db_product.name = product.name
				db_product.description = product.description
				db_product.colors = product.colors
				db_product.illustrationCode = product.illustrationCode
				db_product.group = product.group
				db_product.subgroup = product.subgroup
				db_product.urlGroup = product.urlGroup
				db_product.urlSubgroup = product.urlSubgroup
				self.product_service.update_product(db_product.id,db_product)
			# get or create the pricing
			db_price, created_price = self.price_service.get_or_create_price(db_product.id,price)
			if not created_price:
				# have to manually set, because the price API does not return the product ID on a GET
				db_price.product = {'id':db_product.id}
				db_price.msrp = price.msrp
				db_price.cost = price.cost
				self.price_service.update_price(db_price.id, db_price)
			# images
			images = []
			# get all primary images
			if 'Primary Image' in group.columns:
				image = self.get_text_field(row['Primary Image'])
				if image:
					images.append(image)
			# get all secondary images
			if 'Secondary Image' in group.columns:
				image = self.get_text_field(row['Secondary Image'])
				if image:
					images.append(image)
			# get all tertiary images
			if 'Tertiary Image' in group.columns:
				image = self.get_text_field(row['Tertiary Image'])
				if image:
					images.append(image)
			
			# remove duplicates from images. Primary/Tertiary are often the same, but not always
			images = list(set([x for x in images if x != 'None']))

			# process the images
			self.process_images(sku, images)

	def process_variants(self, name, group):
		log.info(f"Working on group: {name}")
		group = group.reset_index()
		# create the option objects for the ParentProduct
		color_options = []
		if 'colorOption' in group.columns:
			colors = group['colorOption'].dropna().unique().tolist()
			for color in colors:
				color_options.append(vars(Option(color)))
			if color_options:
				color_option = BaseOption('Color',color_options)
		size_options = []
		if 'sizeOption' in group.columns:
			sizes = group['sizeOption'].dropna().unique().tolist()
			for size in sizes:
				size_options.append(vars(Option(size)))
			if size_options:
				size_option = BaseOption('Size',size_options)
		position_options = []
		if 'positionOption' in group.columns:
			positions = group['positionOption'].dropna().unique().tolist()
			for position in positions:
				position_options.append(vars(Option(position)))
			if position_options:
				position_option = BaseOption('Position',position_options)

		# get image information
		images = []
		# get all primary images
		if 'Primary Image' in group.columns:
			image_list = group['Primary Image'].dropna().unique().tolist()
			for image in image_list:
				images.append(image)
		# get all secondary images
		if 'Secondary Image' in group.columns:
			image_list = group['Secondary Image'].dropna().unique().tolist()
			for image in image_list:
				images.append(image)
		# get all tertiary images
		if 'Tertiary Image' in group.columns:
			image_list = group['Tertiary Image'].dropna().unique().tolist()
			for image in image_list:
				images.append(image)

		# remove duplicates from images. Primary/Tertiary are often the same, but not always
		images = list(set([x for x in images if x != 'None']))

		# process the images
		self.process_images(name, images)

		# get info for master variant container (name, description, etc)
		row = group.loc[0]
		# required fields
		sku = name
		mpn = name
		brand = self.brand
		catalog_type = 'ACCESSORIES'
		subtype = 'MERCHANDISE'
		product = CoreProduct(sku, mpn, brand, catalog_type, subtype)
		# optional fields
		product.catalog = 'custom'
		product.name = self.get_text_field(row['marketingName'])
		product.description = self.format_description(self.get_text_field(row['description']))
		product.colors = colors
		product.illustrationCode = self.get_text_field(row['Primary Image'])
		product.group = self.get_text_field(row['Category1_Level1'])
		product.subgroup = self.get_text_field(row['Subcategory1_Level2'])
		# process url group/subgroup
		product.urlGroup = urllib.parse.quote(product.group)
		product.urlSubgroup = urllib.parse.quote(product.subgroup)
		# set isUniversal = True on all products
		product.isUniversal = True
		# pricing info
		msrp = float(self.get_float_field(row['productMSRP ($)']))*100
		cost = float(self.get_float_field(row['productCost ($)']))*100
		price = Price(msrp,cost,0,0)

		# create blank master variant for initial create
		master_variant = MasterVariant(product)
		db_master, created_master = self.product_service.get_or_create_product(master_variant, brand)
		# create options if the master variant creation returns the product ID
		if not created_master:
			db_master.mpn = master_variant.mpn
			db_master.brand = master_variant.brand
			db_master.catalogType = master_variant.catalogType
			db_master.catalogSubtype = master_variant.catalogSubtype
			db_master.name = master_variant.name
			db_master.description = master_variant.description
			db_master.colors = master_variant.colors
			db_master.illustrationCode = master_variant.illustrationCode
			db_master.group = master_variant.group
			db_master.subgroup = master_variant.subgroup
			db_master.urlGroup = master_variant.urlGroup
			db_master.urlSubgroup = master_variant.urlSubgroup
			self.product_service.update_product(db_master.id,db_master)
			# get options
			option_list = self.option_service.list_options(db_master.id)
			for option in option_list:
				option_obj = self.option_service.get_option(db_master.id,option.id)
				if option_obj.name == 'Size':
					self.update_option(db_master.id, option_obj, sizes)
				if option_obj.name == 'Color':
					self.update_option(db_master.id, option_obj, colors)
				if option_obj.name == 'Position':
					self.update_option(db_master.id, option_obj, positions)

			# find all variants that are in the current feed
			new_variants = group['partNumber'].dropna().unique().tolist()
			# get existing variant skus into a list
			existing_skus = self.product_service.list_products(brand=self.brand['name'], mpnStripped=db_master.mpn)
			# get existing variants skus into a list
			existing_variants = [x for x in existing_skus if x.type() == 'Variant']
			# compare list
			deleted_skus =  [x for x in existing_variants if x.sku not in new_variants]
			# delete any sku not in new list
			for sku in deleted_skus:
				self.product_service.delete_product(sku)

		else:
			if len(color_options) > 1:
				self.option_service.create_option(db_master.id,vars(color_option))
			if len(size_options) > 1:
				self.option_service.create_option(db_master.id,vars(size_option))
			if position_options and len(position_options) > 1:
				self.option_service.create_option(db_master.id,vars(position_option))

		db_price, created_price = self.price_service.get_or_create_price(db_master.id,price)
		if not created_price:
			# have to manually set, because the price API does not return the product ID on a GET
			db_price.product = {'id':db_master.id}
			db_price.msrp = price.msrp
			db_price.cost = price.cost
			self.price_service.update_price(db_price.id, db_price)

		for index, row in group.iterrows():
			# required fields
			sku = self.get_text_field(row['partNumber'])
			mpn = name
			# need to lookup brand
			brand = self.brand
			catalog_type = 'ACCESSORIES'
			subtype = 'MERCHANDISE'
			product = CoreProduct(sku, mpn, brand, catalog_type, subtype)
			# optional fields
			product.catalog = 'custom'
			product.name = self.get_text_field(row['marketingName'])
			product.description = self.format_description(self.get_text_field(row['description']))
			product.colors = colors
			product.illustrationCode = self.get_text_field(row['Primary Image'])
			product.group = self.get_text_field(row['Category1_Level1'])
			product.subgroup = self.get_text_field(row['Subcategory1_Level2'])
			# process url group/subgroup
			product.urlGroup = urllib.parse.quote(product.group)
			product.urlSubgroup = urllib.parse.quote(product.subgroup)
			# set isUniversal = True on all products
			product.isUniversal = True
			# pricing info
			msrp = float(self.get_float_field(row['productMSRP ($)']))*100
			cost = float(self.get_float_field(row['productCost ($)']))*100
			price = Price(msrp,cost,0,0)

			# get image information
			images = []
			# get all primary images
			if 'Primary Image' in group.columns:
				image = self.get_text_field(row['Primary Image'])
				if image:
					images.append(image)
			# get all secondary images
			if 'Secondary Image' in group.columns:
				image = self.get_text_field(row['Secondary Image'])
				if image:
					images.append(image)
			# get all tertiary images
			if 'Tertiary Image' in group.columns:
				image = self.get_text_field(row['Tertiary Image'])
				if image:
					images.append(image)

			# remove duplicates from images. Primary/Tertiary are often the same, but not always
			images = list(set([x for x in images if x != 'None']))

			# process the images
			self.process_images(sku, images)

			# pull options for the product
			option_list = self.option_service.list_options(db_master.id)
			new_options = []
			# get color option
			if 'colorOption' in group.columns:
				if len(color_options) > 1:
					option_value = self.get_text_field(row['colorOption'])
					if option_value:
						color_option_id = None
						for item in option_list:
							if item.name == 'Color':
								color_option_id = item.id
						if color_option_id:
							color_option = self.option_service.get_option(db_master.id,color_option_id)
							color_option_list = color_option.options
							option = next((item for item in color_option_list if item["name"] == option_value), None)
							new_options.append({'id':option['id']})
			# get size option
			if 'sizeOption' in group.columns:
				if len(size_options) > 1:
					option_value = self.get_text_field(row['sizeOption'])
					if option_value:
						size_option_id = None
						for item in option_list:
							if item.name == 'Size':
								size_option_id = item.id
						if size_option_id:
							size_option = self.option_service.get_option(db_master.id,size_option_id)
							size_option_list = size_option.options
							option = next((item for item in size_option_list if item["name"] == option_value), None)
							new_options.append({'id':option['id']})
			# get position option
			if 'positionOption' in group.columns:
				if len(position_options) > 1:
					option_value = self.get_text_field(row['positionOption'])
					if option_value:
						position_option_id = None
						for item in option_list:
							if item.name == 'Position':
								position_option_id = item.id
						if position_option_id:
							position_option = self.option_service.get_option(db_master.id,position_option_id)
							position_option_list = position_option.options
							option = next((item for item in position_option_list if item["name"] == option_value), None)
							new_options.append({'id':option['id']})

			# add all other variants under the master variant
			master_variant = {'id':db_master.id}

			variant = Variant(product, master_variant)
			db_variant, created_variant = self.product_service.get_or_create_product(variant, brand)
			if not created_variant:
				db_variant.mpn = product.mpn
				db_variant.brand = product.brand
				db_variant.catalogType = product.catalogType
				db_variant.catalogSubtype = product.catalogSubtype
				db_variant.name = product.name
				db_variant.description = product.description
				db_variant.colors = product.colors
				db_variant.illustrationCode = product.illustrationCode
				db_variant.group = product.group
				db_variant.subgroup = product.subgroup
				db_variant.urlGroup = product.urlGroup
				db_variant.urlSubgroup = product.urlSubgroup
				self.product_service.update_product(db_variant.id,db_variant)
			db_price, created_price = self.price_service.get_or_create_price(db_variant.id,price)
			if not created_price:
				# have to manually set, because the price API does not return the product ID on a GET
				db_price.product = {'id':db_master.id}
				db_price.msrp = price.msrp
				db_price.cost = price.cost
				self.price_service.update_price(db_price.id, db_price)
			
			# update variantOptions on the product
			if new_options:
				db_variant.variantOptions = new_options
				self.product_service.update_product(db_variant.id,db_variant)

	def process_bundles(self, name, group):
		group = group.reset_index()
		row = group.loc[0]
		# required fields
		sku = self.get_text_field(row['partNumber'])
		mpn = name
		brand = self.brand
		catalog_type = 'ACCESSORIES'
		subtype = 'MERCHANDISE'
		product = CoreProduct(sku, mpn, brand, catalog_type, subtype)
		# optional fields
		product.catalog = 'custom'
		product.name = self.get_text_field(row['marketingName'])
		product.description = self.format_description(self.get_text_field(row['description']))
		product.illustrationCode = self.get_text_field(row['Primary Image'])
		product.group = self.get_text_field(row['Category1_Level1'])
		product.subgroup = self.get_text_field(row['Subcategory1_Level2'])
		# process url group/subgroup
		product.urlGroup = urllib.parse.quote(product.group)
		product.urlSubgroup = urllib.parse.quote(product.subgroup)
		# set isUniversal = True on all products
		product.isUniversal = True

		bundle = ProductBundle(product)
		bundle, created = self.product_service.get_or_create_product(bundle,brand)

		all_variants = []

		for index, row in group.iterrows():

			variant_sku = self.get_text_field(row['partNumber'])
			variant = self.product_service.get_product_by_sku(variant_sku, brand)
			all_variants.append(variant)
		
		all_variants = [x for x in all_variants if x != None]

		if all_variants:

			variant_mpn_list = list(set([x.mpn for x in all_variants]))

			for mpn in variant_mpn_list:
				master_variant = self.product_service.get_product_by_sku(mpn,brand)
				bundle_ref = bundleReference(master_variant,bundle,1)
				bundle.bundleReferences['items'].append(vars(bundle_ref))
			
			self.product_service.update_product(bundle.id,bundle)
		
		# get image information
		images = []
		# get all primary images
		if 'Primary Image' in group.columns:
			image = self.get_text_field(row['Primary Image'])
			if image:
				images.append(image)
		# get all secondary images
		if 'Secondary Image' in group.columns:
			image = self.get_text_field(row['Secondary Image'])
			if image:
				images.append(image)
		# get all tertiary images
		if 'Tertiary Image' in group.columns:
			image = self.get_text_field(row['Tertiary Image'])
			if image:
				images.append(image)

		# remove duplicates from images. Primary/Tertiary are often the same, but not always
		images = list(set([x for x in images if x != 'None']))

		# process the images
		self.process_images(sku, images)

	def format_description(self, description):

		if description:
			# split the description into bullet points
			full_description = description.split('\n·')
			# first item in list is the description
			description = full_description[0]
			# all others are bullet points
			list_points = full_description[1:]
			# check if description begins with bullet
			if description.startswith('·'):
				list_points.insert(0, description.lstrip('· '))
				description = ''
			# put the bullet points into an unordered list
			description = description + '<ul>'
			for point in list_points:
				sub_points = point.split('\n-')
				# if more than one item in sub_points, there is a sublist
				if len(sub_points) > 1:
					description = description + '<li>' + sub_points[0]
					description = description + '<ul>'
					for sub_point in sub_points[1:]:
						description = description + '<li>' + sub_point + '</li>'
					description = description + '</ul></li>'
				else:
					description = description + '<li>' + point + '</li>'
			description = description + '</ul>'

		return description

	def update_option(self, product_id, option_obj, option_list):
		
		existing_values = [x['name'] for x in option_obj.options]
		# find new items
		new_values = [str(x) for x in option_list if str(x) not in existing_values]
		# find items that are no longer available
		deleted_values = [x for x in existing_values if x not in option_list]
		if new_values:
			# update option to include new option
			for value in new_values:
				option_obj.options.append(vars(Option(value)))
			self.option_service.update_option(product_id,option_obj.id,vars(option_obj))
		if deleted_values:
			for value in deleted_values:
				option_obj.options = [i for i in option_obj.options if not (i['name'] == value)]
			self.option_service.update_option(product_id,option_obj.id,vars(option_obj))

	def process_images(self, sku, image_list):

		# create final image dir
		final_dir = Path(self.temp_directory) / 'images'
		final_dir.mkdir(parents=True, exist_ok=True)

		img_dir = Path(self.temp_directory)
		final_dir = Path(self.temp_directory) / 'images'

		for index, image in enumerate(image_list):

			file_path = img_dir / image

			# first image has no suffix
			if index == 0:
				new_file_path = final_dir / (sku + '.jpg')
			# other images need "_<count>" suffix
			else:
				new_file_path = final_dir / (sku + '_' + str(index + 1) + '.jpg')
			
			# rename image, convert any tif to jpg
			try:
				with open(file_path, 'rb') as file:
					img = Image.open(file)
					# Need to convert to RGB since tif uses CYMK
					img.convert("RGB")
					img.save(str(new_file_path))
			except FileNotFoundError as e:
				log.error(e)

	def get_text_field(self, field):

		# a blank row into pandas is a Numpy 'NaN'. This check if the field is 'NaN', and assigns Pythonic 'None'
		if pd.isnull(field) or field == '':
			field = None
		else:
			return str(field)
		
		return str(field)

	def get_float_field(self, field):

		# a blank row into pandas is a Numpy 'NaN'. This check if the field is 'NaN', and assigns Pythonic 'None'
		if pd.isnull(field) or field == '':
			field = 0
		else:
			return float(field)
		
		return float(field)
