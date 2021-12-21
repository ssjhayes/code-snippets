import re
from core.service.load_config_service import LoadConfigService
from catv2_loader.util import html_formatter
from core.models.product import Product
import xml.etree.ElementTree as ET

ns = {'ac': 'http://www.autocare.org'}

class PiesToProductConverter:

	"""
	Converts PIES XML format to the api payload
	"""
	product = Product()

	def __init__(self, pies_item: ET, load_config_service: LoadConfigService):
		self.lcs = load_config_service
		self.__base_properties(pies_item)
		self.__brand(pies_item)
		self.__packages(pies_item)
		self.__descriptions(pies_item)

	def __strip(self, value: str):
		return ''.join(e for e in value if e.isalnum())

	def __get_item_text(self, item: ET, name: str):
		i = item.find(name, ns)
		if i is None:
			return None
		else:
			return i.text

	def __base_properties(self, item: ET):
		self.product.ptid = self.__get_item_text(item=item, name='ac:PartTerminologyID')
		self.product.sku = item.find('ac:PartNumber', ns).text
		self.product.mpn = item.find('ac:PartNumber', ns).text
		self.product.sku_stripped = self.__strip(self.product.sku)
		self.product.mpn_stripped = self.__strip(self.product.mpn)
		self.product.upc = self.__get_item_text(item=item, name='ac:ItemLevelGTIN')

	def __brand(self, item: ET):
		brand_id = item.find('ac:BrandAAIAID', ns).text
		if brand_id:
			self.product.brand = {'id': self.lcs.get_brand_id(brand_id), 'name': self.lcs.get_brand_name(brand_id)}

	def __dimensions(self, package: ET):
		dim = {'dimensionUnit': 'IN',
				'length': None,
				'width': None,
				'height': None,
				'weightUnit': 'LB',
				'weight': None
				}

		dimensions = package.find('ac:Dimensions', ns)
		if dimensions is not None:
			dim['dimensionUnit'] = dimensions.attrib['UOM']
			dim['length'] = dimensions.find('ac:Length', ns).text
			dim['width'] = dimensions.find('ac:Width', ns).text
			dim['height'] = dimensions.find('ac:Height', ns).text

		weights = package.find('ac:Weights', ns)
		if weights is not None:
			wuom = weights.attrib['UOM']
			if wuom == 'PG':
				dim['weightUnit'] = 'LB'
			else:
				dim['weightUnit'] = 'KG'
			dim['weight'] = weights.find('ac:Weight', ns).text

		return dim

	def __clean(self, s: str):
		return re.sub('\\s{2,}', ' ', s.replace('\n', ''))

	def __packages(self, item: ET):

		packages = item.find('ac:Packages', ns)
		for package in packages.findall('ac:Package', ns):
			if package.find('ac:HazardousMaterial', ns):
				self.product.hazmat = True

			uom = package.find('ac:PackageUOM', ns)

			if uom is not None and uom.text == 'EA':
				self.product.sold_in_qty = int(package.find('ac:QuantityofEaches', ns).text)
				self.product.dimensions = self.__dimensions(package)

	def __include_attribute(self, attr_id: str):
		attr_blacklist = self.lcs.get_pies_attr_blacklist()
		if not attr_blacklist:
			return True
		else:
			return attr_id not in attr_blacklist

	def __descriptions(self, item: ET):
		descriptions = item.find('ac:Descriptions', ns)
		fd = ['', '']
		attributes = []

		for desc in descriptions.findall('ac:Description', ns):
			code = desc.attrib['DescriptionCode']
			value = desc.text
			if code == "DES":
				self.product.name = value
				fd[0] = html_formatter.p(self.__clean(value))
			elif code == "MKT":
				fd[1] = html_formatter.p(self.__clean(value))

		product_attributes = item.find('ac:ProductAttributes', ns)
		if product_attributes is not None:
			for attr in product_attributes.findall('ac:ProductAttribute', ns):
				attr_id = attr.attrib['AttributeID']
				if attr_id == self.lcs.get_prop_65_attribute():
					self.product.disclaimer_text = self.__clean(attr.text)
				else:
					if self.__include_attribute(attr_id):
						attributes.append(f'{attr_id}: {self.__clean(attr.text)}')

		if attributes:
			fd.append('<ul>')
			for ai in attributes:
				fd.append(f'<li>{ai}</li>')
			fd.append('</ul>')
		self.product.description = ''.join(fd)

