import pandas as pd
import re
import sys
import pathlib
from urllib.parse import quote_plus as urlquote
from sqlalchemy.engine import create_engine
import hashlib # md5 hash gen
from PIL import Image
import datetime

# TODO: create empty rp_illustration_hotspots csv in the final exports

# get command line argument. skipping the first one (MergeAhmEpc.py --temp-dir=/tmp/32145)
args = sys.argv[1:]

# parse out arguments and assign a base directory
for arg in args:
	arg = arg.split('=')
	if arg[0] == '--temp-dir':
		# set base_dir for all working files
		base_dir = pathlib.Path(arg[1])
	if arg[0] == '--db-pass':
		db_pass = arg[1]

# raw db connection
catalog_engine = create_engine("mysql+pymysql://rp_read_only:%s@dev-catdb.revolutionparts.vpc/rp_834ce" % urlquote(db_pass))
# set global path variables

temp_raw_dir = pathlib.Path(base_dir / "working/epc/")
try:
	temp_raw_dir.mkdir(parents=True, exist_ok=False)
except FileExistsError:
	pass

final_raw_dir = pathlib.Path(base_dir / "working/final_raw/")
try:
	final_raw_dir.mkdir(parents=True, exist_ok=False)
except FileExistsError:
	pass

processed_dir = pathlib.Path(base_dir / "processed/epc")
try:
	processed_dir.mkdir(parents=True, exist_ok=False)
except FileExistsError:
	pass

image_dir = pathlib.Path(base_dir / "working/image/")
try:
	image_dir.mkdir(parents=True, exist_ok=False)
except FileExistsError:
	pass

processed_image_dir = pathlib.Path(base_dir / "processed/image/")
try:
	processed_image_dir.mkdir(parents=True, exist_ok=False)
except FileExistsError:
	pass

processed_illustrations_dir = pathlib.Path(processed_dir / "illustrations/")
try:
	processed_illustrations_dir.mkdir(parents=True, exist_ok=False)
except FileExistsError:
	pass

# row definitions
# sublists are [item length, number of occurrences]
# Record Type 10 - Vehicle Header
record_10_widths = [3,8,2,30,1,1,1]

# Record Type 15 - Vehicle Color Info
record_15_widths = [3,8,2,20,60]

# Record Type 20 - Vehicle Info
record_20_widths = [3,8,2,[2,10],[3,10],[6,24],[1,10],[7,26],[3,10]]

# Record Type 25 - Vehicle Serial Info
record_25_widths = [3,8,2,2,1,6,3,3,6,8,8,8,8,8,8,8,8,8,8,8,8]

# Record Type 30 - Vehicle Parts Block Header
record_30_header_widths = [3,8,2,2,7,3,8,75,5,13,7,2,15]

# Record Type 30 - Vehicle Parts Block Info
record_30_widths = [3,8,2,2,7,3,2,5,60,2,[2,5],[2,5],[3,10],[6,24],8,1,8,[1,10],[2,26],[3,10],3,17,7]

# take the widths and apply slice() function for different row types
def generate_slices(widths):
	slices = []
	offset = 0
	for w in widths:
		if type(w) is int:
			slices.append(slice(offset, offset+w))
			offset += w
		if type(w) is list:
			count = 0
			while count < w[1]:
				slices.append(slice(offset, offset+w[0]))
				offset += w[0]
				count += 1
	return slices

# Group definitions
group_definitions = {
	'01':'ENGINE',
	'02':'TRANSMISSION - MANUAL',
	'03':'TRANSMISSION - AUTOMATIC',
	'04':'TRANSMISSION - HONDAMATIC',
	'05':'ELECTRICAL / EXHAUST / HEATER / FUEL',
	'06':'CHASSIS',
	'07':'INTERIOR / BUMPER',
	'08':'BODY / AIR CONDITIONING',
	'09':'ACCESSORIES'
	}

image_sizes = {
	'image_size_1600':1600, # high res for magnify in ebay, gen 5 images
	'image_size_1000':1000, # high res for magnify in ebay
	'image_size_640':640, # full size image
	'image_size_485':485, # standard size for assembly
	'image_size_300':300, # preview
	'image_size_150':150, # thumbnail
	}

def get_body_style(door_count):

	if door_count == 2:
		body_style = 'Coupe'
	if door_count == 3:
		body_style = 'Hatchback'
	if door_count == 4:
		body_style = 'Sedan'
	if door_count == 5:
		body_style = '5-Door'

	return body_style

def get_position_id(position):

	aces_position_id_map = {
		'FR\.'		:['Front',22],
		'RR\.'		:['Rear',30],
		'R\.'		:['Right',12],
		'L\.'		:['Left',2],
		'FR\. L\.'	:['Front Left', 103],
		'RR\. L\.'	:['Rear Left', 105],
		'FR\. R\.'	:['Front Right', 104],
		'RR\. R\.'	:['Rear Right', 106],
	}
	position_map = None
	for id_map in aces_position_id_map:
		if re.match(rf"\b{id_map}$",position):
			position_map = aces_position_id_map[id_map]
			break
	
	if position_map == None:
		date_time = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
		log = date_time + ' - position could not be mapped: ' + str(position) + '\n'
		# log new position that needs mapped
		file_path = temp_raw_dir.resolve().parent / "PositionMapError.txt"
		file1 = open(file_path, "a+")
		file1.write(log)
		file1.close()

	return position_map

def URLifyString(string):

	string = string.replace("&","and") # replace ampersand
	string = re.sub(r"(/)|( +)|(,)|([(])|([)])|(\.)|(')", "-", string)
	string = re.sub(r"-{2,}", "-", string)
	string = string.rstrip("-").lower() # convert string to lowercase

	# string = urlquote(string) # alternate URLify option. does not match current config though

	return string

def get_aces_table(make):

	aces_cars = pd.read_sql_query(
		'''SELECT DISTINCT model, year, trim, engine FROM aces_cars WHERE make = %s ORDER BY model, year, trim, engine''',
		catalog_engine,
		params=[make])

	return aces_cars

def get_illustration_info_table(images):

	# convert list to string for sql injection
	images = "', '".join(images)
	images = "'" + images + "'"

	query_string = "SELECT imageID, file_checksum FROM rp_illustration_info WHERE imageID in (" + images + ")"

	illustration_info = pd.read_sql_query(
		query_string,
		catalog_engine)

	return illustration_info

def validate_aces(_aces, _model, _year, _trim):
	_model = _model.lower()
	_trim = _trim.lower()
	_year = int(_year)

	# set variable for return
	validated_list = []

	search_string = str(_year) + ' - ' + str(_model) + ' - ' + str(_trim)

	# Pull in aces db for the year
	aces_table = _aces[(_aces['year'] == _year)]
	# format AHM models for known differences
	specific_model_transforms = [
			# min_year, max_year, trim, model match, model replacement
			[2010, 2011, '', 'crosstour', 'accord crosstour'],
			[1984, 1987, '', 'crx', 'civic crx'],
			[1993, 1997, '', 'del sol', 'civic del sol'],
			[1969, 1972, '', 'n600', '600'],
			[1969, 1972, '', 'z600', '600'],
			[1984, 1987, 'wv', 'civic', 'wagovan']
	]
	for transform_list in specific_model_transforms:
		if _year >= transform_list[0] and _year <= transform_list[1] and not (transform_list[2] or (_trim in transform_list[2])):
			_model = re.sub(rf"^{transform_list[3]}", transform_list[4], _model)
	
	# parse AHM models for trim definitions
	model = None
	model_trim = None

	# The model sometimes contains terms that are part of the trim - We will append these to the trim after transforming it
	model_trims = [
		'crx',
		'electric',
		'ev',
		'fuel cell',
		'hybrid',
		'ngv',
		'plug-in',
	]

	# Match the model to an ACES model
	# Start with the full model and trim off terms from the end until we find a match
	temp_model = _model
	temp_model_terms = temp_model.split(" ")

	# search all rows containing models matching the terms list
	aces_table = aces_table[aces_table['model'].str.contains('|'.join(temp_model_terms),flags=re.IGNORECASE)]
	# set model
	if not aces_table['model'].empty:
		model = aces_table['model'].unique()[0]
	else:
		raise ValueError('Could not map ' + search_string +". Model not found in ACES")
	# if a trim is in the terms list, set variable
	for trim in temp_model_terms:
		if trim in model_trims:
			model_trim = trim
			break

	# format AHM trims for known differences
	specific_trim_transforms = [
		# min_year, max_year, model, trim match, trim replacement
		[1993, 1993, 'accord', 'anniversary ed\.', '10th anniversary'],
		[1996, 1996, 'accord', 'anniversary ed\.', '25th anniversary edition'],
		[2006, 2006, 'accord', 'se', 'lx special edition'],
		[2017, 2017, 'accord', 'se', 'sport se'],
		[1975, 1979, 'civic', '1500', 'cvcc'],
		[1975, 1979, 'civic', '^\*\*$', 'cvcc'],
		[1980, 1980, 'civic', '^\*\*$', '1500'],
		[1988, 1991, 'civic', '4wd', 'rt 4wd'],
		[2001, 2002, 'mdx', 'prem', 'touring'],
		[1985, 1987, 'prelude', 'si', '2.0 si'],
		[2014, 2014, 'ridgeline', 'rtl-s', 'se'],
		[1999, 2004, 'rl', 'rl', 'premium'],
		[2014, 2014, 'tl', '3\.7', 'sh-awd 3.7'],
	]
	
	for transform_list in specific_trim_transforms:
		if _year >= transform_list[0] and _year <= transform_list[1] and model == transform_list[2]:
			_trim = re.sub(rf"{transform_list[3]}", transform_list[4], _trim)
	
	# Append any trim term we pulled from the model
	if model_trim:
		if not model_trim.lower() in _trim:
			_trim = model_trim + ' ' + _trim
	# strip leading and trailing whitespace
	_trim = _trim.strip()

	# apply generic trim transformations
	trim_transforms = [
		# Remove 2w,4w,aw,etc and everything after it
		['2w.*|4w.*|aw.*',''],
		# Remove single digit in beginning of string if not followed by another number
		['^(\d{1})(?!\d)',''],
		# Special characters
		['a/c',''],
		['([^0-9])\.', '\\1'],
		['[()*]', ''],
		['[\/]', ' '],
		# Separate/swap terms
		['([a-zA-Z]+)(v\d)', '\\1 \\2'],        # Separate the engine config
		['((\d+\.?\d+)?)(([a-zA-Z]+)?)((\d+\.?\d+)?)', '\\3'],  # Separate the engine liter
		# Hybrid
		['(.*)phev', 'plug-in hybrid \\1'],
		['([el]x-?l?|tour(ing)?) hybrid', 'hybrid \\1'],
		['hybrid( nv)? lea?(ther)?', 'hybrid-l'],
		['hy-',''],
		# Special edition
		['(?<!se )special edition', 'special edition'],
		['\\b(spec(ia)?l?)(?!\s+edition)\\b', 'special edition'],
		['\\bed\\b', 'edition'],
		['\\bse\\b', 'se special edition'],
		['lssp', 'ls special edition'],
		# General
		['-w$',''],
		['-p$',''],
		['abs.*',''],
		['2lnr|4lnr',''],
		['2.05', '2.1 '],
		['((dx)-?value|vp)', '\\2 value package'],
		['dx','dx '],
		['vp','value package'],
		['(?<!sh-)awd|shawd', 'sh-awd'],
		['ex-s','ex s'],
		['exlnr|ex-ln|exlt|exln|exlres|exlsns|exls|exl', 'ex-l '],
		['(si)?v-?tec', '\\1 vtec'],
		['(spt|sprt)', 'sport'],
		['(hytour|tourin|tourng|tourrn|tourpx|tournv|tourt|toura|tour2|tourv|trg|trng|trn|tour|tr)', 'touring'],
		['xl2sne|xl4sne','touring'],
		['black', 'black edition'],
		['exnav|exres|exsns|exn|exs','ex'],
		['ex lea?(ther)?', 'ex-l'],
		['(ex)(.*)(?=turbo)', '\\1-t\\2'],
		['ex-?l-t', 'ex-l touring'],
		['ex-t[al]+', 'ex-t'],
		['exlsul', 'ex-l sul'],
		['extsns|ext','ex-t'],
		['gsl', 'gs'],
		['lx-car|lxsns|lxabs|lx&|lx-c','lx'],
		['lxs','lx-s'],
		['lx\+', 'lx-p'],
		['pre', 'premium'],
		['rtl ([et])', 'rtl-\\1 '],
		['rtlsn|rtls','rtl rts'],
		['rtlt','rtl-t'],
		['type-r', 'type r'],
		['r-tour','type r'],
		['van', 'cargo'],
		['wv', 'dx'],
		['elite4','elite'],
		['typsnv|typs|types','type-s '],
		['abac',''],
		['$r',''],
		['^sport','sport '],
		['spose','sport se'],
		['sihptn','si']
	]

	for transform in trim_transforms:
		_trim = re.sub(rf"{transform[0]}", rf"{transform[1]}", _trim)
	
	# strip leading and trailing whitespace
	_trim = _trim.rstrip().lstrip()

	# Match the trim to an ACES trim
	# Match each ACES trim for the model to the trim we have starting with the longest one
	# Each term of the ACES trim must be in the trim in order, but not necessarily adjacent

	valid_trims = aces_table['trim'].unique()
	# sort list by length of item
	valid_trims = sorted(valid_trims, key=len, reverse=True)

	trim_list = []

	for valid_trim in valid_trims:
		if _trim is not None:
			match = valid_trim.lower()
			match_regex = re.compile(rf"\b{match}\b")
			# if entire trim matches, add to list and remove from search string
			if match_regex.match(_trim):
				trim_list.append(valid_trim)
				_trim = _trim.replace(match, '')
			elif match_regex.search(_trim):
				trim_list.append(valid_trim)
	
	if not trim_list:
		if 'Base' in valid_trims:
			trim_list.append('Base')
		else:
			raise ValueError('Could not map ' + search_string + ' missing trim - ' + _trim + ' valid trims: ' + str(valid_trims))

	for trim in trim_list:
		# parse engines from trims, try to assign specific engines. if not, assign ALL engines found for trim
		engines = []
		potential_matches = []

		# Get any engine liter or engine config terms from the trim
		search = re.search(r"(\d\.\d|[lv]\d)", _trim, flags=re.IGNORECASE)
		if search is not None:
			potential_matches.append(search.group(0))

		# Get any engine cylinder terms from the trim and convert them to an engine config
		search = re.search(r"(\d)[ -]?cyl", _trim, flags=re.IGNORECASE)
		if search is not None:
			potential_matches.append("v"+search.group(0))
			potential_matches.append("l"+search.group(0))

		# Get all of the ACES engine for this MMYT
		if trim in [x for x in aces_table['trim'].values]:
			all_engines = aces_table[aces_table['trim'].isin([trim])]['engine'].values
		else:
			all_engines = []

		# If any of the engines matches what we parsed from the trim, keep it
		for match in potential_matches:
			for engine in all_engines:
				if engine in match:
					engines.append(engine)

		# If no engines matched, use all the engines
		if not engines:
			engines = all_engines

		validated_list.append([model, _year, trim, [x for x in engines]])

	# Return the ACES UI values
	return validated_list

def create_rp_car(vehicle_df, honda_aces, acura_aces):

	# create empty datarame
	rp_car = pd.DataFrame(columns=[
		'make_id',
		'car_id',
		'make',
		'model',
		'year',
		'trim',
		'engine',
		'url_make',
		'url_model',
		'url_trim',
		'url_engine',
		'has_parts',
		'has_accessories',
		'external_car_ids',
		'vehicle_class'
	])

	# process vehicles dataframe
	aces_validated_df = pd.DataFrame(columns=['car_id', 'make', 'model', 'year', 'trim', 'engine', 'vin'])
	# loop over available engines from validate_aces function
	log = ''
	# create empty container for car_id to vehicle listings
	vehicle_car_id_dict = {}
	for index, row in vehicle_df.iterrows():
		try:
			if row['make'] == 'Honda':
				aces_list = validate_aces(honda_aces, row['model'], row['year'], row['trim'])
				make_id = 2 # create constant for make_id
			if row['make'] == 'Acura':
				aces_list = validate_aces(acura_aces, row['model'], row['year'], row['trim'])
				make_id = 1 # create constant for make_id
			# if more than one vehicle is returned, duplicate the vehicle to store 2 car_id's
			if len(aces_list) > 1:
				vehicle_df = vehicle_df.append(vehicle_df.loc[index])
				# log vehicles mapped to multiple ACES
				orig_model = str(row['year']) + ' ' + str(row['model']) + ' ' + str(row['trim'])
				mapped_models = str([x[2] for x in aces_list])
				file_path = temp_raw_dir.resolve().parent / "AcesMappedMoreThanOnce.txt"
				file1 = open(file_path, "a+")
				file1.write(orig_model + " - mapped to " + mapped_models + "\n")
				file1.close()
			for aces in aces_list:
				model = aces[0]
				year = aces[1]
				trim = aces[2]
				engines = aces[3]
				for engine in engines:
					# assign string to generate md5 hash
					car_id_string = str(model) + str(year) + str(trim) + str(engine)
					# encode the string to bytes with encode(), then return the hash with hexdigest()
					car_id = hashlib.md5(car_id_string.encode()).hexdigest()
					new_row = {'car_id':car_id, 'make':row['make'], 'model':model, 'year':year, 'trim':trim, 'engine':engine, 'vin':row['vin_serial']}
					aces_validated_df = aces_validated_df.append(new_row, ignore_index=True)
					# update add car_id to list for assignment
					if index in vehicle_car_id_dict:
						vehicle_car_id_dict[index].append(car_id)
					else:
						vehicle_car_id_dict[index] = [car_id]
		except ValueError as err:
			log = log + str(row['origin']) + ' - ' + str(row['make']) + ' - ' + str(err) + '\n'

	# export logs
	log_file_path = temp_raw_dir.resolve().parent / "AcesMapError.txt"
	log_file = open(log_file_path, "a+")
	log_file.write(log)
	log_file.close()

	# reset index on vehicle_df to provide unique index for duplicated rows
	vehicle_df = vehicle_df.reset_index()

	# assign car_id's to vehicles
	for group_name, vehicle_group in vehicle_df.groupby(['index']):
		# search through vehicle_car_id_dict for matching index
		try:
			car_id_list = vehicle_car_id_dict[group_name]
			for row_count, row in enumerate(vehicle_group.iterrows()):
				vehicle_df.at[row[0],'car_id'] = str(car_id_list[row_count])
		# if this fails, there was an ACES mapping error. check the logs
		except:
			continue

	# reset index again for further processing groups
	vehicle_df = vehicle_df.set_index('index')

	# remove duplicated md5 hash. vin/trans/doors/engine code cause duplicates. until we get further ACES data to assign vehicles, these are removed.
	aces_validated_df = aces_validated_df.drop_duplicates()
	# process through and generate the rp_cars df using unique car_id
	aces_rp_car = aces_validated_df.drop_duplicates('car_id')
	for index, row in aces_rp_car.iterrows():
		if row['make'] == 'Honda':
			make_id = 2 # create constant for make_id
		if row['make'] == 'Acura':
			make_id = 1 # create constant for make_id

		new_car = {
			'make_id':make_id,
			'car_id':row['car_id'],
			'make':row['make'],
			'model':row['model'],
			'year':row['year'],
			'trim':row['trim'],
			'engine':row['engine'],
			'url_make':URLifyString(row['make']),
			'url_model':URLifyString(row['model']),
			'url_trim':URLifyString(row['trim']),
			'url_engine':URLifyString(row['engine']),
			'has_parts':0,
			'has_accessories':0,
			'external_car_ids':None,
			'vehicle_class':None
		}

		rp_car = rp_car.append(new_car, ignore_index=True)

	return rp_car, vehicle_df, aces_validated_df

def create_rp_classifications(vehicle_df, part_df, illustration_part_df, illustration_df):

	# create tuple from dataframe for iterating over
	illustration_part_df_values = [(a,b,c,d,e,f) for a,b,c,d,e,f in zip(illustration_part_df['car_id'], illustration_part_df['mpn_stripped'], illustration_part_df['illustration_ref_id'], illustration_part_df['illustration_ref_num'], illustration_part_df['qty_req'], illustration_part_df['region'])]
	# remove duplicates from above list
	illustration_part_df_values = list(set(illustration_part_df_values))
	# create empty container for new rows
	acura_us_parts = []
	acura_can_parts = []
	honda_us_parts = []
	honda_can_parts = []
	# loop through part_df
	for index, row in enumerate(illustration_part_df_values):

		# Get records from other dataframes
		vehicle_record = vehicle_df.loc[[row[0]]] # Provide .loc with a list value and it will always return a dataframe
		part_record = part_df.loc[row[1]]
		illustration_record = illustration_df.loc[row[2]]

		for index, vehicle in vehicle_record.iterrows():

			# assign variables for table generation
			car_id = vehicle.car_id
			mpn_stripped = row[1]
			part_name = part_record.part_name
			db_group = illustration_record.group
			db_subgroup = illustration_record.subgroup
			footnote = vehicle.emissions
			# color, comment, superseded by
			description_elements = []
			try:
				if not pd.isna(part_record.color):
					description_elements.append(str(part_record.color))
			except ValueError:
				print("color error on mpn: " + row[1])
			try:
				if not pd.isna(part_record.comment):
					description_elements.append(str(part_record.comment))
			except ValueError:
				print("comment error on mpn: " + row[1])
			description = ", ".join(description_elements)

			application = None

			position = None
			position_id = None
			url_position = None
			position_list = ['FR.','RR.','R.','L.']
			position_data = " ".join([x.strip() for x in position_list if (x in part_name)])
			if position_data:
				position_map = get_position_id(position_data)
				if position_map:
					position = position_map[0]
					position_id = position_map[1]
					url_position = URLifyString(position)

			# get position from part_name
			illustration_code = illustration_record.id
			art_callout_id = row[3]
			url_db_group = URLifyString(db_group)
			url_db_subgroup = URLifyString(db_subgroup)
			url_part_name = URLifyString(part_name)
			url_part_number = URLifyString(mpn_stripped)
			msrp = None
			quantity = row[4]
			part_term = None
			if db_group == 'ACCESSORIES':
				catalog_type = 'ACCESSORIES'
				catalog_subtype = 'ACCESSORIES'
			else:
				catalog_type = 'PARTS'
				catalog_subtype = 'PARTS'

			new_part = [
					car_id,
					mpn_stripped,
					part_name,
					db_group,
					db_subgroup,
					footnote,
					description,
					application,
					position,
					illustration_code,
					art_callout_id,
					url_db_group,
					url_db_subgroup,
					url_part_name,
					url_part_number,
					msrp,
					quantity,
					part_term,
					catalog_type,
					position_id,
					url_position,
					catalog_subtype
			]

			if str(row[5]).startswith('U'):
				if vehicle.make == 'Honda':
					honda_us_parts.append(new_part)
				if vehicle.make == 'Acura':
					acura_us_parts.append(new_part)
			if str(row[5]).startswith('C'):
				if vehicle.make == 'Honda':
					honda_can_parts.append(new_part)
				if vehicle.make == 'Acura':
					acura_can_parts.append(new_part)
	# TODO: ask what to do about supersession data and discontinued flag

	classification_columns = ['car_id',
		'part_number_stripped',
		'part_name',
		'db_group',
		'db_subgroup',
		'footnote',
		'description',
		'application',
		'position',
		'illustration_code',
		'art_callout_id',
		'url_db_group',
		'url_db_subgroup',
		'url_part_name',
		'url_part_number',
		'mrsp',
		'quantity',
		'part_terminology',
		'catalog_type',
		'position_id',
		'url_position',
		'catalog_subtype']

	classification_honda_us = pd.DataFrame(honda_us_parts, columns=classification_columns)

	classification_honda_can = pd.DataFrame(honda_can_parts, columns=classification_columns)

	classification_acura_us = pd.DataFrame(acura_us_parts, columns=classification_columns)

	classification_acura_can = pd.DataFrame(acura_can_parts, columns=classification_columns)

	return classification_honda_us, classification_honda_can, classification_acura_us, classification_acura_can

def create_rp_illustration_info(illustration_df):
	# create empty dataframe
	rp_illustration_info = pd.DataFrame(columns=[
		'ImageID',
		'width',
		'height',
		'source',
		'orig_width',
		'orig_height',
		'part_source',
		'base_name',
		'file_checksum',
		'processed_file_checksum'
	])

	# get image list
	image_list = [(x, y, z) for x, y, z in zip(illustration_df['id'], illustration_df['location'], illustration_df['make'])]
	image_id_list = [x[0] for x in image_list]

	# get existing image checksums
	existing_images = get_illustration_info_table(image_id_list)

	# loop through images
	for image in image_list:

		with open(image[1], 'rb') as file:
			img = Image.open(file)
			# set file data for resizing
			image_width, image_height = img.size
			image_ratio = image_width / image_height

			# calculate checksum of file
			new_file_checksum = hashlib.md5(img.tobytes()).hexdigest()

			# If the checksum is the same as the previously processed imageID, skip it
			image_record = existing_images.loc[existing_images['imageID'] == image[0]]
			# if results returned
			if not image_record.empty:
				# grab the first row in the dataframe
				if image_record.iloc[0].file_checksum == new_file_checksum:
					continue

			# transform image into multiple sizes if file checksum is different
			for size in image_sizes:
				new_width = image_sizes[size]
				new_size = (new_width,int(round(new_width/image_ratio)))
				new_img = img.resize(new_size, resample=Image.LANCZOS)
				new_img_checksum = hashlib.md5(new_img.tobytes()).hexdigest()
				# export new image file to directory
				# save the image name with the following data, seaprated by underscore, to import into PHP for s3 upload: image_id, image_source, part_source, image_size (width), processed_file_checksum
				img_name = image[0] + '_ahm_ahm_' +  str(image_sizes[size]) + '_' + new_img_checksum +'.png'
				file_path = processed_image_dir / img_name
				# Need to convert to RGB since tiff uses CYMK
				new_img.convert("RGB")
				new_img.save(str(file_path), format="png")

				if image_sizes[size] == 485:
					new_illust = {
						'ImageID':image[0],
						'file_checksum':new_file_checksum,
						'processed_file_checksum':new_img_checksum,
						'width':new_size[0],
						'height':new_size[1],
						'orig_width':image_width,
						'orig_height':image_height,
						'part_source':image[2],
						'base_name':None,
						'source':10}
					rp_illustration_info = rp_illustration_info.append(new_illust, ignore_index=True)

				# close new image
				new_img.close()
			# make sure we close the original image
			img.close()
		file.close()

	return rp_illustration_info

def create_rp_vin_masks(aces_validated_df):

	# create empty dataframe
	rp_vin_masks = pd.DataFrame(columns=[
		'vin_type',
		'vin_mask',
		'car_id'
	])

	# pull the 2 columns we need from the dataframe using list comprehesion (faster than looping over the dataframe)
	vehicle_vin_list = [(x, y) for x, y in zip(aces_validated_df['car_id'], aces_validated_df['vin'])]

	for vehicle in vehicle_vin_list:
		new_vin_mask = {'vin_type':'2', 'vin_mask':vehicle[1], 'car_id':vehicle[0]}
		rp_vin_masks = rp_vin_masks.append(new_vin_mask, ignore_index=True)

	return rp_vin_masks

def process_rp_car(rp_car, class_honda_us, class_honda_can, class_acura_us, class_acura_can):

	# comebine all class tables
	class_list = [class_honda_us, class_honda_can, class_acura_us, class_acura_can]
	class_all = pd.concat(class_list)

	for car in rp_car['car_id']:
		# search car_id in classification tables for US fitments
		if car in class_all['car_id'].values:
			# search for rows with this car_id and a db_group that has "accessories"
			if not class_all[(class_all['db_group'] == 'ACCESSORIES') & (class_all['car_id'] == car)].empty:
				rp_car.loc[rp_car['car_id'] == car, 'has_accessories'] = 1
			if not class_all[(class_all['db_group'] != 'ACCESSORIES') & (class_all['car_id'] == car)].empty:
				rp_car.loc[rp_car['car_id'] == car, 'has_parts'] = 1
	return rp_car

def parse_raw_ahm_catalog(file):

	# create empty dataframes
	catalog_df = pd.DataFrame(columns=['id','desc','manufacturer'])
	illustration_df = pd.DataFrame(columns=['id','ref_id','make','group','subgroup','location'])
	vehicle_df = pd.DataFrame(columns=['car_id','make','model','year','trans','doors','emissions','trim','origin','vin_serial','vin_serial_from','vin_serial_to','eng_serial_type','trans_serial_type'])
	part_df = pd.DataFrame(columns=['mpn_stripped','mpn','manufacturer','part_name', 'color', 'comment','superseded_by','discontinued'])

	# Source file
	with open(file) as catalog_file:
		# global variables for line-down file processing
		record_30_block_num = None
		# record_30_ref_no = None
		part_name = None
		part_color = None
		part_comment = None
		part_super = None
		grade_definitions = {}
		new_illustration_part_list = []
		# iterate through rows in source file
		for row in catalog_file:
			# print(row)
			# get record type
			row_type = row[11:13]
			row_catalog_id = row[3:11].rstrip()
			# filter data definitions based on row type and apply slices, exporting new row to destination file
			if row_type == '10':
				fields = [row[slice].rstrip() for slice in generate_slices(record_10_widths)]
				row_catalog_id = fields[1]      # catalog_df.catalog_id
				catalog_name = fields[3]    # catalog_df.catalog_desc, vehicle_df.model (after stripping year info)
				product_div = fields[4]     # catalog_df.division, vehicle_df.make (A = Honda, B = Acura)
				if product_div == 'A':
					vehicle_make = 'Honda'
				elif product_div == 'B':
					vehicle_make = 'Acura'
				else:
					vehicle_make = None
				print(vehicle_make)
				print(catalog_name)
				if vehicle_make != 'Acura':
					raise ValueError('This is not an Acura Catalog')
				vehicle_model = catalog_name.split("'", 1)[0].replace('4D','').replace('5D','').replace('3D','').replace('/','').rstrip()
				# catalog_df dataframe
				new_catalog = {'id':row_catalog_id, 'desc':catalog_name, 'manufacturer':vehicle_make}
				catalog_df = catalog_df.append(new_catalog, ignore_index=True)
				# print(catalog_name)
			# elif row_type == '15': # Do not need
			# 	fields = [row[slice].rstrip() for slice in generate_slices(record_15_widths)]
				# color_id = fields[3]    # color_info.color_id
				# color_desc = fields[4]  # color_info.color_desc
			# process row 20 for grade definitions (LX2W = 'A')
			elif row_type == '20':
				fields = [row[slice].rstrip() for slice in generate_slices(record_20_widths)]
				# valid years
				valid_years = []
				for x in range(3,13): # 5
					valid_years.append(fields[x])
				valid_years = [x for x in valid_years if x]
				# print(valid_years)
				# trim definitions
				grades = []
				for x in range(57,83): # 5
					grades.append(fields[x])
				grades = [x for x in grades if x]
				# assign grade definitions
				for grade in grades:
					definition = grade[6].rstrip()
					# print(grade)
					grade = grade[0:6].rstrip().replace('*','').replace('$','').replace('#','').replace('?','').replace('%','')
					if not grade in grade_definitions:
						grade_definitions.update({definition: grade})
					else:
						grade_definitions[definition] = grade
			elif row_type == '25':
				fields = [row[slice].rstrip() for slice in generate_slices(record_25_widths)]
				year = int([x for x in valid_years if x.endswith(fields[3])][0])                # vehicle_df.year
				if year <= 40: # this should be the number where you think it stops to be 20xx (like 15 for 2015; for every number after that it will be 19xx)
					year = year + 2000
				else:
					year = year + 1900
				door = fields[4]                # vehicle_df.doors
				area = fields[5]                # vehicle_df.area
				transmission = fields[6]        # vehicle_df.trans
				origin = fields[7]              # vehicle_df.origin
				grade = fields[8].replace('*','').replace('$','').replace('#','').replace('?','').replace('%','') # vehicle_df.grade
				model_serial = fields[9]        # vehicle_df.vin_serial
				model_serial_low = fields[10]   # vehicle_df.vin_serial_low
				model_serial_high = fields[11]  # vehicle_df.vin_serial_high
				engine_serial = fields[12]      # vehicle_df.eng_serial_type
				# engine_serial_low = fields[13]  # vehicle_df.eng_serial_low
				# engine_serial_high = fields[14] # vehicle_df.eng_serial_high
				trans_serial = fields[15]       # vehicle_df.trans_serial_type
				# trans_serial_low = fields[16]   # vehicle_df.trans_serial_low
				# trans_serial_high = fields[17]  # vehicle_df.trans_serial_high
				# vehicle dataframe
				# grab everything that begins with U or C for north american markets (usa, can)
				search = re.search(r"(U|C)", origin, flags=re.IGNORECASE)
				if search is not None:
					new_vehicle = {'make':vehicle_make,'model':vehicle_model,'year':year,'trans':transmission,'doors':door,'emissions':area,'trim':grade,'origin':origin,'vin_serial':model_serial,'vin_serial_from':model_serial_low,'vin_serial_to':model_serial_high,'eng_serial_type':engine_serial,'trans_serial_type':trans_serial}
					vehicle_df = vehicle_df.append(new_vehicle, ignore_index=True)
			elif row_type == '30':
				fields = [row[slice].rstrip() for slice in generate_slices(record_30_widths)]
				section_id = fields[3]
				block_num = fields[4]
				# check if new block
				if block_num != record_30_block_num:
					fields = [row[slice].rstrip() for slice in generate_slices(record_30_header_widths)]
					# process header record
					record_30_block_num = block_num     # illustration_df.ref_id
					# block_seq = fields[5]
					# block_pre_id = fields[6]
					block_desc = fields[7]              # illustration_df.name
					print(block_desc)
					# block_suf_id = fields[8]
					illustration_id = fields[9].split(' ')[0]         # illustration_df.id
					# if 13 chracters, strip last character for check character
					if len(illustration_id) == 13:
						illustration_id = illustration_id[:-1]
					# catalog_block_num_2 = fields[10]
					# catalog_section_id = fields[11]
					# catalog_block_chart_id = fields[12]
					# illustration_df.location is generated from catalog_df.id and illustration_df.id
					illustration_location = str(image_dir) + '/' + str(row_catalog_id).lower() + '/illust/' + illustration_id + '.tif' # illustration_df.location
					# illustrations dataframe entry
					if record_30_block_num != None:
						new_illustration = {'id':illustration_id,'ref_id':record_30_block_num,'make':vehicle_make,'group':group_definitions[section_id],'subgroup':block_desc,'location':illustration_location}
						illustration_df = illustration_df.append(new_illustration, ignore_index=True)
					# print(block_desc)
				# if record in existing block
				elif block_num == record_30_block_num:
					# process body record into variables
					illustration_ref_num = fields[5] # block
					# illustration_ref_num_ext = fields[6] # not used in file
					# illustration_row_seq = fields[7] # block items
					line_desc = fields[8].replace('(','').replace(')','')
					line_desc_cont = fields[9]

					years_1_5 = []
					for x in range(10,15): # 5
						years_1_5.append(fields[x])
					years_1_5 = [x for x in years_1_5 if x]

					years_6_10 = []
					for x in range(15,20): # 5
						years_6_10.append(fields[x])
					years_6_10 = [x for x in years_6_10 if x]

					years = years_1_5 + years_6_10

					transmissions = []
					for x in range(20,30): # 10
						transmissions.append(fields[x])
					transmissions = [x for x in transmissions if x]

					areas = []
					for x in range(30,54): # 24
						areas.append(fields[x])
					areas = [x for x in areas if x]

					serial_num_from = fields[54]
					# fields[55] is a filler
					serial_num_to = fields[56]

					doors = []
					for x in range(57,67): # 10
						doors.append(fields[x])
					doors = [x for x in doors if x]

					grades = []
					for x in range(67,93): # 26
						grades.append(fields[x])
					grades = [x for x in grades if x]

					origins = []
					for x in range(93,103): # 10
						origins.append(fields[x])
					origins = [x for x in origins if x]

					qty_req =  fields[103].lstrip()
					part_num = fields[104]

					# process line for entry into illustrations_part_df and part_df dataframes
					if line_desc_cont == '01':
						part_name = line_desc
					elif line_desc_cont == '03':
						part_color = line_desc
					elif line_desc_cont == '05':
						part_comment = line_desc
					elif line_desc_cont == '07':
						if 'NA USE ALT:' in line_desc:
							# TODO: parse out the superseded mpn from line_desc
							part_super = line_desc
						elif 'NOT AVAILABLE' in line_desc:
							part_disc = 1

					# insert into part_df if part_num does not already exist
					if not (part_df['mpn'] == part_num).any():
						# if the part does not exist, clear identifying info from last part
						part_color = None
						part_comment = None
						part_super = None
						part_disc = None
						# strip the part number
						mpn_stripped = part_num.replace('-','').replace(' ','').replace('.','')
						mpn_stripped = [x for x in mpn_stripped]
						for x in range(len(mpn_stripped)):
							mpn_stripped[x] = mpn_stripped[x].lower()
						mpn_stripped = ''.join(mpn_stripped)
						# create the new part record
						new_part = {'mpn_stripped':mpn_stripped, 'mpn':part_num, 'manufacturer':vehicle_make, 'part_name':part_name, 'color':part_color, 'comment':part_comment, 'superseded_by':part_super, 'discontinued':part_disc}
						# print('creating new part')
						part_df = part_df.append(new_part, ignore_index=True)
					# assign color to matching mpns that were processed before the color row
					if part_color != None:
						part_df.loc[part_df.mpn_stripped == mpn_stripped, 'color'] = part_color
						# print("setting color on " + mpn_stripped)
					# assign comments to matching mpns that were processed before the comment row
					if part_comment != None:
						part_df.loc[part_df.mpn_stripped == mpn_stripped, 'comment'] = part_comment
						# print("setting comment on " + mpn_stripped)
					# assign comments to matching mpns that were processed before the comment row
					if part_super != None:
						part_df.loc[part_df.mpn_stripped == mpn_stripped, 'superseded_by'] = part_super
						# print("setting supersedence on " + mpn_stripped)
					# assign comments to matching mpns that were processed before the comment row
					if part_disc != None:
						part_df.loc[part_df.mpn_stripped == mpn_stripped, 'discontinued'] = part_disc
						# print("setting discontinued on " + mpn_stripped)
					# insert into illustration_part_df
					# one record for every unique option, loop through all lists
					for year in years:
						for transmission in transmissions:
							for area in areas:
								for door in doors:
									for grade in grades:
										for origin in origins:
											# grab everything that begins with U or C for north american markets (usa, can)
											search = re.search(r"(U|C)", origin, flags=re.IGNORECASE)
											if search is not None:
												# get valid year option
												year = [x for x in valid_years if x.endswith(year)][0]
												if int(year) <= 40: # this should be the number where you think it stops to be 20xx (like 15 for 2015; for every number after that it will be 19xx)
													search_year = int(year) + 2000
												else:
													search_year = int(year) + 1900
												# get vehicle from options
												vehicle = vehicle_df[(vehicle_df['make'] == vehicle_make) & (vehicle_df['model'] == vehicle_model) & (vehicle_df['year'] == search_year) & (vehicle_df['doors'] == door) & (vehicle_df['trans'] == transmission) & (vehicle_df['emissions'] == area) & (vehicle_df['trim'] == grade_definitions[grade]) & (vehicle_df['origin'] == origin)]
												vehicle = vehicle.index.values[0]
												new_illustration_part = [
													mpn_stripped,
													record_30_block_num,
													illustration_ref_num,
													vehicle,
													qty_req,
													serial_num_from,
													serial_num_to,
													origin
												]
												new_illustration_part_list.append(new_illustration_part)
	
	illustration_part_df = pd.DataFrame(new_illustration_part_list, columns=['mpn_stripped','illustration_ref_id','illustration_ref_num','car_id','qty_req','vin_serial_from','vin_serial_to','region'])
	# remove any duplicates from the dataframes, set indices if required
	catalog_df = catalog_df.drop_duplicates().dropna(how='all',axis=0)
	illustration_df = illustration_df.drop_duplicates().dropna(how='all',axis=0).set_index('ref_id')
	vehicle_df = vehicle_df.drop_duplicates().dropna(how='all',axis=0)
	part_df = part_df.set_index('mpn_stripped')
	part_df = part_df[~part_df.index.duplicated(keep='first')].dropna(how='all',axis=0)
	illustration_part_df = illustration_part_df.drop_duplicates().dropna(how='all',axis=0)

	# set car_id to string to hold MD5 hash
	vehicle_df.car_id = vehicle_df.car_id.astype(str)

	# clean up part_df for disabled regions
	# get row indexes of mpn's in illustration_part_df
	mpn_list = illustration_part_df['mpn_stripped'].tolist()
	part_df = part_df.loc[part_df.index.intersection(mpn_list)]

	return catalog_df, illustration_df, vehicle_df, part_df, illustration_part_df

def export_blank_hotspot_dataframe():
	with open(processed_illustrations_dir / 'rp_illustration_hotspots.csv', "w") as f:
		rp_illustration_hotspots = pd.DataFrame(columns=['imageID','referenceCode','x','y','width','height','part_source','filename','original_image_width','original_image_height','width_to_height_ratio','processed_file_checksum'])
		rp_illustration_hotspots.to_csv(f, encoding='utf-8', index=False)

def export_intermediate_df_to_csv(catalog_id, illustration_df, vehicle_df, part_df, illustration_part_df):

	intermediate_dir = pathlib.Path(base_dir / "working/intermediate/")
	try:
		intermediate_dir.mkdir(parents=True, exist_ok=False)
	except FileExistsError:
		pass

	with open(intermediate_dir / (str(catalog_id) + '_IllustrationInfo.csv'), "w") as f:
		illustration_df.to_csv(f, encoding='utf-8')
	with open(intermediate_dir / (str(catalog_id) + '_VehicleInfo.csv'), "w") as f:
		vehicle_df.to_csv(f, encoding='utf-8')
	with open(intermediate_dir / (str(catalog_id) + '_PartInfo.csv'), "w") as f:
		part_df.to_csv(f, encoding='utf-8')
	with open(intermediate_dir / (str(catalog_id) + '_IllustrationPartInfo.csv'), "w") as f:
		illustration_part_df.to_csv(f, encoding='utf-8')

def export_final_dataframes_to_csv(catalog_id, rp_car, class_honda_us, class_honda_can, class_acura_us, class_acura_can, rp_illustration_info, rp_vin_masks):
	with open(processed_dir / (str(catalog_id) + '_rp_car.csv'), "w") as f:
		rp_car.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / (str(catalog_id) + '_classification_honda.csv'), "w") as f:
		class_honda_us.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / (str(catalog_id) + '_classification_honda_ca.csv'), "w") as f:
		class_honda_can.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / (str(catalog_id) + '_classification_acura.csv'), "w") as f:
		class_acura_us.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / (str(catalog_id) + '_classification_acura_ca.csv'), "w") as f:
		class_acura_can.to_csv(f, encoding='utf-8', index=False)
	with open(processed_illustrations_dir / (str(catalog_id) + '_rp_illustration_info.csv'), "w") as f:
		rp_illustration_info.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / (str(catalog_id) + '_rp_vin_masks.csv'), "w") as f:
		rp_vin_masks.to_csv(f, encoding='utf-8', index=False)

def combine_final_dataframes():

	# create empty filename lists
	rp_car_files = []
	class_files_honda = []
	class_files_honda_ca = []
	class_files_acura = []
	class_files_acura_ca = []
	rp_vin_masks_files = []
	rp_illust_files = []

	# sort file names into lists
	for path in processed_dir.iterdir():
		if path.is_file() and path.name.endswith('rp_car.csv'):
			rp_car_files.append(path)
		if path.is_file() and path.name.endswith('classification_honda.csv'):
			class_files_honda.append(path)
		if path.is_file() and path.name.endswith('classification_honda_ca.csv'):
			class_files_honda_ca.append(path)
		if path.is_file() and path.name.endswith('classification_acura.csv'):
			class_files_acura.append(path)
		if path.is_file() and path.name.endswith('classification_acura_ca.csv'):
			class_files_acura_ca.append(path)
		if path.is_file() and path.name.endswith('rp_vin_masks.csv'):
			rp_vin_masks_files.append(path)
	
	for path in processed_illustrations_dir.iterdir():
		if path.is_file() and path.name.endswith('rp_illustration_info.csv'):
			rp_illust_files.append(path)

	#combine all files in the list
	rp_car_combined_csv = pd.concat([pd.read_csv(f) for f in rp_car_files])
	rp_illust_combined_csv = pd.concat([pd.read_csv(f) for f in rp_illust_files])
	rp_vin_masks_combined_csv = pd.concat([pd.read_csv(f) for f in rp_vin_masks_files])
	class_honda_combined_csv = pd.concat([pd.read_csv(f) for f in class_files_honda])
	class_honda_ca_combined_csv = pd.concat([pd.read_csv(f) for f in class_files_honda])
	class_acura_combined_csv = pd.concat([pd.read_csv(f) for f in class_files_acura])
	class_acura_ca_combined_csv = pd.concat([pd.read_csv(f) for f in class_files_acura_ca])

	# remove all files in processed directory
	for path in processed_dir.iterdir():
		try:
			path.unlink()
		except:
			pass
	
	for path in processed_illustrations_dir.iterdir():
		try:
			path.unlink()
		except:
			pass

	#export to new files to csv
	with open(processed_dir / 'rp_car.csv', "w") as f:
		rp_car_combined_csv.to_csv(f, encoding='utf-8', index=False)
	with open(processed_illustrations_dir / 'rp_illustration_info.csv', "w") as f:
		rp_illust_combined_csv.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / 'rp_vin_masks.csv', "w") as f:
		rp_vin_masks_combined_csv.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / 'classification_honda.csv', "w") as f:
		class_honda_combined_csv.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / 'classification_honda_ca.csv', "w") as f:
		class_honda_ca_combined_csv.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / 'classification_acura.csv', "w") as f:
		class_acura_combined_csv.to_csv(f, encoding='utf-8', index=False)
	with open(processed_dir / 'classification_acura_ca.csv', "w") as f:
		class_acura_ca_combined_csv.to_csv(f, encoding='utf-8', index=False)

if __name__ == "__main__":

	# get aces tables to pass to rp_car function
	honda_aces = get_aces_table('honda')
	acura_aces = get_aces_table('acura')

	for path in temp_raw_dir.iterdir():
		if path.is_file() and path.suffix == '.txt' and path.name == '13SD201.txt':
			print('found file')
			
			try:
				catalog_df, illustration_df, vehicle_df, part_df, illustration_part_df = parse_raw_ahm_catalog(path)
				# if there are no valid parts, don't continue processing
				if part_df.empty:
					print('found empty part df')
					continue

				catalog_id = catalog_df.iloc[0].id
				# export_intermediate_df_to_csv(catalog_id, illustration_df, vehicle_df, part_df, illustration_part_df)
				rp_car, vehicle_df, aces_validated_df = create_rp_car(vehicle_df, honda_aces, acura_aces)

				rp_vin_mask = create_rp_vin_masks(aces_validated_df)

				rp_illustration_info = create_rp_illustration_info(illustration_df)
				
				class_honda_us, class_honda_can, class_acura_us, class_acura_can = create_rp_classifications(vehicle_df, part_df, illustration_part_df, illustration_df)

				# process has_accessories, has_parts
				rp_car = process_rp_car(rp_car, class_honda_us, class_honda_can, class_acura_us, class_acura_can)

				export_final_dataframes_to_csv(catalog_id, rp_car,class_honda_us, class_honda_can, class_acura_us, class_acura_can, rp_illustration_info, rp_vin_mask)
			except:
				
				print('error')
				continue


	combine_final_dataframes()
	export_blank_hotspot_dataframe()
