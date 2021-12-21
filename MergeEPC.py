import sys
import pathlib
import shutil
import os

# get command line argument. skipping the first one (MergeAhmEpc.py --temp-dir=/tmp/32145)
args = sys.argv[1:]

# parse out arguments and assign a base directory
for arg in args:
	arg = arg.split('=')
	if arg[0] == '--temp-dir':
		# set base_dir for all working files
		base_dir = pathlib.Path(arg[1])

def get_raw_file():
	ahm_dir = base_dir / 'ahm/data/'
	for path in ahm_dir.iterdir():
		if path.name.startswith('autodata.') and path.is_dir():
			for file_path in path.iterdir():
				if file_path.name.startswith('autodata.'):
					return file_path

def get_data_dir():

	data_dir = base_dir / 'ahm/data/'

	return data_dir

def create_dir(dir):
	dir = base_dir / dir
	try:
		dir.mkdir(parents=True, exist_ok=False)
	except FileExistsError:
		pass

	return dir

def parse_images(data_dir, working_dir):

	for path in data_dir.iterdir():
		if path.is_dir() and path.name.startswith('autodata'):
			for catalog in path.iterdir():
				if catalog.is_dir():
					print(catalog.name)
					# try to move the folder, if it fails, it already exists
					try:
						shutil.move(str(catalog), str(working_dir))
					except:
						# if it fails, remove the directory and then move the new files
						shutil.rmtree(str(working_dir)+'/'+catalog.name)
						shutil.move(str(catalog), str(working_dir))

def parse_raw_ahm_complete_file(file, working_dir):

	# Source file
	with open(file, encoding='latin-1') as catalog_file:
		# iterate through rows in source file
		for row in catalog_file:
			row_catalog_id = row[3:11].rstrip()
			# export catalog rows to file
			file_name = row_catalog_id + ".txt"
			file_path = working_dir / file_name
			file = open(file_path, "a+")  # append mode
			file.write(row)
			file.close()

if __name__ == "__main__":
	# get new raw file from downloaded zip
	file_path = get_raw_file()
	print(file_path)
	# create working directories
	working_raw = create_dir('working/raw')
	working_complete = create_dir('working/complete')
	# process raw file into catalogs
	parse_raw_ahm_complete_file(file_path, working_raw)
	# merge files into complete directory
	for path in working_raw.iterdir():
		if path.is_file():
			shutil.move(os.path.join(working_raw, path.name), os.path.join(working_complete, path.name))

	# process image directories into a new folder for zip/FPSM upload
	working_image = create_dir('working/image')
	data_dir = get_data_dir()
	parse_images(data_dir, working_image)