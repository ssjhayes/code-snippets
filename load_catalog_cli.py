from load_catalog import LoadCatalog
from core.service.load_config_service import LoadConfigService
from core.service.job_properties_service import JobPropertiesService
import argparse, logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

arg_parser = argparse.ArgumentParser(description='Load PIES File')
arg_parser.add_argument('--load-config-file', required=True,  help='Load Config File')
arg_parser.add_argument('--protected-file', required=True, help='Protected File')
arg_parser.add_argument('--topo-file', required=True, help='Topology File')
arg_parser.add_argument('--temp_dir', help='Temporary directory used by batch system')
arg_parser.add_argument('--file_path', help='ZIP folder containing catalog and images. images must be in images/ sub-directory')
arg_parser.add_argument('--s3_path', help='S3 Path to recursively process files')

"""
Load PIES File command line interface

Sample command
load_pies_cli.py \
--load-config-file catv2_loader/load_defs/exide.yaml \
--protected-file tests/config/dev.env \
--topo-file tests/config/topo.env \
--file tests/config/pies10.xml

"""

if __name__ == "__main__":
	args = arg_parser.parse_args()
	load_catalog = LoadCatalog(
		job_properties_service=JobPropertiesService(protected_file_name=args.protected_file, topo_file_name=args.topo_file),
		load_config_service=LoadConfigService(file_name=args.load_config_file),
		temp_directory=args.temp_dir
		)

	load_catalog.load_catalog(args.file_path)
