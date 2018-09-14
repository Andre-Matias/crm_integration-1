from load_resources import *
import sys
import simplejson as json
from datetime import date

conf_file = sys.argv[1]
source_conf_file = sys.argv[2]
target_conf_file = sys.argv[3]

data = json.load(open(conf_file))
resources = data['resources_fulldump_load'].split(',')

copyToAnotherRedshift(source_conf_file,target_conf_file,resources)