import sys
import simplejson as json

arg_one = sys.argv[1]
arg_two = sys.argv[2]

data_one = json.load(open(arg_one))
data_two = json.load(open(arg_two))
print(data_one['country'])
print(data_two['dbname'])

print('All done')