import requests
import simplejson as json
import time
import datetime
import boto
from boto.s3.key import Key
import os
import basecrm
import contextlib
import math
import glob
import sys
import gzip
import base64

#get gzip files
conf_file=sys.argv[1] 

#global variables :: token, country, category, access to the s3 , i (i: is to navigate the list of resources)
with open(conf_file) as json_data:
    d = json.load(json_data)
	
lista= d['resources_firehose'].split(',')
print(lista)
listatoken= d['token_list_firehose'].split(',')
i=0
keyId=d['s3_key']
sKeyId=d['s3_skey']


var_s3_data_path_sync=d['s3_data_path_sync']
print(var_s3_data_path_sync)

bucketName=d['bucket_name']
print(bucketName)

def method_convert_custom_fields(custom_field_values):
	convert_json = {}
	for custom_fld in custom_field_values:
		name = (custom_fld['custom_field']['data']['name'])
		if (isinstance(custom_fld['value'],list)):
			aux_array = []
			for list_element in custom_fld['value']:
				aux_array.append(list_element['name'])
			convert_json[name] = aux_array  
		if (isinstance(custom_fld['value'],dict)):
			if('name' in custom_fld['value']):
				convert_json[name] = custom_fld['value']['name']
			else:
				convert_json[name] = ""
		else:
			convert_json[name] = custom_fld['value']
	return convert_json
	
def method_convert_tags(tags):
    convert_tag = []
    for tag in tags:
        convert_tag.append(tag['data']['name'])
    return convert_tag

# get api data
def sub_getDataApi (listatoken, lista, i, var_token, var_category, var_country, var_subject):
				rows=0
				qty_pages=0
				qty_files=0
				rows_per_page=3000
				f=1
				startingPosition = 'tail'
				print(var_subject)
				while f==1:
						onTop = False
						while not onTop:
							#print(var_subject)
							url = "https://api.getbase.com/v3/"+str(var_subject)+"/stream"
							response = requests.get(url,
									params={'position': startingPosition},
									headers={'Authorization':'Bearer {}'.format(var_token)}, timeout=2000)
							if response.status_code != 200:
								raise Exception('Request failed with {}'
								.format(response.status_code))
							for item in response.json()['items']:		
							  qty_pages = math.ceil( float(rows)  /  float(rows_per_page))
							  rows=rows+1 	  
							  thefile="firehose_"+str(var_country)+"_"+str(var_category)+"_"+str(var_subject)+str(qty_pages)+".txt.gz"
							  if 'custom_field_values' in item['data']:
							    item['data']['custom_field'] = method_convert_custom_fields(item['data']['custom_field_values'])
							  if 'tags' in item['data']:
							    item['data']['tags'] = method_convert_tags(item['data']['tags'])
							  file = (gzip.open(thefile, mode="a"))
							  file.write(json.dumps(item, indent=4).encode('utf-8'))
							  file.close()
							onTop = response.json()['meta']['top']
							startingPosition = response.json()['meta']['position']
							if onTop == True:
								f=0
				return[qty_pages,var_subject,rows]
				print("done sub_getDataApi")
				print(var_subject)			
def sub_moveToS3 (qty_pages, rows, var_category, var_country, var_subject, i, keyId, sKeyId, var_s3_data_path_sync, bucketName):
		while qty_pages>=0 and rows>0 and i<9:
			diayhora=('{:%Y%m%d}'.format(datetime.datetime.now()))
			fileName="firehose_"+str(var_country)+"_"+str(var_category)+"_"+str(var_subject)+"_"+str(qty_pages)+"_"+diayhora+".txt.gz"
			anio=('{:%Y}'.format(datetime.datetime.now()))
			mes=('{:%m}'.format(datetime.datetime.now()))
			dia=('{:%d}'.format(datetime.datetime.now()))
			path = str(var_s3_data_path_sync)+str(var_subject)+"/"+anio+"/"+mes+"/"+dia+"/"
			full_key_name = os.path.join(path, fileName)
			conn = boto.connect_s3(keyId,sKeyId)
			bucket = conn.get_bucket(bucketName)
			k = bucket.new_key(full_key_name)
			k.key=full_key_name
			thefile2="firehose_"+str(var_country)+"_"+str(var_category)+"_"+str(var_subject)+str(qty_pages)+".txt.gz"
			k.set_contents_from_filename(thefile2)			
			os.remove(thefile2)
			qty_pages=qty_pages-1
			print("done delete_local_files")
		print("done sub_moveToS3")
#main process
def main_sourceToS3 (listatoken, lista, i, var_s3_data_path_sync):
	var_token=listatoken[0]
	var_category=listatoken[1]
	var_country=listatoken[2]
	for object_in_lista in lista:
		var_subject=str(lista[i])
		list_return_sub_getDataApi = sub_getDataApi(listatoken, lista, i, var_token, var_category, var_country, var_subject)	
		sub_moveToS3(list_return_sub_getDataApi[0], list_return_sub_getDataApi[2], var_category, var_country, list_return_sub_getDataApi[1] , i, keyId, sKeyId, var_s3_data_path_sync, bucketName)
		print("done subject_sourceToS3")
		print(var_subject)
		i=i+1
	print("done main_sourceToS3")
	print(var_subject)
	
#execute this	
main_sourceToS3 (listatoken, lista, i, var_s3_data_path_sync)