


def s3_fulldump_deals(client,keyId,sKeyId)
	#Iterate for everypage returned by the API
	aux = 1

	while 1:

		print("Downloading page #" + str(aux))
		
		data = client.deals.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else: return 1

		#Write on local gz file
		print("Writing file #" + str(aux))	
		output = gzip.open("/home/ubuntu/Reports/latam_deals_" + str(aux).zfill(10) + ".txt.gz", 'wb')

		data = convert_timestamps(data)

		#Iterate the list of deals
		for deal_data in data:
			output.write(json.dumps(deal_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		print("Uploading to S3")
		localName = "/home/ubuntu/Reports/latam_deals_" + str(aux).zfill(10) + ".txt.gz"
		fileName="latam_deals_" + str(aux).zfill(10) + ".txt.gz"
		full_key_name = os.path.join(path, fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1


