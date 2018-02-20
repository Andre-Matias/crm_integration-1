from connections.connectors import *
import gzip
from datetime import timedelta
from datetime import datetime

def getDates():
	dt = datetime(2017, 7, 1)
	end = datetime.now()
	step = timedelta(days=1)

	result = []

	while dt < end:
		result.append(dt.strftime('%Y/%m/%d'))
		dt += step
	return result


def evaluateS3(conf):
	s3 = hydraS3('live-temp','hydra_verticals',conf['hydra_s3_key'],conf['hydra_s3_skey'])

	output = gzip.open("./tmp/cassiopeia_s3.gz", 'wb')

	for date in getDates():
		counter = 0
		size_counter = 0
		for file in s3.getBucketFiles().list(prefix = 'hydra_verticals/' + date):
			counter += 1
			size_counter += file.size

		if(counter == 0):
			evaluate = "Missing data" 
		elif(size_counter/1000000 == 0):
			evaluate = "Empty data files"
		else:
			evaluate = "OK"

		# S3 path for day X contains data for day X-1
		# Therefore this operation is needed	
		data_date = (datetime.strptime(date, '%Y/%m/%d') - timedelta(days=1)).strftime('%Y/%m/%d')

		print("{0},{1},{2},{3},{4},{5},{6},{7},{8}".format(
			"Yamato",
			"S3",
			"Storage",
			"live-temp/hydra_verticals",
			"Verticals",
			data_date,
			counter,
			size_counter/1000000,
			evaluate
			))

		output.write("{0},{1},{2},{3},{4},{5},{6},{7},{8}\n".format(
			"Yamato",
			"S3",
			"Storage",
			"live-temp/hydra_verticals",
			"Verticals",
			data_date,
			counter,
			size_counter/1000000,
			evaluate
			))

	output.close()


def evaluateDWH(conf):
	mssql = MsSQL('10.98.8.38',conf['mssql_user'],conf['mssql_pass'],1433)
	table = "[DWH].[dbo].[FactAdImpressionsHydra_NEW]"
	output = gzip.open("./tmp/cassiopeia_dwh.gz", 'wb')

	mssql.connect()

	data = mssql.runQuery("SELECT a.Date,COALESCE(b.ct,0) from [DWH].[dbo].[DimDate] a \
        LEFT JOIN \
        (SELECT ViewDate,count(*) as ct FROM {0} \
         WHERE PlatformID = 100 AND CountryID = 616 \
         group by ViewDate) b \
        on a.Date = b.ViewDate \
        where a.Date between '2017-01-01' and getdate() \
        order by date desc".format(table))

	for row in data:
	
		if (row[1] == 0):
			evaluate = "Missing data" 	
		else:
			evaluate = "OK"

		print("{0},{1},{2},{3},{4},{5},{6},{7},{8}".format(
			"Hikari",
			"MsSQL",
			"DWH",
			table,
			"otomoto.pl",
			row[0],
			row[1],
			'',
			evaluate
			))

		output.write("{0},{1},{2},{3},{4},{5},{6},{7},{8}\n".format(
			"Hikari",
			"MsSQL",
			"DWH",
			table,
			"otomoto.pl",
			row[0],
			row[1],
			'',
			evaluate
			))

	output.close()	
	mssql.disconnect()


def evaluateStaging(conf):
	mssql = MsSQL('10.98.8.38',conf['mssql_user'],conf['mssql_pass'],1433)
	table = "[Staging].[hydra].[AdImpressions_verticals]"
	output = gzip.open("./tmp/cassiopeia_stg.gz", 'wb')

	mssql.connect()

	data = mssql.runQuery("SELECT a.Date,COALESCE(b.ct,0) from [DWH].[dbo].[DimDate] a \
        LEFT JOIN\
        (SELECT date,count(*) as ct FROM {0} \
         group by date) b \
        on a.Date = b.date \
        where a.Date between '2017-07-01' and getdate() \
        order by date desc".format(table)) 

	for row in data:

		if (row[1] == 0):
			evaluate = "Missing data" 	
		else:
			evaluate = "OK"

		print("{0},{1},{2},{3},{4},{5},{6},{7},{8}".format(
			"Hikari",
			"MsSQL",
			"Staging",
			table,
			"Verticals",
			row[0],
			row[1],
			'',
			evaluate
			))

		output.write(
			"{0},{1},{2},{3},{4},{5},{6},{7},{8}\n".format(
			"Hikari",
			"MsSQL",
			"Staging",
			table,
			"Verticals",
			row[0],
			row[1],
			'',
			evaluate
			))

	output.close()	
	mssql.disconnect()


def evaluateOLAP(conf):
	mssql = MsSQL('10.98.8.38',conf['mssql_user'],conf['mssql_pass'],1433)
	output = gzip.open("./tmp/cassiopeia_olap.gz", 'wb')

	mssql.connect()

	data = mssql.runQuery("SELECT * FROM openquery([ARIADNEASMAIN], \
		'SELECT [Measures].[Ad Impressions (Big Data)] on 0, [Time].[Date].[Date] on 1 \
				FROM [Model] \
				WHERE \
				{ \
				([Site].[Site Hierarchy].[Site].&[otomoto.pl],[Time].[Year].&[2017]), \
				([Site].[Site Hierarchy].[Site].&[otomoto.pl],[Time].[Year].&[2018]) \
				}\
		')")

	for row in data:

		if (row[1] == None):
			evaluate = "Missing data" 	
		else:
			evaluate = "OK"

		if (row[1] == None):
			qty_check = '' 	
		else:
			qty_check = row[1]	

		print("{0},{1},{2},{3},{4},{5},{6},{7},{8}".format(
			"Ariadne",
			"OLAP",
			"Tabular Model",
			"[Measures].[Ad Impressions (Big Data)]",
			"otomoto.pl",
			row[0],
			row[1],
			'',
			evaluate
			))

		output.write(
			"{0},{1},{2},{3},{4},{5},{6},{7},{8}\n".format(
			"Ariadne",
			"OLAP",
			"Tabular Model",
			"[Measures].[Ad Impressions (Big Data)]",
			"otomoto.pl",
			row[0],
			qty_check,
			'',
			evaluate
			))
			
	output.close()				
	mssql.disconnect()


def sendResultsToS3(filename,localfile,conf):
	s3 = auxS3('verticals-raw-data','/cassiopeia/',conf['aux_s3_key'],conf['aux_s3_skey'])
	s3.sendToS3(filename,localfile)


def sendAllResultsToS3(conf):
	sendResultsToS3('cass_s3.gz','./tmp/cassiopeia_s3.gz',conf)
	sendResultsToS3('cass_stg.gz','./tmp/cassiopeia_stg.gz',conf)
	sendResultsToS3('cass_dwh.gz','./tmp/cassiopeia_dwh.gz',conf)
	sendResultsToS3('cass_olap.gz','./tmp/cassiopeia_olap.gz',conf)

def loadResultsToReshift(conf):

	print("Inserting results")
	rs = redshift('10.101.5.159','5671','main',conf['yamato_user'],conf['yamato_pass'])

	rs.connect()

	rs.runQuery("TRUNCATE TABLE miguel_chin.cassiopeia_results;")

	rs.runQuery("copy miguel_chin.cassiopeia_results \
		from 's3://verticals-raw-data/cassiopeia/cass_stg.gz' \
		access_key_id '{0}' \
		secret_access_key '{1}' \
		gzip \
		delimiter ',' escape;".format(conf['aux_s3_key'],conf['aux_s3_skey']))

	rs.runQuery("copy miguel_chin.cassiopeia_results \
		from 's3://verticals-raw-data/cassiopeia/cass_dwh.gz' \
		access_key_id '{0}' \
		secret_access_key '{1}' \
		gzip \
		delimiter ',' escape;".format(conf['aux_s3_key'],conf['aux_s3_skey']))

	rs.runQuery("copy miguel_chin.cassiopeia_results \
		from 's3://verticals-raw-data/cassiopeia/cass_olap.gz' \
		access_key_id '{0}' \
		secret_access_key '{1}' \
		gzip \
		delimiter ',' escape;".format(conf['aux_s3_key'],conf['aux_s3_skey']))

	rs.runQuery("copy miguel_chin.cassiopeia_results \
		from 's3://verticals-raw-data/cassiopeia/cass_s3.gz' \
		access_key_id '{0}' \
		secret_access_key '{1}' \
		gzip \
		delimiter ',' escape;".format(conf['aux_s3_key'],conf['aux_s3_skey']))

	rs.disconnect()

	print("Process completed.")

