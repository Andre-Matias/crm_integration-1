import pymssql
from boto.s3.connection import S3Connection, Bucket, Key
import datetime
import gzip
import os
import boto
import psycopg2

class hydraS3:
	def __init__(self, bucket, path, awskey,awsskey):
		self.s3_bucket = bucket
		self.s3_path = path
		self.key = awskey
		self.skey = awsskey

	def getBucketFiles(self):
		conn = S3Connection(self.key, self.skey)
		return Bucket(conn, self.s3_bucket)


class MsSQL:
	def __init__(self, server, user, password, port):
		self.mssql_server = server
		self.mssql_user = user
		self.mssql_pass = password
		self.mssql_port = port

	def connect(self):
		self.con = pymssql.connect(server=self.mssql_server, user=self.mssql_user, password=self.mssql_pass, port=self.mssql_port)
		self.cursor = self.con.cursor()

	def disconnect(self):
		self.cursor.close()
		self.con.close()

	def runQuery(self,query):
		self.cursor.execute(query)
		return self.cursor.fetchall()


class auxS3:
	def __init__(self, bucket, path, awskey, awsskey):
		self.s3_bucket = bucket
		self.s3_path = path
		self.key = awskey
		self.skey = awsskey

	def sendToS3(self,filename,localfile):		
		full_key_name = os.path.join(self.s3_path, filename)
		conn = boto.connect_s3(self.key,self.skey)
		bucket = conn.get_bucket(self.s3_bucket)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name
		k.set_contents_from_filename(localfile)
		
class redshift:
	def __init__(self, host, port, db, user, password):
		self.redshift_host = host
		self.redshift_port = port
		self.redshift_db = db
		self.redshift_user = user
		self.redshift_password = password

	def connect(self):
		self.con = psycopg2.connect(dbname=self.redshift_db, host=self.redshift_host, port=self.redshift_port, user=self.redshift_user, password=self.redshift_password)
		self.cursor = self.con.cursor()

	def disconnect(self):
		self.cursor.close()
		self.con.close()

	def runQuery(self,query):
		self.cursor.execute(query)
		self.con.commit()
		#return self.cursor.fetchall()



