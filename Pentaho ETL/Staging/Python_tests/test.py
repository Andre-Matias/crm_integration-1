import psycopg2
import numpy as np

conn=psycopg2.connect(dbname= 'globalverticals', host='gv-chandra.ckwrimb1igb1.us-west-2.redshift.amazonaws.com', 
port= '5439', user= 'pyrate', password= 'Pyrate4life')

cur = conn.cursor()


COUNTRY = 'IN'
CATEGORY = 'Cars'
DATE_PATH = '2017/10/11'

cur.execute(
	("COPY reporting.rocha_metric_m_mau FROM 's3://pyrates-data-ocean/rocha_report/MAU/%s/%s/%s/data.csv.gz' \
	CREDENTIALS 'aws_access_key_id=AKIAJ64EPWTWUB3XAQTA;aws_secret_access_key=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'\
	GZIP  \
	DELIMITER ';'  \
	ACCEPTINVCHARS \
	ESCAPE \
	TIMEFORMAT 'auto'\
	DATEFORMAT 'auto'\
	NULL AS '\\000'\
	" % (COUNTRY,CATEGORY,DATE_PATH)
	))


data = np.array(cur.fetchall())

print(data)

cur.close()
conn.close()


raise ValueError('Error START PANICKING!!!!')
