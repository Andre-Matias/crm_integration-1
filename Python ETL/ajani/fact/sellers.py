import psycopg2
import simplejson as json
import sys
from aws_commons import *
from datetime import date, timedelta

silver_conf = sys.argv[1]

conn_silver = getConnection(silver_conf)
cur = conn_silver.cursor()



def getDailyRegisteredSellers():
	return 	"INSERT INTO verticals_bi.fact_d_crm_aux_registered_sellers (SELECT "\
				"CAST( %(date)s AS date ), "\
				"current_date, "\
				"map.country_id,map.category_id, map.country_id ||'|'|| map.category_id ||'|'|| map.type as platform_sk, "\
				"map.country_id || '|' || con.id_geo_level_1 || '|' ||map.type as geo_l1_sk, "\
				"map.country_id || '|' || con.id_geo_level_2 || '|' ||map.type as geo_l2_sk, "\
				"map.country_id || '|' || con.id_geo_level_3 || '|' ||map.type as geo_l3_sk, "\
				"COUNT(DISTINCT id_user) as registered_sellers "\
			"FROM livesync.verticals_crm_cars_user users "\
			"INNER JOIN verticals_bi.map_livesync_dbname map ON users.livesync_dbname = map.livesync_dbname "\
			"INNER JOIN livesync.verticals_crm_cars_contact_information con ON (users.id_contact_information = con.id_contact_information AND users.livesync_dbname = con.livesync_dbname) "\
			"WHERE users.status = 1 "\
			"AND users.create_date < %(date)s "\
			"GROUP BY 1,2,3,4,5,6,7,8 "\
			"UNION "\
			"SELECT "\
				"CAST( %(date)s AS date ), "\
				"current_date, "\
				"map.country_id, "\
				"map.category_id, "\
				"map.country_id ||'|'|| map.category_id ||'|'|| map.type as platform_sk, "\
				"map.country_id || '|' || con.id_geo_level_1 || '|' ||map.type as geo_l1_sk, "\
				"map.country_id || '|' || con.id_geo_level_2 || '|' ||map.type as geo_l2_sk, "\
				"map.country_id || '|' || con.id_geo_level_3 || '|' ||map.type as geo_l3_sk, "\
				"COUNT(DISTINCT id_user) as registered_sellers "\
			"FROM livesync.verticals_crm_real_estate_user users "\
			"INNER JOIN verticals_bi.map_livesync_dbname map ON users.livesync_dbname = map.livesync_dbname "\
			"INNER JOIN livesync.verticals_crm_real_estate_contact_information con ON (users.id_contact_information = con.id_contact_information AND users.livesync_dbname = con.livesync_dbname) "\
			"WHERE users.status = 1 "\
			"AND users.create_date <= %(date)s "\
			"GROUP BY 1,2,3,4,5,6,7,8)"

def getMonthlyRegisteredSellers():
	return 	"INSERT INTO verticals_bi.fact_m_crm_aux_registered_sellers (SELECT "\
				"DATE_TRUNC('month',CAST( %(date)s AS date )), "\
				"current_date, "\
				"map.country_id,map.category_id, map.country_id ||'|'|| map.category_id ||'|'|| map.type as platform_sk, "\
				"map.country_id || '|' || con.id_geo_level_1 || '|' ||map.type as geo_l1_sk, "\
				"map.country_id || '|' || con.id_geo_level_2 || '|' ||map.type as geo_l2_sk, "\
				"map.country_id || '|' || con.id_geo_level_3 || '|' ||map.type as geo_l3_sk, "\
				"COUNT(DISTINCT id_user) as registered_sellers "\
			"FROM livesync.verticals_crm_cars_user users "\
			"INNER JOIN verticals_bi.map_livesync_dbname map ON users.livesync_dbname = map.livesync_dbname "\
			"INNER JOIN livesync.verticals_crm_cars_contact_information con ON (users.id_contact_information = con.id_contact_information AND users.livesync_dbname = con.livesync_dbname) "\
			"WHERE users.status = 1 "\
			"AND users.create_date < %(date)s "\
			"GROUP BY 1,2,3,4,5,6,7,8 "\
			"UNION "\
			"SELECT "\
				"DATE_TRUNC('month',CAST( %(date)s AS date )), "\
				"current_date, "\
				"map.country_id, "\
				"map.category_id, "\
				"map.country_id ||'|'|| map.category_id ||'|'|| map.type as platform_sk, "\
				"map.country_id || '|' || con.id_geo_level_1 || '|' ||map.type as geo_l1_sk, "\
				"map.country_id || '|' || con.id_geo_level_2 || '|' ||map.type as geo_l2_sk, "\
				"map.country_id || '|' || con.id_geo_level_3 || '|' ||map.type as geo_l3_sk, "\
				"COUNT(DISTINCT id_user) as registered_sellers "\
			"FROM livesync.verticals_crm_real_estate_user users "\
			"INNER JOIN verticals_bi.map_livesync_dbname map ON users.livesync_dbname = map.livesync_dbname "\
			"INNER JOIN livesync.verticals_crm_real_estate_contact_information con ON (users.id_contact_information = con.id_contact_information AND users.livesync_dbname = con.livesync_dbname) "\
			"WHERE users.status = 1 "\
			"AND users.create_date <= %(date)s "\
			"GROUP BY 1,2,3,4,5,6,7,8)"

def getDailyPayingSellers_India():
 return "INSERT INTO verticals_bi.fact_d_crm_aux_paying_sellers (SELECT "\
			"CAST( %(date)s AS date ), "\
			"current_date, "\
			"map.country_id, "\
			"map.category_id, "\
			"map.country_id ||'|'|| map.category_id ||'|'|| map.type as platform_sk, "\
			"map.country_id || '|' || con.id_geo_level_1 || '|'||map.type as geo_l1_sk, "\
			"map.country_id || '|' || con.id_geo_level_2 || '|'||map.type as geo_l2_sk, "\
			"map.country_id || '|' || con.id_geo_level_3 || '|'||map.type as geo_l3_sk, "\
			"COUNT(DISTINCT buyer) "\
		"FROM livesync.verticals_crm_cars_package_subscription ps "\
		"INNER JOIN livesync.verticals_crm_cars_package p ON ps.package_id = p.id "\
		"INNER JOIN livesync.verticals_crm_cars_user u ON u.id_user = ps.buyer "\
		"INNER JOIN verticals_bi.map_livesync_dbname map ON u.livesync_dbname = map.livesync_dbname "\
		"INNER JOIN livesync.verticals_crm_cars_contact_information con ON (u.id_contact_information = con.id_contact_information AND u.livesync_dbname = con.livesync_dbname) "\
		"WHERE ps.STATUS = 1 "\
		"AND p.livesync_dbname = 'crm_cars_in' "\
		"AND ps.livesync_dbname = 'crm_cars_in' "\
		"AND u.livesync_dbname = 'crm_cars_in' "\
		"AND ( LOWER(p.name) LIKE %(retail)s) "\
		"AND (CAST(SUBSTRING(name,POSITION('Rs_' IN name)+3,POSITION('_' IN SUBSTRING(name,POSITION('Rs_' IN name)+3,len(name)))-1) AS INT) > 0 "\
		"OR p.name LIKE %(corporate)s "\
		") "\
		"AND NOT( u.email LIKE %(olx)s OR u.email LIKE %(sunfra_com)s "\
		"OR u.email LIKE %(sunfra_in)s OR u.email LIKE %(fixeads)s "\
		"OR u.email LIKE %(dispostable)s "\
		") "\
		"AND NOT((ps.end_date <= %(date)s + '00:00:00') OR (ps.start_date >= %(date)s + '23:59:59')) "\
		"GROUP BY 1,2,3,4,5,6,7,8)"

def getMonthlyPayingSellers_India():
 return "INSERT INTO verticals_bi.fact_m_crm_aux_paying_sellers (SELECT "\
			"DATE_TRUNC('month',CAST( %(date)s AS date )), "\
			"current_date, "\
			"map.country_id, "\
			"map.category_id, "\
			"map.country_id ||'|'|| map.category_id ||'|'|| map.type as platform_sk, "\
			"map.country_id || '|' || con.id_geo_level_1 || '|'||map.type as geo_l1_sk, "\
			"map.country_id || '|' || con.id_geo_level_2 || '|'||map.type as geo_l2_sk, "\
			"map.country_id || '|' || con.id_geo_level_3 || '|'||map.type as geo_l3_sk, "\
			"COUNT(DISTINCT buyer) "\
		"FROM livesync.verticals_crm_cars_package_subscription ps "\
		"INNER JOIN livesync.verticals_crm_cars_package p ON ps.package_id = p.id "\
		"INNER JOIN livesync.verticals_crm_cars_user u ON u.id_user = ps.buyer "\
		"INNER JOIN verticals_bi.map_livesync_dbname map ON u.livesync_dbname = map.livesync_dbname "\
		"INNER JOIN livesync.verticals_crm_cars_contact_information con ON (u.id_contact_information = con.id_contact_information AND u.livesync_dbname = con.livesync_dbname) "\
		"WHERE ps.STATUS = 1 "\
		"AND p.livesync_dbname = 'crm_cars_in' "\
		"AND ps.livesync_dbname = 'crm_cars_in' "\
		"AND u.livesync_dbname = 'crm_cars_in' "\
		"AND ( LOWER(p.name) LIKE %(retail)s) "\
		"AND (CAST(SUBSTRING(name,POSITION('Rs_' IN name)+3,POSITION('_' IN SUBSTRING(name,POSITION('Rs_' IN name)+3,len(name)))-1) AS INT) > 0 "\
		"OR p.name LIKE %(corporate)s "\
		") "\
		"AND NOT( u.email LIKE %(olx)s OR u.email LIKE %(sunfra_com)s "\
		"OR u.email LIKE %(sunfra_in)s OR u.email LIKE %(fixeads)s "\
		"OR u.email LIKE %(dispostable)s "\
		") "\
		"AND NOT((ps.end_date <= DATE_TRUNC('month',CAST( %(date)s AS date ))) OR (ps.start_date >= %(date)s + '23:59:59')) "\
		"GROUP BY 1,2,3,4,5,6,7,8)"


def getDeleteQueries():
 	return "DELETE FROM verticals_bi.fact_d_crm_aux_registered_sellers WHERE date_id = CAST( %(date)s AS date ); "\
 			"DELETE FROM verticals_bi.fact_m_crm_aux_registered_sellers WHERE month_id = DATE_TRUNC('month',CAST( %(date)s AS date )); "\
 			"DELETE FROM verticals_bi.fact_d_crm_aux_paying_sellers WHERE date_id = CAST( %(date)s AS date ); "\
 			"DELETE FROM verticals_bi.fact_m_crm_aux_paying_sellers WHERE month_id = DATE_TRUNC('month',CAST( %(date)s AS date ));" 



delimiter = '|'
date = (date.today() - timedelta(1)).strftime('%Y/%m/%d')

conn_silver = getConnection(silver_conf)
cur = conn_silver.cursor()
data = {'date': date }

query = getDeleteQueries()
cur.execute(query,data)
conn_silver.commit()

query = getDailyRegisteredSellers()
cur.execute(query,data)
conn_silver.commit()

query = getMonthlyRegisteredSellers()
cur.execute(query,data)
conn_silver.commit()

data2 = {
    'date': date,
    'retail' : '%retail%',
    'corporate': '%Corporate Stockars : Paid package',
    'olx' : '%olx.com',
	'sunfra_com': '%sunfra.com',
	'sunfra_in': '%sunfra.in',
	'fixeads': '%fixeads.com',
	'dispostable' : '%dispostable.com'
}
query = getDailyPayingSellers_India()
cur.execute(query,data2)
conn_silver.commit()

query = getMonthlyPayingSellers_India()
cur.execute(query,data2)
conn_silver.commit()

cur.close()
conn_silver.close()




