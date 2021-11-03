#!/usr/local/bin/python2.7
# -*- coding: utf-8 -*-
# before to negative epcoh
from Cryptodome.Hash import SHA256, SHA1
from Cryptodome.Signature import pss
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import traceback
import ConfigParser
import logging
import sys
import os
import base64
import psycopg2
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import datetime
import uuid

config = ConfigParser.RawConfigParser()
config.read(os.path.dirname(os.path.realpath(__file__)) + "/bq.properties")

#Uncomment the below line in case there is any proxy configured for your google cloud project. Update the proxy in the bq.properties file
#os.environ['http_proxy'] = config.get('Proxy', 'http.proxy')
#os.environ['https_proxy'] = config.get('Proxy', 'https.proxy')


def bqLogger(path, name):
	logging.basicConfig(format='%(asctime)s - %(levelname)s- %(message)s', datefmt='%d-%b-%y %H:%M:%S')
	formatter = logging.Formatter('%(asctime)s - %(levelname)s  - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
	logger = logging.getLogger(name)
	logger.setLevel(logging.INFO)
	handler = logging.FileHandler(os.path.join(path, name), 'w')
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	return logger

def createConnectionPool():
	import psycopg2
	from psycopg2 import pool
	try:
		postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 4,host=config.get('DatabaseSection', 'database.url'),port=config.get('DatabaseSection', 'database.port') ,user=config.get('DatabaseSection', 'database.user'), password=decryptPasswd, dbname=config.get('DatabaseSection', 'database.dbname'))
		if(postgreSQL_pool):
			logger.info("Connection pool created successfully")
		else:
			logger.exception("Exception Occured")
			return "NA"
		return postgreSQL_pool;
	except (Exception, psycopg2.DatabaseError) as error :
		logger.error("Error while connecting to PostgreSQL", error)
		return "NA"

def get_decrypted_password(encrypted_password):
	privKeyObj = RSA.importKey(open(config.get('DecryptionJar', 'metadata_privpem')).read())
	cipher = PKCS1_OAEP.new(key=privKeyObj, hashAlgo=SHA256, mgfunc=lambda x, y: pss.MGF1(x, y, SHA1))
	logger.info('Decrypting Key')
	return cipher.decrypt(base64.b64decode(encrypted_password))

def bq_client(project, service_account_file_path):
	import json
	from google.oauth2 import service_account
	contents = ""
	with open(service_account_file_path) as f:
		for line in f.readlines():
			contents += line
	info = json.loads(contents)
	credentials = service_account.Credentials.from_service_account_info(info)
	os.remove(service_account_file_path)
	return bigquery.Client(project=project, credentials=credentials)


def fetch_records(query,postgresPool):
	try:
		connection=postgresPool.getconn()
		cur = connection.cursor()
		logger.info(query)
		cur.execute(query)
		logger.info('Executed')
		return cur.fetchall()
	except Exception as e:
		logger.exception("Unable to Connect to JUNIPER meta data table :{0}".format(e))
	finally:
		cur.close()
		postgresPool.putconn(connection)

def insert_records(query,postgresPool):
	try:
		connection=postgresPool.getconn()
		cur = connection.cursor()
		logger.info(query)
		cur.execute(query)
		connection.commit()
	except Exception as e:
		logger.exception("Unable to Connect to JUNIPER meta data table :{0}".format(e))
	finally:
		cur.close()
		postgresPool.putconn(connection)


def date_format_check(format, col):
	casting_string = ""
	if format == 'yyyy-MM-dd':
		casting_string = " PARSE_TIME('%Y-%m-%d', " + col + ") as " + col + " ,"
	elif format == 'yyyy-MM-dd HH:mm:ss+ss':
#casting_string = " COALESCE(CASE WHEN length(" + col + ") = 10 then DATE(PARSE_TIMESTAMP('%Y-%m-%d', " + col + ")) ELSE NULL END,CASE when length(" + col + ") = 21 then DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', " + col + ")) ELSE NULL END, DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', " + col + "))) as " + col + " ,"
		casting_string = " COALESCE(CASE WHEN " + col + " IS NOT NULL and length(cast (" + col + "  as string)) = 10 then DATE(PARSE_TIMESTAMP('%Y-%m-%d', cast(" + col + " as string ))) ELSE NULL END,CASE when " + col + " IS NOT NULL and length( cast(" + col + " as string)) = 22 then DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', cast(" + col + " as string))) ELSE NULL END, case when " + col + " IS NOT NULL then DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', cast(" + col + " as string))) else NULL end) as " + col + " ,"
	else:  # default
		casting_string = " " + col + " ,"
	return casting_string
#casting_string = " " + col + " ,"
#return casting_string


def timestamp_format_check(format, col):
	casting_string = ""
	casting_string = " " + col + " ,"
	return casting_string


def datetime_format_check(format, col):
	casting_string = ""
	casting_string = " " + col + " ,"
	return casting_string


def time_format_check(format, col):
	casting_string = ""
	if format == 'dd-MM-yy':
		casting_string = " PARSE_TIME('%d-%m-%Y', " + col + ") as " + col + " ,"
	elif format == 'yyyy-MM-dd':
		casting_string = " PARSE_TIME('%Y-%m-%d', " + col + ") as " + col + " ,"
	elif format == 'yyyy-MM-dd':
		casting_string = " PARSE_TIME('%Y-%m-%d', " + col + ") as " + col + " ,"
	else:  # default
		casting_string = " " + col + " ,"
	return casting_string


def create_query(runid, datasetName, columns, tempTableName, srcDateFormat, srcDateTimeFormat, srcTimeStampFormat,srcTimeFormat, src_type,ext_feed_id,trim_char,postgresPool):
	logger.info(columns)
	query = "select "
	def checkDateTimeFormat():
		if type.strip().lower() == "DATE".lower():
			return date_format_check(srcDateFormat, name)
		elif type.strip().lower() == "TIMESTAMP".lower():
			return timestamp_format_check(srcTimeStampFormat, name)
		elif type.strip().lower() == "TIME".lower():
			return time_format_check(srcTimeFormat, name)
		elif type.strip().lower() == "DATETIME".lower():
			return datetime_format_check(srcDateTimeFormat, name)
		else:
			return name + ","
	for col in columns:
		name = col[0]
		type = col[1]
		
		if src_type.lower() == "unix":
#if type.strip().lower() == "TIMESTAMP".lower():
#       query = query + "TIMESTAMP_MILLIS(" + name + ") as " + name + ","
#else:
				if type == "STRING" and trim_char == "Y":
					query = query + "TRIM("+ name +"),"
				else:
					query = query + name + ","
		elif src_type.lower() == "oracle" or src_type.lower()=="teradata" or src_type.lower()=="db2":
			if(type == "DATE" ):
				fetch_data_type=""" select src_data_typ,tgt_data_typ from juniperx.juniper_ext_cust_datatyp_map where feed_id=%d and src_data_typ='DATE'""" % (ext_feed_id)
				data_type_list=fetch_records(fetch_data_type,postgresPool)
				
				if len(data_type_list)>0 and data_type_list[0][0] == "DATE":
					query=query+date_format_check("yyyy-MM-dd HH:mm:ss+ss",name);
				else:
					if type == "STRING" and trim_char == "Y":
						query = query + "TRIM("+ name +"),"
					else:
						query = query + name + ","
			else:
				if type=="STRING" and trim_char == "Y":
					query = query + "TRIM("+ name +"),"
				else:
					query = query + name + ","
		else:
			if type=="STRING" and trim_char == "Y":
				query = query + "TRIM("+ name +"),"
			else:
				query = query + name + ","
		
	return query + " " + runid + " as JUNIPER_RUN_ID , CURRENT_TIMESTAMP() AS JUNIPER_PUBLISHED_TIME  from " + datasetName + "." + tempTableName


def update_table_published_status(pub_feed_sequence, feed_table_id, run_id, tgt_ds, tgt_tbl_name, tgt_record_count,status,postgresPool):
	import datetime
	now = datetime.datetime.now()
	connection=postgresPool.getconn()
	cur = connection.cursor()
	statement = 'insert into JUNIPERX.JUNIPER_PUB_FEED_TBL_STATS (pub_feed_sequence,feed_table_id,run_id,tgt_ds,tgt_tbl_name,tgt_record_count,crtd_dt,uptd_dt,crtd_by,uptd_by,status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
	cur.execute(statement, (pub_feed_sequence, feed_table_id, run_id, tgt_ds, tgt_tbl_name, tgt_record_count, str(now), str(now), '', '',status))
	connection.commit()
	logger.info('{}:Table Published Status Updated Successfully , Status:{}'.format(feed_table_id, status))
	cur.close()
	postgresPool.putconn(connection)

def update_logger_stats_master(feed_unique_name, run_id, tgt_name, tbl_name, tgt_record_count, postgresPool):
	import datetime
	now = datetime.datetime.now()
	connection=postgresPool.getconn()
	cur = connection.cursor()
	event_type=tgt_name + '~' + str(tbl_name) + '-count'
	statement = 'insert into juniperx.logger_stats_master (event_feed_id, event_batch_date, event_run_id, event_type, event_value, event_ipaddress) values (%s,now(),%s,%s,%s,%s)'
	cur.execute(statement, (feed_unique_name, run_id, event_type, tgt_record_count,'pub-server'))
	connection.commit()
	cur.close()
	postgresPool.putconn(connection)

def update_feed_published_status(pub_feed_sequence, feed_unique_name, run_id, publishing_date, job_start_time,job_end_time, status,postgresPool):
	import datetime
	now = datetime.datetime.now()
	connection=postgresPool.getconn()
	cur = connection.cursor()
	statement = 'insert into juniperx.juniper_pub_feed_status (pub_feed_sequence,feed_unique_name,run_id,publishing_date,job_start_time,job_end_time,status) values (%s,%s,%s,%s,%s,%s,%s)'
	cur.execute(statement,(pub_feed_sequence, feed_unique_name, run_id, publishing_date, job_start_time, job_end_time, status))
	connection.commit()
	logger.info('{}:Feed Published Status Updated Successfully.. : Status:{}'.format(feed_unique_name, status))
	cur.close()
	postgresPool.putconn(connection)


def load_tbl_from_query(client, query, dataset_id, dest_table, write_disposition):
	from google.cloud import bigquery
	from google.cloud.bigquery.table import Table
	from google.cloud.bigquery.dataset import Dataset
	job_config = bigquery.QueryJobConfig()
	destination_dataset = client.dataset(dataset_id)
	destination_table = destination_dataset.table(dest_table)
	job_config.destination = destination_table
	# Set configuration.query.createDisposition
	job_config.create_disposition = 'CREATE_IF_NEEDED'
	# job_config.location = 'europe-west2'
	logger.info('Write Disposition' + write_disposition)
	# Set configuration.query.writeDisposition
	job_config.write_disposition = write_disposition
	if(write_disposition == 'WRITE_APPEND'):
		job_config.schema_update_options = [
			bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
			]
	# Start the query
	job = client.query(query, job_config=job_config)
	# Wait for the query to finish
	job.result()


def load_avro_file(client, dataset_name, file_gcs_path, table_name):
	dataset_ref = client.dataset(dataset_name)
	job_config = bigquery.LoadJobConfig()
	job_config.source_format = bigquery.SourceFormat.AVRO
	job_config.use_avro_logical_types=True
	job_config.write_disposition = 'WRITE_TRUNCATE'
	# job_config.location = 'europe-west2'
	uri = file_gcs_path
	logger.info('GCS file path is :{}'.format(uri))
	# 'gs://cloud-samples-data/bigquery/us-states/us-states.avro'
	try:
		load_job = client.load_table_from_uri(uri,dataset_ref.table(table_name),job_config=job_config)  # API request
		logger.info('Starting load job :{}'.format(load_job.job_id))
		load_job.result()  # Waits for table load to complete.
		logger.info('Load Job finished.')
		destination_table = client.get_table(dataset_ref.table(table_name))
		logger.info('Loaded {} rows.'.format(destination_table.num_rows))
		logger.info('Destination Schema : '+str(destination_table.schema))
	except Exception as e:
		logger.exception("Unable to Load AVRO file to target :{0}".format(e))


def process_load_job(client, run_id, pub_feed_seq, bq_tgt_ds, feed_unique_name, src_type, job_type, target_name, custom_target_flag, custom_target_path,ext_feed_id,postgresPool):
	# fetch table info for this run_id
	feed_status = 'no-update'
	logger.info('Inside process load job method, RunID:'+str(run_id)+',Pub_feed_seq:'+str(pub_feed_seq))
	fetch_table_info_qry = """
	select a.FEED_TABLE_ID, a.feed_load_type ,sum(a.TABLE_COUNT) from
	(
	select RTRIM(feed_table_id, '_*') AS feed_table_id , batch_type as feed_load_type , p.PUB_FEED_SEQUENCE, e.TABLE_COUNT from
	juniperx.juniper_pub_feed_file_dtls p, JUNIPERX.JUNIPER_PUB_FEED_DTLS pf, JUNIPERX.JUNIPER_EXT_TABLE_STATUS e
	where pf.PUB_FEED_SEQUENCE = p.PUB_FEED_SEQUENCE and pf.EXT_FEED_SEQUENCE=e.feed_id and
	RTRIM(feed_table_id, '_*') =e.TABLE_NAME and e.run_id ='%s' and  p.PUB_FEED_SEQUENCE=%d) a,
	( select  PUB_FEED_SEQUENCE, RTRIM(feed_table_id, '_*') AS feed_table_id from JUNIPERX.juniper_pub_feed_file_dtls where PUB_FEED_SEQUENCE=%d
	EXCEPT
	select distinct PUB_FEED_SEQUENCE, RTRIM(feed_table_id, '_*') AS feed_table_id from JUNIPERX.JUNIPER_PUB_FEED_TBL_STATS where PUB_FEED_SEQUENCE = %d and run_id='%s' and upper(status)='SUCCESS') b
	where  a.PUB_FEED_SEQUENCE=b.PUB_FEED_SEQUENCE and a.FEED_TABLE_ID = b.FEED_TABLE_ID group by  a.FEED_TABLE_ID, a.feed_load_type;
""" % (run_id, pub_feed_seq, pub_feed_seq, pub_feed_seq, run_id)
	tables_info = fetch_records(fetch_table_info_qry,postgresPool)
	i = 0
	logger.info('Load Type :'+job_type)
	if job_type == 'batch':
		tbl_load_type_query = """ select BATCH_TYPE from JUNIPERX.JUNIPER_PUB_FEED_DTLS where PUB_FEED_SEQUENCE= '%s' """ % (pub_feed_seq)
		tbl_load_type = fetch_records(tbl_load_type_query,postgresPool)
		for row in tbl_load_type:
			tbl_load_type = row[0]
	fetch_trim_char= """ select trim_char from JUNIPERX.JUNIPER_PUB_FEED_DTLS where PUB_FEED_SEQUENCE= '%s' """ % (pub_feed_seq)
	trim_char= fetch_records(fetch_trim_char,postgresPool)
	trim_char_var=""
	if len(trim_char) == 1 :
		if trim_char[0][0] != 'unchecked' and trim_char[0][0] is not None:
			trim_char_var=trim_char[0][0]
	for table in tables_info:
		try:
			tbl_name_with_schema = table[0].split('.')
			if len(tbl_name_with_schema) > 1:
				tbl_name = tbl_name_with_schema[1]
			else:
				tbl_name = tbl_name_with_schema[0]
			# tbl_name=table[0]
			if job_type != 'batch':
				tbl_load_type = table[1]
			# tbl_partition_column=table[2]
			fetch_prefix_suffix_query= """ select table_prefix,table_suffix from JUNIPERX.JUNIPER_PUB_FEED_DTLS where PUB_FEED_SEQUENCE= '%s' """ % (pub_feed_seq)
			tbl_pre_suf=fetch_records(fetch_prefix_suffix_query,postgresPool)
			
			if len(tbl_pre_suf) == 1 :
				if tbl_pre_suf[0][0] != 'unchecked' and tbl_pre_suf[0][0] is not None:
					tbl_name = str(tbl_pre_suf[0][0])+tbl_name
				if tbl_pre_suf[0][1] != 'unchecked' and tbl_pre_suf[0][1] is not None:
					tbl_name = tbl_name+str(tbl_pre_suf[0][1])
			table_count = table[2]
			logger.info("""Table load type : """.format(tbl_load_type)+", Count in Table Staus :"+str(table_count))
			write_disposition = 'WRITE_TRUNCATE'
			if tbl_load_type != 'full':
				write_disposition = 'WRITE_APPEND'
			# fetch table field metadata
			table_metadata_qry = """
				SELECT
				FEED_FLD_NAME , trg_fld_data_typ
				FROM
				juniperx.juniper_pub_feed_fld_dtls fi where upper(fi.FEED_TABLE_ID)=upper('%s') and fi.PUB_FEED_SEQUENCE=%d order by feed_fld_pos_num""" % (table[0], pub_feed_seq)
			tbl_metadata = fetch_records(table_metadata_qry,postgresPool)
			if table_count != 0:
				if src_type == 'Oracle':
					srcDateFormat = 'yyyy-MM-dd HH:mm:ss.SSS'
					srcTimeStampFormat = 'yyyy-MM-dd HH:mm:ss.SSS'
				else:
					srcDateFormat = ''
					srcTimeStampFormat = ''
				srcDateTimeFormat = ''
				srcTimeFormat = ''
				casting_query = create_query(run_id, bq_tgt_ds, tbl_metadata, tbl_name + '_TEMP', srcDateFormat,srcDateTimeFormat,srcTimeStampFormat, srcTimeFormat, src_type,ext_feed_id,trim_char_var,postgresPool)
				logger.info(' Casting Query ' + casting_query)
				if casting_query == '':
					logger.info('Failed to Fetch the metadata of the Table for this RunId')
			# TODO: update table status as failed for this runid	
					continue
		# fetch gcs path for this run_id
				tgt_ds_path_qry = """
				select  distinct m.FEED_SRC_BKT, n.country_code,
				n.feed_unique_name,extracted_date,run_id  from
				(select COUNTRY_CODE ,FEED_ID, FEED_UNIQUE_NAME , RUN_ID , EXTRACTED_DATE , PROJECT_SEQUENCE from juniperx.juniper_ext_nifi_status where
				feed_unique_name='%s' and JOB_TYPE='W' and RUN_ID ='%s') n ,
				(select FEED_SRC_BKT , PROJECT_SEQUENCE from JUNIPERX.JUNIPER_PUB_FEED_DTLS where PUB_FEED_SEQUENCE=%d) m where m.PROJECT_SEQUENCE = n.PROJECT_SEQUENCE
				""" % (feed_unique_name, run_id, pub_feed_seq)
				tgt_ds_path = fetch_records(tgt_ds_path_qry,postgresPool)
		# Loading files into temp table
				for f in tgt_ds_path:
					logger.info('gs://' + f[0] + '/' + f[1] + '/' + f[2] + '/' + f[3] + '/' + f[4] + '/data/')
					logger.info('Source Type :'+src_type)
					
					if custom_target_flag == 'Y':
						custom_target_path=custom_target_path.replace("<<RUNID>>",f[4])
						custom_target_path=custom_target_path.replace("<<YYYYMMDD>>",f[3])
						avro_file_uri = custom_target_path + "/"+ table[0] +"*.avro"
					elif src_type.lower() == "unix":
						avro_file_uri = 'gs://' + f[0] + '/' + f[1] + '/' + f[2] + '/' + f[3] + '/' + f[4] + '/data/' + feed_unique_name + '_' + table[0] + '*.avro'
					else:
						avro_file_uri = 'gs://' + f[0] + '/' + f[1] + '/' + f[2] + '/' + f[3] + '/' + f[4] + '/data/' + feed_unique_name + '_' + table[0] + '_' + run_id + '_*.avro'
					logger.info('DS URI IS: ' + avro_file_uri)
					load_avro_file(client, bq_tgt_ds, avro_file_uri, tbl_name + '_TEMP')
					load_tbl_from_query(client, casting_query, bq_tgt_ds, tbl_name, write_disposition)
					logger.info('Casting QUERY :' + casting_query)
					# execute load job
					delete_table(client, bq_tgt_ds, tbl_name + '_TEMP')
					tgt_record_count = bq_record_count(client, bq_tgt_ds, tbl_name, run_id)
					logger.info('Records ### {}'.format(tgt_record_count))	
					i = i + 1
					update_table_published_status(pub_feed_seq, table[0], run_id, bq_tgt_ds, tbl_name, tgt_record_count,'success',postgresPool)
					update_logger_stats_master(feed_unique_name, run_id, target_name, table[0], tgt_record_count, postgresPool)
			else:
				logger.info('Extracted Count for {0} table is zero, Empty Table Will be created if Table not Exists'.format(table))
				create_table(client, gcp_dataset_id, tbl_name, tbl_metadata,tbl_load_type)
				tgt_record_count = 0
				logger.info('Records ### {}'.format(tgt_record_count))
				update_table_published_status(pub_feed_seq, table[0], run_id, bq_tgt_ds, tbl_name, tgt_record_count,'success',postgresPool)
				update_logger_stats_master(feed_unique_name, run_id, target_name, tbl_name, tgt_record_count, postgresPool)
				i = i + 1
		except:
			logger.exception('Error Publishing Table:{}'.format(tbl_name))
			# traceback.print_exc()
			update_table_published_status(pub_feed_seq, table[0], run_id, bq_tgt_ds, tbl_name, 0,'failed',postgresPool)
			feed_status = 'failed'
			logger.exception('Error Publishing Table {0} :{1}'.format(tbl_name, traceback.format_exc()))
		continue
	if i > 0:
		if feed_status != 'failed':
			feed_status = 'success'
	logger.info(feed_status)
	return feed_status



def create_dataset(client, dataset_id):
	from google.api_core import exceptions
	try:
		"""Create a dataset."""
		dataset_ref = client.dataset(dataset_id)
		dataset = bigquery.Dataset(dataset_ref)
		dataset.location = "europe-west2"
		client.create_dataset(dataset)  # API request
	except:
		logger.exception('Exception in creating Dataset')




def delete_table(client, dataset_id, table_id):
	table_ref = client.dataset(dataset_id).table(table_id)
	client.delete_table(table_ref)  # API request
	logger.info('Table {}:{} deleted.'.format(dataset_id, table_id))


def bq_record_count(client, dataset_id, table_id, run_id):
	record = 0
	query_string = """
	select count(*) as record_count from %s.%s where juniper_run_id=%d""" % (dataset_id, table_id, int(run_id))
	try:
		query_job = client.query(query_string)
		for row in query_job.result():  # Wait for the job to complete.
			logger.info("Records Loaded for {0} Runid is :{1}".format(run_id, row['record_count']))
			record = row['record_count']
		return record
	except Exception as e:
		logger.exception("Error while retrieving the Count from target")
		jlogger('Error while retrieving the Count from target :{0}'.format(e))


def create_table(client, dataset_id, table_id, schema,tbl_load_type):
	table_schema = []
	try:
		for col in schema:
			name = col[0]
			type = col[1]
			table_schema.append(bigquery.SchemaField(name, type))
	
		table_schema.append(bigquery.SchemaField('JUNIPER_RUN_ID','INTEGER'))
		table_schema.append(bigquery.SchemaField('JUNIPER_PUBLISHED_TIME','TIMESTAMP'))
		dataset_ref = client.dataset(dataset_id)
		table_ref = dataset_ref.table(table_id)
		if(tbl_load_type == 'full'):
			client.delete_table(table_ref)
		table = bigquery.Table(table_ref, schema=table_schema)
		client.create_table(table)
		logger.info('Empty Table Created , Table Id is : {}'.format(table_id))
	except NotFound as error:
		table = bigquery.Table(table_ref, schema=table_schema)
		client.create_table(table)
		logger.info('Empty Table Created , Table Id is : {}'.format(table_id))
	except Exception as e:
		logger.info('Error While Creating Empty Table :'+str(e))

def check_file_field_details(pub_feed_seq,postgresPool):
	qry_check_field_details = """
	select count(*) from JUNIPERX.JUNIPER_PUB_FEED_FILE_DTLS f, JUNIPERX.JUNIPER_PUB_FEED_FLD_DTLS fi where f.PUB_FEED_SEQUENCE = fi.PUB_FEED_SEQUENCE and fi.PUB_FEED_SEQUENCE = %d
	""" % (pub_feed_seq)
	logger.info(qry_check_field_details)
	pub_field_details_info = fetch_records(qry_check_field_details,postgresPool)
	file_field_count = pub_field_details_info[0][0]
	logger.info('Pub Tables/Columns count:{0}'.format(file_field_count))
	return file_field_count

def count_file_field_details(ext_feed_id,postgresPool):
	qry_view_check_field_details = """select count(*) from JUNIPERX.JUNIPER_EXT_FILE_MASTER_VW f, JUNIPERX.JUNIPER_EXT_FIELD_MASTER_VW fi where f.FEED_SEQUENCE = fi.FEED_SEQUENCE and fi.FEED_SEQUENCE = %d""" % (ext_feed_id)
	view_field_details_info = fetch_records(qry_view_check_field_details,postgresPool)
	file_field_count = view_field_details_info[0][0]
	logger.info('View Table/Columns count: {0}'.format(file_field_count))
	return file_field_count


def getSourceType(pub_feed_id,postgresPool):
	query = """select FEED_SRC_TYPE from JUNIPERX.JUNIPER_PUB_FEED_DTLS where PUB_FEED_SEQUENCE=%d""" % (pub_feed_id)
	logger.info("""Source Type fetch Query : {0}""".format(query))
	src_type = fetch_records(query,postgresPool)
	return src_type[0][0]


def getDataTypeMappingQuery(pub_feed_id, sourceColume,postgresPool):
	logger.info('columns ::::: ' + sourceColume)
	src_type = getSourceType(pub_feed_id,postgresPool)
	query = """select SRC_DATA_TYP,TGT_DATA_TYP from JUNIPERX.MSTR_DATATYPE_LINK_DTLS where TGT_DB_TYP='BigQuery' and SRC_DB_TYP='%s'""" % (src_type.upper())
	logger.info("""data type mapping Query : {0}""".format(query))
	results = fetch_records(query,postgresPool)
	buffer = "COALESCE( "
	for rs in results:
		buffer = buffer + "CASE WHEN " + sourceColume + "='" + rs[0].upper() + "' THEN '" + rs[1].upper() + "' ELSE NULL END,"
	
	buffer = buffer + " NULL,'STRING')"
	return buffer


def populate_file_field_details(pub_feed_id, juniper_project_id, feed_unique_name, userName,postgresPool):
	tableName = "JUNIPERX.JUNIPER_PUB_FEED_FILE_DTLS"
	delPrevRecQry="delete from JUNIPERX.JUNIPER_PUB_FEED_FILE_DTLS where PUB_FEED_SEQUENCE={0}".format(pub_feed_id)
	insert_records(delPrevRecQry,postgresPool)
	if getSourceType(pub_feed_id,postgresPool) == 'Unix':
		insertFileEntry = """
	insert into  %s (PUB_FEED_SEQUENCE ,FEED_TABLE_id ,FEED_TABLE_name ,FEED_TABLE_desc ,FEED_TABLE_type ,FEED_TABLE_delimiter,tgt_tbl_name ,FEED_sch_loc ,FEED_hdr_cnt  ,FEED_trl_cnt  ,FEED_cnt_start_idx ,FEED_cnt_end_idx ,data_class_catg ,FEED_load_type ,CREATED_BY ,UPDATED_BY ,CREATED_DATE ,UPDATED_DATE) select %d as PUB_FEED_SEQUENCE,split_part(src_table, '.',1) as FEED_TABLE_id, split_part(src_table, '.',1) as FEED_TABLE_name , split_part(src_table, '.',1) as FEED_TABLE_desc , '' as FEED_TABLE_type, 'A' as FEED_TABLE_delimiter , split_part(src_table, '.',1)  as tgt_tbl_name , null as FEED_sch_loc , null as FEED_hdr_cnt, null as FEED_trl_cnt, null as FEED_cnt_start_idx, null as FEED_cnt_end_idx , null as data_class_catg , FETCH_TYPE as FEED_load_type, '%s' as CREATED_BY, '%s' as UPDATED_BY , '%s' as CREATED_DATE , '%s' as UPDATED_DATE from JUNIPERX.JUNIPER_EXT_FILE_MASTER_VW where FEED_SEQUENCE = %d
	""" % (tableName, pub_feed_id, userName, userName, str(datetime.datetime.now()), str(datetime.datetime.now()),feed_unique_name)
	else:
		insertFileEntry = """
	insert into  %s (PUB_FEED_SEQUENCE ,FEED_TABLE_id ,FEED_TABLE_name ,FEED_TABLE_desc ,FEED_TABLE_type ,FEED_TABLE_delimiter,tgt_tbl_name ,FEED_sch_loc ,FEED_hdr_cnt  ,FEED_trl_cnt  ,FEED_cnt_start_idx ,FEED_cnt_end_idx ,data_class_catg ,FEED_load_type ,CREATED_BY ,UPDATED_BY ,CREATED_DATE ,UPDATED_DATE) select %d as PUB_FEED_SEQUENCE,  SRC_TABLE as FEED_TABLE_id, SRC_TABLE as FEED_TABLE_name , SRC_TABLE as FEED_TABLE_desc , '' as FEED_TABLE_type, 'A' as FEED_TABLE_delimiter , SRC_TABLE as tgt_tbl_name , null as FEED_sch_loc , null as FEED_hdr_cnt, null as FEED_trl_cnt, null as FEED_cnt_start_idx, null as FEED_cnt_end_idx , null as data_class_catg , FETCH_TYPE as FEED_load_type, '%s' as CREATED_BY, '%s' as UPDATED_BY ,
	'%s' as CREATED_DATE , '%s' as UPDATED_DATE from JUNIPERX.JUNIPER_EXT_FILE_MASTER_VW where FEED_SEQUENCE = %d
	""" % (tableName, pub_feed_id, userName, userName, str(datetime.datetime.now()), str(datetime.datetime.now()),feed_unique_name)
	logger.info("""populate_file_field_details Query : {0}""".format(insertFileEntry))
	insert_records(insertFileEntry,postgresPool)
	tableName = "JUNIPERX.JUNIPER_PUB_FEED_FLD_DTLS"
	delPrevRecQry="delete from JUNIPERX.JUNIPER_PUB_FEED_FLD_DTLS where PUB_FEED_SEQUENCE={0}".format(pub_feed_id)
	insert_records(delPrevRecQry,postgresPool)
	if getSourceType(pub_feed_id,postgresPool) != 'Unix':
		insertFieldEntry = """ insert into %s (PUB_FEED_SEQUENCE,FEED_TABLE_id ,FEED_fld_pos_num ,FEED_sch_name ,FEED_fld_name ,FEED_fld_desc ,
		FEED_fld_data_typ,trg_fld_data_typ ,fld_null_flg ,tgt_tbl_prtn_flg ,pii_flg  ,fxd_fld_strt_idx ,fxd_fld_end_idx ,fxd_fld_len ,pkey ,CREATED_DATE ,UPDATED_DATE ,CREATED_BY ,UPDATED_BY)
		select %d as PUB_FEED_SEQUENCE, src_table as FEED_TABLE_id, COLUMN_POS as FEED_fld_pos_num, null as FEED_sch_name, SRC_PHY_ATTRIBUTE_NAME as FEED_fld_name, SRC_PHY_ATTRIBUTE_NAME as FEED_fld_desc , 
		SRC_PHY_ATTRIBUTE_TYPE as FEED_fld_data_typ , """ % (tableName, pub_feed_id)
	else:
		insertFieldEntry = """ insert into %s (PUB_FEED_SEQUENCE,FEED_TABLE_id ,FEED_fld_pos_num ,FEED_sch_name ,FEED_fld_name ,FEED_fld_desc ,FEED_fld_data_typ,trg_fld_data_typ ,fld_null_flg ,tgt_tbl_prtn_flg ,
		pii_flg  ,fxd_fld_strt_idx ,fxd_fld_end_idx ,fxd_fld_len ,pkey ,CREATED_DATE ,UPDATED_DATE ,CREATED_BY ,UPDATED_BY) 
		select %d as PUB_FEED_SEQUENCE, 
		split_part(src_table, '.',1) as FEED_TABLE_id, COLUMN_POS as FEED_fld_pos_num, null as FEED_sch_name, 
		SRC_PHY_ATTRIBUTE_NAME as FEED_fld_name, SRC_PHY_ATTRIBUTE_NAME as FEED_fld_desc , SRC_PHY_ATTRIBUTE_TYPE as FEED_fld_data_typ , """ % (tableName, pub_feed_id)
	logger.info(insertFieldEntry)
	insertFieldEntry1 = getDataTypeMappingQuery(pub_feed_id,"TRIM(regexp_replace(UPPER(SRC_PHY_ATTRIBUTE_TYPE), '\\(.*?\\)$', ''))",postgresPool)
	insertFieldEntry2 = """ as trg_fld_data_typ ,  null as fld_null_flg ,  null as tgt_tbl_prtn_flg ,  null as pii_flg ,  null as fxd_fld_strt_idx ,  null as fxd_fld_end_idx ,  null as fxd_fld_len,  null as  pkey, '%s' as CREATED_DATE , '%s' as UPDATED_DATE, '%s' as CREATED_BY, '%s' as UPDATED_BY  from JUNIPERX.JUNIPER_EXT_FIELD_MASTER_VW where FEED_SEQUENCE =%d""" % (str(datetime.datetime.now()), str(datetime.datetime.now()), '', '', feed_unique_name)
	insertFieldEntry = insertFieldEntry + ' ' + insertFieldEntry1 + ' ' + insertFieldEntry2
	logger.info(insertFieldEntry)
	insert_records(insertFieldEntry,postgresPool)
	#insertRowNum(pub_feed_id, ext_feed_id,postgresPool)


def insertRowNum(pub_feed_id, ext_feed_id,postgresPool):
	tableName = "JUNIPERX.JUNIPER_PUB_FEED_FLD_DTLS"
	query = """ select t1.COLUMNS,t1.TABLE_NAME, t2.fcount, t2.FEED_SRC_TYPE from JUNIPERX.JUNIPER_EXT_TABLE_MASTER t1,(select FEED_TABLE_ID, max(FEED_FLD_POS_NUM) as fcount, FEED_SRC_TYPE from JUNIPERX.JUNIPER_PUB_FEED_FLD_DTLS fi, JUNIPERX.JUNIPER_PUB_FEED_DTLS f where fi.PUB_FEED_SEQUENCE = f.PUB_FEED_SEQUENCE and upper(f.FEED_SRC_TYPE) != 'DB2' and fi.PUB_FEED_SEQUENCE=%d group by fi.FEED_TABLE_ID,f.FEED_SRC_TYPE ) t2 where t1.TABLE_NAME=t2.FEED_TABLE_ID and t1.FEED_SEQUENCE=%d """ % (pub_feed_id, ext_feed_id)
	res = fetch_records(query,postgresPool)
	for rs in res:
		if rs[0] == 'all' and rs[3] != 'Teradata' and rs[3] != 'Oracle':
			insertFieldEntry = """ insert into %s (PUB_FEED_SEQUENCE,FEED_TABLE_id ,FEED_fld_pos_num ,FEED_sch_name ,FEED_fld_name ,FEED_fld_desc ,FEED_fld_data_typ,trg_fld_data_typ ,fld_null_flg ,tgt_tbl_prtn_flg ,pii_flg  ,fxd_fld_strt_idx ,fxd_fld_end_idx ,fxd_fld_len ,pkey ,CREATED_DATE ,UPDATED_DATE ,CREATED_BY ,UPDATED_BY) values (   %d  , '%s',%d,  null , 'RNUM' , 'RNUM' , 'VARCHAR2' , 'STRING' ,  null ,  null  ,  null  ,  null  ,  null  ,  null  ,  null , '%s' , '%s' , ' ' , ' ' )""" % (tableName, pub_feed_id, rs[1], rs[2] + 1, str(datetime.datetime.now()), str(datetime.datetime.now()))
			logger.info(insertFieldEntry)
			insert_records(insertFieldEntry,postgresPool)


if __name__ == '__main__':
	import sys
	import datetime
	import subprocess
	feed_unique_name = sys.argv[1]
	gcp_details = sys.argv[2]
	run_ids = sys.argv[3]
	juniper_project_id_type = sys.argv[5]
	juniper_project_id=juniper_project_id_type.split(':')[0]
	job_type=juniper_project_id_type.split(':')[1]
	pub_name=juniper_project_id_type.split(':')[2]
	logFilePath = config.get('Logger', 'logFilePath')
	logFilename = "{0}/{1}_publishing_{2}.log".format(logFilePath, feed_unique_name,
	datetime.datetime.now().strftime("%d-%b-%y_%H:%M:%S"))
	logger = bqLogger(logFilePath, logFilename)
	decryptPasswd = get_decrypted_password(config.get('DatabaseSection', 'database.password'))
	if feed_unique_name != '' and gcp_details != '' and juniper_project_id != '' and run_ids != '' and job_type != '':
		logger.info('#' * 20 + 'Initiated BQ job for feed : {0}'.format(feed_unique_name) + '#' * 20)
		logger.info("Arguments Received are : {}  {}  {}  {}  {}".format(feed_unique_name, gcp_details, juniper_project_id,run_ids, juniper_project_id_type))
	else:
		logger.warn('Inputs are not Valid, Please Check')
		logger.info("{} ## {} ## {} ## {} ## {}".format(feed_unique_name, gcp_details, juniper_project_id, run_ids, job_type))
		sys.exit(1)
	
	gcp = gcp_details.split(':')
	gcp_project_id = gcp[0]
	gcp_dataset_id = gcp[1]
	gcp_sa_name = gcp[2]
	schMasterTbl = 'JUNIPER_SCH_MASTER_JOB_DETAIL'
	# fetch Publishing Feed Details
	# TODO: Fetch All details Here
	if job_type == 'adhoc':
		pub_feed_details_qry = """
		select PUB_FEED_SEQUENCE , SERVICE_ACC_NAME, GCP_SEQUENCE,EXT_FEED_SEQUENCE,FEED_SRC_TYPE from JUNIPERX.JUNIPER_PUB_FEED_DTLS p, JUNIPERX.JUNIPER_EXT_GCP_MASTER g , JUNIPERX.JUNIPER_EXT_FEED_MASTER f where p.EXT_FEED_SEQUENCE=f.feed_sequence and p.PROJECT_SEQUENCE = g.PROJECT_SEQUENCE and p.service_acc_name=g.service_account_name and p.GCP_PROJ_NAME='%s' and p.gcp_proj_name=g.gcp_project and p.FEED_TGT_DS='%s' and f.feed_unique_name='%s' and p.run_id='%s' and p.pub_name='%s'
		""" % (gcp_project_id, gcp_dataset_id, feed_unique_name,run_ids,pub_name)
	else:
		pub_feed_details_qry = """
		select PUB_FEED_SEQUENCE , SERVICE_ACC_NAME, GCP_SEQUENCE,EXT_FEED_SEQUENCE,FEED_SRC_TYPE from JUNIPERX.JUNIPER_PUB_FEED_DTLS p, JUNIPERX.JUNIPER_EXT_GCP_MASTER g , JUNIPERX.JUNIPER_EXT_FEED_MASTER f where p.EXT_FEED_SEQUENCE=f.feed_sequence and  p.PROJECT_SEQUENCE = g.PROJECT_SEQUENCE and p.service_acc_name=g.service_account_name and p.GCP_PROJ_NAME='%s' and p.gcp_proj_name=g.gcp_project and p.FEED_TGT_DS='%s' and f.feed_unique_name='%s' and p.pub_name='%s'
		""" % (gcp_project_id, gcp_dataset_id, feed_unique_name,pub_name)
	
	logger.info("""publishing feed details Query""".format(pub_feed_details_qry))
	postgresPool=createConnectionPool();
	if(postgresPool == "NA"):
		logger.exception("Cannot get connection from database. Nothing executed.")
		sys.exit(1)
	
	pub_feed_details_info = fetch_records(pub_feed_details_qry,postgresPool)
	if(len(pub_feed_details_info) == 0):
		logger.exception("The above query retured no result meaning there is something wrong with the feed being created. Please validate using the above query")
		sys.exit(1)
	pub_feed_seq = pub_feed_details_info[0][0]
	gcp_sequence = []
	i = 0
	for row in pub_feed_details_info:
		gcp_sequence.append(pub_feed_details_info[i][2])
		i=i+1
	
	#gcp_sequence = pub_feed_details_info[0][2]
	ext_feed_id = pub_feed_details_info[0][3]
	src_type = pub_feed_details_info[0][4]
	gcp_sequence_list= ",".join(map(str,gcp_sequence))
	#updateFeedDetails()
	target_details_qry = """select target_unique_name, 'gs://'|| hdp_hdfs_path||'/' || cust_tgt_path as custom_path, coalesce(cust_tgt_flag,'N') as cust_tgt_flag
		from JUNIPERX.juniper_ext_feed_master extfeed
		inner join juniperx.juniper_ext_feed_src_tgt_link link on extfeed.feed_sequence=link.feed_sequence
		inner join juniperx.juniper_ext_target_conn_master tgt on tgt.target_conn_sequence=link.target_sequence
		left join juniperx.juniper_ext_customize_target custtgt on tgt.target_conn_sequence=custtgt.target_sequence and extfeed.feed_Sequence=custtgt.feed_sequence
		where target_type='GCS' and feed_unique_name='%s' and gcp_sequence in (%s); 
		""" % (feed_unique_name, gcp_sequence_list)
	target_details = fetch_records(target_details_qry,postgresPool)
	target_name=target_details[0][0]
	custom_target_path=target_details[0][1]
	custom_target_flag=target_details[0][2]
	
	if job_type == 'batch':
		logger.info('Batch Job'+str(feed_unique_name)+', GCP Project Id:'+str(gcp_project_id))
		# fetch the latest run_id and process the job
	
		max_run_id_qry = """with 
		extraction as 
		(select feed_unique_name,run_id from juniperx.juniper_ext_nifi_status 
		where job_type='W' and upper(status)='SUCCESS' and FEED_UNIQUE_NAME='%s'   group by feed_unique_name,run_id),
		publishing as 
		(select FEED_UNIQUE_NAME,RUN_ID from JUNIPERX.JUNIPER_PUB_FEED_STATUS n , 
		(select PUB_FEED_SEQUENCE 
		from JUNIPERX.JUNIPER_PUB_FEED_DTLS where GCP_PROJ_NAME='%s' and FEED_TGT_DS='%s') n1 
		where n.PUB_FEED_SEQUENCE= n1.PUB_FEED_SEQUENCE
		and upper(status)='SUCCESS' and FEED_UNIQUE_NAME='%s'  group by  FEED_UNIQUE_NAME,RUN_ID)
		select MAX(extraction.RUN_ID) FROM extraction LEFT JOIN 
		publishing ON extraction.feed_unique_name=publishing.feed_unique_name and extraction.RUN_ID=publishing.RUN_ID
		where publishing.RUN_ID is null 
		and cast(extraction.RUN_ID as bigint) >(select cast(COALESCE(max(publishing.RUN_ID),'0') as bigint) from publishing ) ; 
		""" % (feed_unique_name, gcp_project_id, gcp_dataset_id,feed_unique_name)
		max_run_id = fetch_records(max_run_id_qry,postgresPool)
		run_ids = max_run_id[0][0]
		logger.info("Max RunId for this Batch Job is {}".format(run_ids))
		# TODO: Check field and file details populated for this feed
	
		if run_ids != sys.argv[3]:
			logger.info('The runid passed as argument and max run id present are different. Verify and run as adhoc')
			if(postgresPool):
				logger.info('Closing the created postgres pool')
				postgresPool.closeall
			sys.exit(1)
		if run_ids == None:
			logger.info('Exiting Script,Recent Runid Published Already')
			if(postgresPool):
				logger.info('Closing the created postgres pool')
				postgresPool.closeall
			sys.exit(1)
	
		populate_file_field_details(pub_feed_seq, juniper_project_id, ext_feed_id, '',postgresPool)
	
	logger.info('GCP Sequence For This Feed : ' + str(gcp_sequence[0]))
	sa_file_path = os.path.dirname(os.path.realpath(__file__)) + '/JUNIPER_EXT_GCP_MASTER_' + str(gcp_sequence[0]) + '_SERVICE_ACCOUNT_KEY_'+str(uuid.uuid4())+'.txt'
	try:
		payload = {'projectName': 'NA', 'serviceAccountEmail': 'NA', 'bucketName': 'NA', 'targetName': 'NA', 'outputPath': sa_file_path, 'gcpSequence': gcp_sequence[0]}
		proxies = {
		"http": None,
		"https": None,
		}
		r = requests.post(config.get('DecryptionJar', 'decription_url'),proxies=proxies, data=payload)
	except Exception as e:
		logger.exception('Exception in Decrypting key')
	if r.status_code != 200:
		logger.info('GCP Service Account Credential not Available for this Feedi :Exiting the job')
		if (postgresPool):
			postgresPool.closeall
		sys.exit(1)
	
	try:
		client = bq_client(gcp_project_id, sa_file_path)
	except Exception as e:
		count = 0;
		if "protocol" in str(e):
			if count != 5:
				count = count + 1
				client = bq_client(gcp_project_id, sa_file_path)
		else:
			raise Exception
	create_dataset(client, gcp_dataset_id)
	for run_id in run_ids.split(','):
		logger.info('#' * 20 + " Job started for RUN ID : {0} ".format(str(run_id)) + '#' * 20)
		job_start_time = datetime.datetime.now()
		status = process_load_job(client, run_id, pub_feed_seq, gcp_dataset_id, feed_unique_name, src_type, job_type, target_name,custom_target_flag,custom_target_path, ext_feed_id,postgresPool)
		job_end_time = datetime.datetime.now()
		if status != 'no-update':
			update_feed_published_status(pub_feed_seq, feed_unique_name, run_id, job_start_time, job_start_time,job_end_time, status,postgresPool)
			logger.info('Run Id - {0} Published Successfully'.format(run_id))
			if status == 'failed':
				if (postgresPool):
					postgresPool.closeall
				sys.exit(1)
		else:
			logger.info('No Tables Configured for this Feed or The Run Id-' + run_id + ' is already Published to BQ'.format(run_id))
	if (postgresPool):
		logger.info('Execution completed, Closing the connection pool')
		postgresPool.closeall
