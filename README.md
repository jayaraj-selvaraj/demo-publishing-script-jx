# demo-publishing-script-jx

***Script used to publish from GCS to Bigquery***

***Arguments taken :
  
  a) Feed Unique Name : Unique name of the extraction feed
  b) Gcp Project Details In the Form :  gcp_project_id:gcp_dataset_id:gcp_serviceaccountname
  c) Run ids (Comma seperated) : Comma seperated list of runids
  d) Dummy : Any dummy value (Currently being modifed. This is due to making it compatible with other schdeuling in scheduler)
  e) JuniperProjectDetails : ProjectId:JobType(batch or adhoc):Publishing feed unique name (Can be found in juniperx.juniper_pub_feed_dtls)

***Enhancement :

  1) The script will truncate a table in case of batch mode and fead load type is truncate and table contains 0 records.
  2) In case of emptry file/table on the first day empty table will be created
  3) The default AVRO mapping used by bigquery is used to create table in case of data file.
  4) In case of an empty table only the mapping specified in mstr_datatype_link_dtls table is used.
  
  ***New Feature :
  
  1) Supports automated column addition to bigquery
  2) Supports changes to extraction, Previously if you edit any extraction feed it was not included in publishing automatically, That is supported now
  3) Previosuly recon counts were available only in publishing dashboard but now it can be viewed in HIP also.
  
  ***Updates on (18/05/2020) :
  
  1) Supports for date older then avro supported date of 1400. You can bring dates as String by using custom datatype in teradata and db2 while publishing it will be casted back to date again.
  2)Supports for trimming character from String fields. Just select the button in UI in case you want to trim the fields.
  
  ***Updates on (15/06/2020) :
  1) Support for date older then avro supported date of 1400 is now available for file pattern as well. Bring date as logical-date;string and it will be casted back as date.
  
  ----------------------------------------------------------------
  
  **Libraries needed for running this script :
  
1)  psycopg2-binary
2)  configparser
3)  pycryptodomex
4)  pycrypto
5)  google-cloud-bigquery

  -----------------------------------------------------------------
  
  **Steps to generated encrypted password and metadata_priv.pem used to decrypt the password.
  
  1) Open the crypto_test_new.py file
  2) On line 26 in place of ***** enter the password you need to encrypt
  3) Run the python code without passing any argument. It will generate a encrypted password and metadata_priv_pg.pem. Place the password in bq.properties file in the property value of database.password
  4) Place the metadata_priv_pg.pem at any secure location and add it's path in metadata_privpem (bq.properties file)
  
  
  ----------------------------------------------------------------
  
  **Properties file values to be updated :
  
[DatabaseSection]
1)  database.url=***postgres database url***
2)  database.user=***database username***
3)  database.password=***explained in the above section point 3***
4)  database.port=***database port***
5)  database.dbname=***database name***

[DecryptionJar]
1)  decription_url=http://<<servername>>:9095/decryptionGcs (url and endpoint of decryptionGcs Service) https://github.com/JuniperX/JuniperX/tree/development/juniper-gcs-decryption (postgres-dev or latest branch)
2)  metadata_privpem=***Explained in the above section point 4***

[Proxy]
1)  http.proxy=http://<<proxy if any>> ***google cloud proxy details***
2)  https.proxy=https://<<proxy if any>> ***google cloud proxy details***
  
[Logger]
1)  logFilePath=***path to create log file***
