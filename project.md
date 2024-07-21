# project

A. Read Different File Formats from ADLS Gen2 Using SQL Syntax
CREATE OR REPLACE TEMPORARY VIEW csv_table
USING csv
OPTIONS (
  path "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/csvfile.csv",
  header "true",
  inferSchema "true"
);

SELECT * FROM csv_table;

#parquet sql

CREATE OR REPLACE TEMPORARY VIEW parquet_table
USING parquet
OPTIONS (
  path "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/parquetfile.parquet"
);

SELECT * FROM parquet_table;

#Avro
CREATE OR REPLACE TEMPORARY VIEW avro_table
USING avro
OPTIONS (
  path "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/avrofile.avro"
);

SELECT * FROM avro_table;

#ORC
CREATE OR REPLACE TEMPORARY VIEW orc_table
USING orc
OPTIONS (
  path "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/orcfile.orc"
);

SELECT * FROM orc_table;

#Delta
CREATE OR REPLACE TEMPORARY VIEW delta_table
USING delta
OPTIONS (
  path "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/deltafile"
);

SELECT * FROM delta_table;

 #Read Different File Formats from ADLS Gen2 Using Python Syntax
 
 #python

 # Mount ADLS Gen2 if not already mounted
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<client-id>",
  "fs.azure.account.oauth2.client.secret": "<client-secret>",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs
)

# CSV
df_csv = spark.read.csv("abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/csvfile.csv", header=True, inferSchema=True)
df_csv.show()

# Parquet
df_parquet = spark.read.parquet("abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/parquetfile.parquet")
df_parquet.show()

# Avro
df_avro = spark.read.format("avro").load("abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/avrofile.avro")
df_avro.show()

# ORC
df_orc = spark.read.orc("abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/orcfile.orc")
df_orc.show()

# Delta
df_delta = spark.read.format("delta").load("abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/path/to/your/deltafile")
df_delta.show()

 
