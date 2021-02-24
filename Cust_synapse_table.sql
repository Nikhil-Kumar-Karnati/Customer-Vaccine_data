Drop external table sample.stg_customer;

CREATE EXTERNAL TABLE sample.stg_customer(
name varchar(255),
cust_i varchar(18),
open_dt  datetime,
consul_dt datetime,
vac_id  varchar(5),
dr_name  varchar(255),
[state] varchar(5),
country varchar(5),
post_code int,
dob datetime,
flag varchar(1)
}
WITH (
    DATA_SOURCE = [AzureDataLakeStore],
    LOCATION = N'/data/processed/stg/synapse/',
	FILE_FORMAT = [PARQUET_SNAPPY]
	
)