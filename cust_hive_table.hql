drop table if exists  sample.stg_customer ;
create external table if not exists sample.stg_customer(
name string,
cust_i string,
open_dt  timestamp,
consul_dt timestamp,
vac_id  string,
dr_name  string,
state string,
country string,
post_code int,
dob timestamp,
flag string
)

stored as parquet 
location '/dbfs/tmp/cust/stg_customer'