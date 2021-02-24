
  
"""
Creating dataframe by dropping header & Detail column name and creating Dataframe , creating view to query it & change the column names
final df is stored as parquet

"""

df=spark.read.csv(path='/FileStore/shared_uploads/Customer_data.txt',sep='|',header=True).drop('H','_c0')

#df.show()
df.createOrReplaceTempView("cust_view")
final_df=spark.sql("""select 
      Customer_Name as Name,
      Customer_Id as Cust_I,
      to_date(Open_Date,'yyyymmdd') as Open_Dt ,
      to_date(Last_Consulted_Date,'yyyymmdd') as Consul_Dt,
      Vaccination_Id as VAC_ID ,
      Dr_Name as DR_Name,
      State,Country,
      to_date(DOB,'ddmmyyyy') as DOB,
      Is_active as FLAG
      from cust_view     """)
final_df.write.format("parquet").mode('overwrite').option('path','/dbfs/tmp/cust/stg_customer').saveAsTable('sample.stg_customer_test')

#final_df.show()
"""
Creating temp view and loading the data into table india and saving it in parquet format in hive external location

"""
final_df.createOrReplaceTempView("final_view")

countrywise_df=spark.sql("select * from final_view where country='IND'")
countrywise_df.write.format("parquet").mode('overwrite').option('path','/dbfs/tmp/cust/stg_countrywise').saveAsTable('sample.Table_Ind')

#countrywise_df.show()

