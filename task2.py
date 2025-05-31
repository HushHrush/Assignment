def run_task(df1,df2):
    from pyspark.sql.functions import col,regexp_extract,sum,count,round,format_number,split,desc,row_number,rank,when,regexp_count,lit,trim,concat
    from pyspark.sql import Window
    from pyspark.sql import SparkSession,DataFrame
    # ### Output #2 - **Marketing Address Information**
    address_df=(df2
    .withColumn("MODIFIED_ADDRESS",when(regexp_count("address",lit(","))==1,trim(split(df2["address"],',').getItem(1))).otherwise(trim(concat(split(df2["address"],',').getItem(0),split(df2["address"],',').getItem(2)))))\
    .withColumn("ZIP_CODE",when(regexp_count("address",lit(","))==1,trim(split(df2["address"],',').getItem(0))).otherwise(trim(split(df2["address"],',').getItem(1))))\
    .select("id","MODIFIED_ADDRESS",'zip_code')\
    ).join(
    df1,"id"  
    ).filter("area='Marketing'").select("MODIFIED_ADDRESS","ZIP_CODE")
    #address_df.show()
    name='marketing_address_info'
    return address_df,name
