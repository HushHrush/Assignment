def run_task(df1,df2):
    from pyspark.sql.functions import col,regexp_extract,sum,count,round,format_number,split,desc,row_number,rank,when,regexp_count,lit,trim,concat
    from pyspark.sql import Window
    from pyspark.sql import SparkSession,DataFrame
    joined_df=df1.join(df2,on="id",how='inner')
    filtered_df = joined_df.filter(col("area") == "IT")
    ordered_df = filtered_df.orderBy(col("sales_amount").desc())    
    top100_df = ordered_df.limit(100)
    name='it_data'
    return top100_df,name