def run_task(df1,df2):
	from pyspark.sql.functions import col,regexp_extract,sum,count,round,format_number,split,desc,row_number,rank,when,regexp_count,lit,trim,concat
	from pyspark.sql import Window
	from pyspark.sql import SparkSession,DataFrame
	department_breakdown_df=(df1.join(df2,"id")\
	.select("id","area","sales_amount","calls_made","calls_successful")\
	.groupBy("area")\
	.agg(format_number(sum("sales_amount"),2).alias("TOTAL_SALES_AMOUNT"),format_number(sum("calls_successful")/sum("calls_made")*100,5).alias("TOTALPERCENTAGE")))
	#department_breakdown_df.show()
	name='department_breakdown'
	return department_breakdown_df,name