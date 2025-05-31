def run_task(df1,df2):
	from pyspark.sql.functions import col,regexp_extract,sum,count,round,format_number,split,desc,row_number,rank,when,regexp_count,lit,trim,concat
	from pyspark.sql import Window
	from pyspark.sql import SparkSession,DataFrame
	### Output #4 - **Top 3 best performers per department**
	df=(df1
	.join(df2,"id")\
	.select("id","name","area","sales_amount","calls_made","calls_successful")\
	.groupBy("ID","area")\
	.agg(
	format_number(sum("sales_amount"),2).alias("TOTAL_SALES_AMOUNT"),
	format_number(sum("calls_successful")/sum("calls_made")*100,5).alias("TOTALPERCENTAGE")
	)
	).filter("TOTALPERCENTAGE>'75'")
	window_spec=Window.partitionBy("area").orderBy(desc("TOTAL_SALES_AMOUNT"),desc("TOTALPERCENTAGE"))
	df_prio=df.withColumn("PRIORITY",row_number().over(window_spec))
	final_df=df_prio.select("ID","area","TOTAL_SALES_AMOUNT","TOTALPERCENTAGE").filter("PRIORITY<=3")
	#final_df.show()
	name='top_3'
	return final_df,name

