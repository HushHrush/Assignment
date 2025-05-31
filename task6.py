def run_task(df1,df2,df3):
	from pyspark.sql.functions import col,regexp_extract,sum,count,round,format_number,split,desc,row_number,rank,when,regexp_count,lit,trim,concat
	from pyspark.sql import Window
	from pyspark.sql import SparkSession,DataFrame
	### Output #6 - **Who is the best overall salesperson per country**
	df_filter3=(df1.join(df2,"id")\
	.withColumnRenamed("id","caller_id")\
	.join(df3,"caller_id")\
	.selectExpr("caller_id as ID","name","country","calls_successful","calls_made","product_sold","sales_amount"))
	window_spec=Window.partitionBy("ID","COUNTRY")
	df_filter4=(df_filter3.withColumn("TOTAL_SALES",sum("sales_amount").over(window_spec)).withColumn("TOTAL_SUCCESSFUL_CALLS",format_number(sum("calls_successful").over(window_spec)/sum("calls_made").over(window_spec)*100,2)).selectExpr("ID","name","country","TOTAL_SALES","calls_made","calls_successful","TOTAL_SUCCESSFUL_CALLS").distinct())
	window_spec2=Window.partitionBy("country").orderBy(desc("TOTAL_SALES"),desc("TOTAL_SUCCESSFUL_CALLS"))
	df_final_filter=(df_filter4.withColumn("PRIORITY",rank().over(window_spec2))\
	.filter("PRIORITY='1'")
	.select("ID","NAME","COUNTRY"))
	name='best_salesperson'
	return  df_final_filter,name
