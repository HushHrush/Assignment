def run_task(df1,df2,df3):
	from pyspark.sql.functions import col,regexp_extract,sum,count,round,format_number,split,desc,row_number,rank,when,regexp_count,lit,trim,concat
	from pyspark.sql import Window
	from pyspark.sql import SparkSession,DataFrame
	### Output #5 - **Top 3 most sold products per department in the Netherlands**
	df_filter=df1.join(df2,"id").withColumnRenamed("id","caller_id").join(df3,"caller_id").filter("country='Netherlands'")
	w_s=Window.partitionBy("area","product_sold")
	df_filter2=(df_filter
	.withColumn("TOTAL_QUANTITY",sum("quantity").over(w_s))\
	.filter("country='Netherlands'")\
	.select("area","TOTAL_QUANTITY","country","product_sold")\
	.distinct()\
	.orderBy(desc("TOTAL_QUANTITY"))\
	)
	w_s2=Window.partitionBy("area")
	final_df1=(df_filter2
	.withColumn("PRIORITY",row_number().over(w_s2.orderBy(desc("TOTAL_QUANTITY"))))\
	.filter("PRIORITY<='3'")
	.drop("priority","total_quantity"))
	name='top_3_most_sold_per_department_netherlands'
	#final_df1.show()
	return final_df1,name