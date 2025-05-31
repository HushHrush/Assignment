"""
Import the required libraries
"""
import os
from pyspark.sql.functions import col,regexp_extract,sum,count,round,format_number,split,desc,row_number,rank,when,regexp_count,lit,trim,concat
from pyspark.sql import Window,SparkSession,DataFrame
import sys
import logging
import pyspark.pandas as ps
from pyspark.sql.types import NumericType
from chispa.dataframe_comparer import assert_df_equality
from tasks import task1,task2,task3,task4,task5,task6

expected_counts={"dataset_one":1000,"dataset_two":1000,"dataset_three":10000}
# Logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s-%(levelname)s-%(message)s')
logger=logging.getLogger(__name__)
input_one="C:/Users/C85463/ASSIGNMENT/dataset_one.csv"
input_two="C:/Users/C85463/ASSIGNMENT/dataset_two.csv"
spark=SparkSession.builder.appName("Task1").getOrCreate()
df1=spark.read.option("header",'True').csv(input_one)
df2=spark.read.option("header",'True').csv(input_two)

def validate_data(df:DataFrame,dataset_name:str,expected_row_count:int)->list[str]:
    warnings=[]
    logger.info(f"Validating dataset: {dataset_name}")

    #Check 1 - Null check
    if df.filter(col("id").isNull()).count()>0:
        warning=f"{dataset_name}: 'id' field contains NULL values."
        logger.warning(warning)
        warnings.append(warning)

    #check Unique constraint
    total_count=df.count()
    distinct_id_count=df.select("id").distinct().count()
    if total_count!=distinct_id_count:
        warning=f"{dataset_name}: 'id' field is not unique."
        logger.warning(warning)
        warnings.append(warning)

    #Rows Check
    if total_count!=expected_row_count:
        warning=f"{dataset_name}: Expected {expected_row_count} rows , but got {total_count}."
        logger.warning(warning)
        warnings.append(warning)    

    #Numeric Value check
    numeric_fields=[f.name for f in df.schema.fields if isinstance(f.dataType,NumericType) and f.name!='id']
    for col_name in numeric_fields:
        if df.filter(col(col_name)<0).count()>0:
            warning=f"{dataset_name}: Columm '{col_name}' has values less than 0."
            logger.warning(warning)
            warnings.append(warning)

    return warnings

def datacheck(spark:SparkSession,file_path:str)->None:
    logger.info(f"Basic Data Check for file :{file_path}")
    try :
        df=spark.read.option("header","true").option("inferschema","true").csv(file_path)
        dataset_name=file_path.split("/")[-1].split(".")[0].lower()
        expected_row_count=expected_counts.get(dataset_name,None)

        if expected_row_count is None:
            logger.error(f"Unknown dataset name '{dataset_name}'. Expected one of : {list(expected_counts.keys())}")
            return

        warnings=validate_data(df,dataset_name,expected_row_count)

        if warnings:
            logger.warning(f"{len(warnings)} issues found in dataset '{dataset_name}'")
        else:
            logger.info(f"No issues found in dataset '{dataset_name}'")

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")



def write_output(df,input_path:str,name)->None:
    base_dir=os.path.dirname(os.path.abspath(input_path))
    output_path=os.path.join(base_dir,name)
    output_file=os.path.join(output_path,name)
    output_file=output_file.replace("\\","/")
    print(output_file)
    os.makedirs(output_path,exist_ok=True)
    if os.path.exists(output_file):
        os.remove(output_file)
        logger.info(f"Deleted existing output file to overwrite")
    try:
        logger.info(f"Writing output to: {output_file}")
        psdf=df.toPandas()
        psdf.to_csv(output_file,index=False)
        logger.info(f"File written succesfully")
    except Exception as e:
        logger.error(f"Failed to write CSV")    

        
    

def main(file_paths:list[str])->None:
    spark=SparkSession.builder.appName("TASK_1").getOrCreate()
    logger.info(f"Starting Spark Application with  :{len(file_paths)} file(s).")

    try:
        for file_path in file_paths:
            datacheck(spark,file_path)

        df1=spark.read.option("Header","TRUE").option("inferSchema","True").csv(file_paths[0])
        df2=spark.read.option("Header","TRUE").option("inferSchema","True").csv(file_paths[1])
        df3=spark.read.option("Header","TRUE").option("inferSchema","True").csv(file_paths[2])
        logger.info(f"task1 started ")
        df_out1,name1=task1.run_task(df1,df2)
        write_output(df_out1,file_paths[0],name1)
        logger.info(f"task1 completed ")
        logger.info(f"task2 started ")
        df_out2,name2=task2.run_task(df1,df2)
        write_output(df_out2,file_paths[0],name2)
        logger.info(f"task2 completed ")
        logger.info(f"task3 started ")
        df_out3,name3=task3.run_task(df1,df2)
        write_output(df_out3,file_paths[0],name3)
        logger.info(f"task3 completed ")
        logger.info(f"task4 started ")
        df_out4,name4=task4.run_task(df1,df2)
        write_output(df_out4,file_paths[0],name4)
        logger.info(f"task4 completed ")
        logger.info(f"task5 started ")
        df_out5,name5=task5.run_task(df1,df2,df3)
        write_output(df_out5,file_paths[0],name5)
        logger.info(f"task5 completed ")
        logger.info(f"task6 started ")
        df_out6,name6=task6.run_task(df1,df2,df3)
        write_output(df_out6,file_paths[0],name6)
        logger.info(f"task6 completed ")

    except Exception as e:
        logger.error(f"Error processing  files {str(e)}")


    finally:
        spark.stop()
        logger.info("Spark Application stopped") 

if __name__=="__main__":
    if len(sys.argv)==3:
        print("Usage: spark-submit task1.py file1.csv file2.csv") 
        sys.exit(1)
    input_paths=sys.argv[1:]
    main(input_paths)              




 
