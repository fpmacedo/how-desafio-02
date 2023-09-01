
import pyspark
from pyspark.sql.functions import date_format, col
from pyspark.sql import SparkSession
import logging
import os
import great_expectations as gx
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_spark_session():
    
    """
    Create the spark session with the passed configs.
    """
    
    spark = SparkSession \
        .builder \
        .appName("How-Desafio-02")\
        .getOrCreate()

    return spark

def process_order(spark, input_data, output_data):

    """
    Perform ETL on orders to create the orders_silver:
    - Extract the match result data and insert in the match_results table.
      
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """


    #reading json files
    order_file_Path = input_data

    orders_df = (spark.read
                  .option("inferSchema", True)
                  .json(order_file_Path))
    
    orders_df_partition = orders_df.withColumn('date_partition', date_format(col('order_created_at'), "yyyy-MM-dd"))

    data_quality(orders_df_partition)

    orders_df_partition.write.partitionBy('date_partition').parquet(os.path.join(output_data, 'orders'), 'overwrite')

    
    print("--- orders.parquet completed ---")


def data_quality(input_dataset):
    
    gx_context = gx.get_context()
    datasource = gx_context.sources.add_spark("my_spark_datasource")

    data_asset = datasource.add_dataframe_asset(name="my_df_asset", dataframe=input_dataset).build_batch_request()
    
    gx_context.add_or_update_expectation_suite("my_expectation_suite")
    
    #my_batch_request = data_asset
    
    validator = gx_context.get_validator(
    batch_request=data_asset,
    expectation_suite_name="my_expectation_suite"
                                        )
    
    order_null = validator.expect_column_values_to_not_be_null(column="order_id")
    order_unique = validator.expect_column_values_to_be_unique(column="order_id")
    date_format = validator.expect_column_values_to_match_strftime_format("date_partition", "%Y-%m-%d")
    rows_number = validator.expect_table_row_count_to_be_between(400,600)

    
    if order_null.success == False :
      raise ValueError(f"Data quality check failed {order_null.expectation_config.kwargs['column']} is null.")
    
    elif order_unique.success == False :
      raise ValueError(f"Data quality check failed {order_unique.expectation_config.kwargs['column']} is not unique.")
    
    elif date_format.success == False :
      raise ValueError(f"Data quality check failed {date_format.expectation_config.kwargs['column']} is not in {date_format.expectation_config.kwargs['strftime_format']} format.")
    
    #elif rows_number.success == False :
    #  raise ValueError(f"Data quality check failed number of rows is not between {rows_number.expectation_config.kwargs['min_value']} and {rows_number.expectation_config.kwargs['max_value']}.")
    
    else: logger.info(f"All validators passed with success!")

def main():
    
    """
    Build ETL Pipeline for How desafio 2:
    
    Call the function to create a spark session;
    Instantiate the input and output paths;
    Call the process functions.
    """
    
    spark = create_spark_session()
    input_data = "s3://how-desafio/raw/*/*.json"
    output_data = "s3://how-desafio/trusted"
    
    process_order(spark, input_data, output_data)  
    #data_quality(spark, input_data, output_data)

if __name__ == "__main__":
    main()