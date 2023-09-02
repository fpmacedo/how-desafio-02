
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

def process_cutomers(spark, input_data, output_data):

    """
    Perform ETL on orders to create the orders_silver:
    - Extract the match result data and insert in the customers table.
      
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """


    #reading json files
    order_file_Path = input_data
    
    orders_trusted=spark.read.parquet(order_file_Path)
    
    customer_data = orders_trusted.select('customer_info.customer_id', 'customer_info.customer_name', 'customer_info.customer_phone_number').distinct()

    data_quality(customer_data)

    customer_data.write.parquet(os.path.join(output_data, 'customers'), 'overwrite')

    
    print("--- customers.parquet completed ---")

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
    
    order_null = validator.expect_column_values_to_not_be_null(column="customer_id")
    order_unique = validator.expect_column_values_to_be_unique(column="customer_id")
    #date_format = validator.expect_column_values_to_match_strftime_format("date_partition", "%Y-%m-%d")
    #rows_number = validator.expect_table_row_count_to_be_between(400,600)

    
    if order_null.success == False :
      raise ValueError(f"Data quality check failed {order_null.expectation_config.kwargs['column']} is null.")
    else : logger.info(f"Data quality check success {order_null.expectation_config.kwargs['column']} is not null.")
    
    if order_unique.success == False :
      raise ValueError(f"Data quality check failed {order_unique.expectation_config.kwargs['column']} is not unique.")
    else: logger.info(f"Data quality check success {order_unique.expectation_config.kwargs['column']} is unique.")
       
    logger.info(f"All validators passed with success!")

def main():
    
    """
    Build ETL Pipeline for How desafio 2:
    
    Call the function to create a spark session;
    Instantiate the input and output paths;
    Call the process functions.
    """
    
    spark = create_spark_session()
    trusted = "s3://how-desafio/trusted/"
    business = "s3://how-desafio/business/"
    
    process_cutomers(spark, trusted, business)
    #data_quality(spark, input_data, output_data)

if __name__ == "__main__":
    main()