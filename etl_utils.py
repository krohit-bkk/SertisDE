import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, sum, row_number, dense_rank, coalesce, lit, count, unix_timestamp, datediff, desc, lpad

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s [%(levelname)s] - %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S"
)

# PostgreSQL URL
postgres_base_url="jdbc:postgresql://postgres-db:5432"

# Method to return PostgreSQL  properties
def get_postgres_properties():
  database_properties = {
    "username": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
  }
  return database_properties


# Create or Get SparkSession
def get_spark_session():
  spark = SparkSession.builder \
    .master('spark://spark:7077') \
    .appName("Sertis ETL") \
    .config("spark.jars", "/opt/postgresql-42.5.2.jar") \
    .getOrCreate()
  
  spark.sparkContext.setLogLevel("WARN")
  return spark 


# Close SparkSession
def close_spark_session():
   get_spark_session().stop()


# Read PSV as dataframe
def read_psv(spark, source_path):
  # return spark.read.csv(source_path, sep='|', header=True, inferSchema=True)
  return spark \
    .read \
    .csv(source_path, sep='|', header=True) \
    .withColumn(
      "unitsSold", 
      col("unitsSold").cast("integer")
    )


# Read from PostgreSQL
def read_from_postgres(db_name, table_name):
  # Define PostgreSQL database connection properties
  database_properties = get_postgres_properties()
  database_url = f"{postgres_base_url}/{db_name}"
  
  print(f"\n>>>> USERNAME: {database_properties.get('username')}; PASSWORD: {database_properties.get('password')}")
  # Read data from PostgreSQL table into a DataFrame
  spark = get_spark_session()
  df = spark.read \
    .format("jdbc") \
    .option("url", database_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", f"{table_name}") \
    .option("user", f"{database_properties.get('username')}") \
    .option("password", f"{database_properties.get('password')}") \
    .load()
  
  return df


# Write to PostgreSQL
def write_to_postgres(df, db_name, table_name, mode):
  # Define PostgreSQL database connection properties
  database_url = f"{postgres_base_url}/{db_name}"
  database_properties = get_postgres_properties()
  
  # Write DataFrame to PostgreSQL table
  df.write \
    .mode(mode) \
    .format("jdbc") \
    .option("url", database_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", f"{table_name}") \
    .option("user", f"{database_properties.get('username')}") \
    .option("password", f"{database_properties.get('password')}") \
    .save()


# Get favourite_product
def get_favourite_product(df):
  # Create ranked_products DataFrame
  # LOGIC --> total_cust_prod_sold := UnitsSold per CustId per ProductSold
  window_spec_cust_prod = Window().partitionBy("customer_id", "productSold").orderBy()
  ranked_products = df.withColumn(
      "total_cust_prod_sold", 
      sum(
        col("unitsSold")
      ).over(window_spec_cust_prod)
    )

  # Create cust_prod_rank DataFrame
  # LOGIC --> rnk := Basis per ProductSold per CustId
  window_spec_cust = Window().partitionBy("customer_id").orderBy(col("total_cust_prod_sold").desc())
  cust_prod_rank = ranked_products.select(
      "customer_id", 
      "productSold",
      "total_cust_prod_sold"
    ) \
    .dropDuplicates() \
    .withColumn(
      "rnk", 
      dense_rank().over(window_spec_cust)
    )

  # Select the desired columns
  result_df = cust_prod_rank.select(
      col("customer_id"),
      col("productSold").alias("favorite_product")
    ).where(
      col("rnk") == 1
    )

  return result_df

# Get longest streak per custId
def get_longest_streak(df):
  # Sort transactions by transactionDate and assign row number
  window_spec_cust = Window().partitionBy("customer_id").orderBy("dateDiff")
  ranked_data = df.select(
      col("customer_id"), 
      col("transactionDate").cast("date")
    ).dropDuplicates().withColumn(
      "dateDiff", 
      datediff(col("transactionDate"), lit("1990-01-01"))
    ).withColumn(
      "DateDifferenceGroup", 
      col("dateDiff") - row_number().over(window_spec_cust)
    )
  
  streak_data = ranked_data.groupBy(
      "customer_id", 
      "DateDifferenceGroup"
    ).agg(
      count(lit(1)).alias("streaks")
    ).groupBy("customer_id").agg(
      max(col("streaks")).alias("longest_streak")
    )
  return streak_data


# Create ETL output
def get_etl_output(favourite_product_df, longest_streak_df):
  result_df = favourite_product_df.join(
      longest_streak_df,
      ['customer_id'],
      "full_outer"
    ).withColumn(
      "customer_id",
      lpad(col("customer_id"), 7, "0")
    ).withColumnRenamed(
      "customer_id",
      "customer_id"
    )
  return result_df

# Wrapper method to encapsulate ETL
def process_etl(source_path, database_name, table_name, mode):
  # Create a Spark Session
  spark: SparkSession = get_spark_session()
  
  # Read the input file
  # df = spark.read.csv(source_path, sep='|', header=True, inferSchema=True)
  df = read_psv(spark, source_path).withColumnRenamed("custId", "customer_id")
  # df.show()
  
  # Write to PostgreSQL
  write_to_postgres(df, database_name, "transaction", mode)
  logging.info(f">>>> Raw Data [transactions] written [SaveMode: {mode}] successfully to PostgreSQL!\n")
  
  # Actual ETL processing
  favourite_product_df = get_favourite_product(df)
  longest_streak_df = get_longest_streak(df)
  final_df = get_etl_output(favourite_product_df, longest_streak_df)

  # Display the output of ETL
  final_df.orderBy(col("longest_streak").desc()).show(50, False)

  # Write output to Postgres
  write_to_postgres(final_df, database_name, table_name, mode)

