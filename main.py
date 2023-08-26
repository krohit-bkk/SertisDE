"""Entry point for the ETL application
# Start network
docker network create my_network

# Start Spark (master, worker) & PostgreSQL
docker compose down --rmi all &&  docker compose up -d spark spark-worker-1 postgres-db

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar

docker exec -it spark ls /opt/bitnami/spark/jars/postgresql-42.5.2.jar
docker exec -it spark-worker-1 ls /opt/bitnami/spark/jars/postgresql-42.5.2.jar
docker exec -it etl /opt/bitnami/spark/jars/postgresql-42.5.2.jar

docker rm $(docker ps -a -q --filter "name=sertisetl-etl-run") && docker image rm --force sertisetl-etl
docker compose down --rmi all
docker compose up -d spark spark-worker-1 postgres-db
docker ps -a

docker rm $(docker ps -a -q --filter "name=sertisetl-etl-run") && docker image rm --force sertisetl-etl && \
docker compose run etl poetry run python main.py --source /opt/data/transaction.csv --database prod_db --table transaction --save_mode overwrite


# Start ETL container
docker compose run etl poetry run python main.py --source /opt/data/transaction.csv --database prod_db --table transaction --save_mode overwrite

# Debug on running ETL container
docker exec -it etl poetry run python main.py

# Debug from inside ETL container
poetry run python main.py



Longest streak -> http://sqlfiddle.com/#!18/c5f72/6

Sample usage:
docker exec -it etl poetry run python main.py
docker-compose run etl poetry run python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse
  --table transactions
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, sum, row_number, dense_rank, coalesce, lit, count, unix_timestamp, datediff, desc, lpad

postgres_base_url="jdbc:postgresql://postgres-db:5432"

# Method to return PostgreSQL  properties
def get_postgres_properties():
  database_properties = {
      "user": "sertis",
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


# Read from PostgreSQL
def read_from_postgres(db_name, table_name):
  # Define PostgreSQL database connection properties
  database_url = f"{postgres_base_url}/{db_name}"
  database_properties = get_postgres_properties()
  
  # Read data from PostgreSQL table into a DataFrame
  spark = get_spark_session()
  df = spark.read \
      .format("jdbc") \
      .option("url", database_url) \
      .option("driver", "org.postgresql.Driver") \
      .option("dbtable", f"{table_name}") \
      .option("user", "sertis") \
      .option("password", "password") \
      .load()
  
  return df


# Write to PostgreSQL
def write_to_postgres(df, db_name, table_name, mode):
  # Create database if not exists
  database_url = f"{postgres_base_url}/{db_name}"
  database_properties = get_postgres_properties()
  
  # Write DataFrame to PostgreSQL table
  df.write \
    .mode(mode) \
    .format("jdbc") \
    .option("url", database_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", f"{table_name}") \
    .option("user", "sertis") \
    .option("password", "password") \
    .save()


# Get favourite_product
def get_favourite_product(df):
  # Create ranked_products DataFrame
  # total_cust_prod_sold := UnitsSold per CustId per ProductSold
  window_spec_cust_prod = Window().partitionBy("custId", "productSold").orderBy()
  ranked_products = df.withColumn(
      "custId",
      col("custId").cast("string") 
    ).withColumn(
      "total_cust_prod_sold", 
      sum(
      col("unitsSold")
      # coalesce(col("unitsSold"), lit(0))
      ).over(window_spec_cust_prod)
    )

  # Create cust_prod_rank DataFrame
  # rnk := Basis per ProductSold per CustId
  window_spec_cust = Window().partitionBy("custId").orderBy(col("total_cust_prod_sold").desc())
  cust_prod_rank = ranked_products.select(
      "custId", 
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
      "custId", 
      "productSold"
    ).where(
      col("rnk") == 1
    ).withColumnRenamed(
      "productSold",
      "favorite_product"
    )

  return result_df

# Get longest streak per custId
def get_longest_streak(df):
  # Sort transactions by transactionDate and assign row number
  window_spec_cust = Window().partitionBy("custId").orderBy("dateDiff")
  ranked_data = df.select(
      col("custId").cast("string"), 
      col("transactionDate").cast("date")
    ).dropDuplicates().withColumn(
      "dateDiff", 
      datediff(col("transactionDate"), lit("1990-01-01"))
    ).withColumn(
      "DateDifferenceGroup", 
      col("dateDiff") - row_number().over(window_spec_cust)
    )
  
  streak_data = ranked_data.groupBy(
      "custId", 
      "DateDifferenceGroup"
    ).agg(
      count(lit(1)).alias("streaks")
    ).groupBy("custId").agg(
      max(col("streaks")).alias("longest_streak")
    )
  return streak_data

# Wrapper method to encapsulate ETL
def process_etl(source_path, database_name, table_name, mode):
  # Create a Spark Session
  spark: SparkSession = get_spark_session()
  
  # Read the input file
  df = spark.read.csv(source_path, sep='|', header=True, inferSchema=True)
  # df.show()
  
  # Write to PostgreSQL
  write_to_postgres(df, database_name, table_name, mode)
  print(f">>>> Dataframe written [SaveMode: {mode}] successfully to PostgreSQL!\n")
  
  favourite_product_df = get_favourite_product(df)
  longest_streak_df = get_longest_streak(df)

  final_df = favourite_product_df.join(
     longest_streak_df,
     ['custId'],
     "full_outer"
  ).withColumn(
     "custId",
     lpad(col("custId"), 7, "0")
  ).withColumnRenamed(
     "custId",
     "customer_id"
  )

  final_df.orderBy(col("longest_streak").desc()).show(50, False)

  # Unit testing - kind of...
  cust_rec=final_df.filter(col("customer_id") == "0023938").select("favorite_product", "longest_streak").limit(1).collect()
  favourite_product=cust_rec[0][0]
  longest_streak=cust_rec[0][1]

  assert favourite_product == "PURA250", f"Favorite product is PURA250; found[{favourite_product}]"
  print("\n>>>> Favorite Product is VALID!")

  assert longest_streak == 2, f"Longest streak is 2; found[{longest_streak}]"
  print("\n>>>> Streak is VALID!")

  # Write ETL output to PostreSQL
  write_to_postgres(final_df, database_name, "customers", mode)


def main():
    parser = argparse.ArgumentParser(description="Process data.")
    parser.add_argument("--source", required=True, help="Path to the source file")
    parser.add_argument("--database", required=True, help="Database name for inserting data")
    parser.add_argument("--table", required=True, help="Table name for inserting data")
    parser.add_argument("--save_mode", required=False, choices=["append", "overwrite"], help="Save mode choices: append|overwrite")

    save_mode = "append"
    default_save_mode = True
    
    args = parser.parse_args()

    source_path = args.source
    database_name = args.database
    table_name = args.table
    
    if args.save_mode is not None:
       default_save_mode = False
       save_mode = args.save_mode
    msg = "[default]" if default_save_mode else "[user provided]"

    # Your code to process data goes here
    print()
    print(f">>>> Processing data from: {source_path}")
    print(f">>>> Database: {database_name}")
    print(f">>>> Table: {table_name}")
    print(f">>>> Save mode: {save_mode} {msg}\n")
    
    try: 
      # Process ETL
      process_etl(source_path, database_name, table_name, save_mode)
      print("\n\n>>>> ETL SUCCESSFUL!\n")

    finally:
      # Close SparkSession if still open
      close_spark_session()


if __name__ == "__main__":
    main()