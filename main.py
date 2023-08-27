"""Entry point for the ETL application
# Start network
docker network create my_network

# Start Spark (master, worker) & PostgreSQL
docker compose down --rmi all && docker compose up -d spark spark-worker-1 postgres-db

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar

docker exec -it spark ls /opt/bitnami/spark/jars/postgresql-42.5.2.jar
docker exec -it spark-worker-1 ls /opt/bitnami/spark/jars/postgresql-42.5.2.jar
docker exec -it etl /opt/bitnami/spark/jars/postgresql-42.5.2.jar

docker exec -it spark-worker-1 pyspark --master=spark://spark:7077 --jars //opt/bitnami/spark/jars/postgresql-42.5.2.jar

docker rm $(docker ps -a -q --filter "name=sertisetl-etl-run") && docker image rm --force sertisetl-etl
docker compose down --rmi all
docker compose up -d spark spark-worker-1 postgres-db
docker ps -a

# RUN ETL
docker rm --force $(docker ps -a -q --filter "name=sertisetl-etl-run") && docker image rm --force sertisetl-etl && \
docker compose run etl poetry run python main.py --source /opt/data/transaction.csv --database prod_db --table transaction --save_mode overwrite

# TEST ETL
docker rm --force $(docker ps -a -q --filter "name=sertisetl-etl-run") && docker image rm --force sertisetl-etl && \
docker compose run etl poetry run python -m unittest discover -s /opt/tests
docker-compose run etl poetry run python -m unittest discover -s /opt/tests


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
from etl_utils import close_spark_session, process_etl

def main():
    # Validate arguments
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