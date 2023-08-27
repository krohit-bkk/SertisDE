"""Entry point for the ETL application
# Start network
docker network create my_network

# Start Spark (master, worker) & PostgreSQL
docker compose down --rmi all && 
docker compose up -d spark spark-worker-1 postgres-db

# RUN ETL
docker rm --force $(docker ps -a -q --filter "name=sertisetl-etl-run") && docker image rm --force sertisetl-etl && \
docker compose run etl poetry run python main.py --source /opt/data/transaction.csv --database warehouse --table customers --save_mode overwrite

# TEST ETL
docker rm --force $(docker ps -a -q --filter "name=sertisetl-etl-run") && docker image rm --force sertisetl-etl && \
docker compose run etl poetry run python -m unittest discover -s /opt/tests

Longest streak -> http://sqlfiddle.com/#!18/c5f72/6

Sample usage:
docker exec -it etl poetry run python main.py
docker-compose run etl poetry run python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse
  --table transactions
"""

import argparse
import logging
from etl_utils import close_spark_session, process_etl

def main():
  # Configure logging
  logging.basicConfig(
      level=logging.INFO,  # Set the desired logging level
      format="%(asctime)s [%(levelname)s] - %(message)s",
      datefmt="%Y-%m-%d %H:%M:%S"
  )
  
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
  logging.info("")
  logging.info(f">>>> Processing data from: {source_path}")
  logging.info(f">>>> Database: {database_name}")
  logging.info(f">>>> Table: {table_name}")
  logging.info(f">>>> Save mode: {save_mode} {msg}\n")
  
  try: 
    # Process ETL
    process_etl(source_path, database_name, table_name, save_mode)
    logging.info(">>>> ETL SUCCESSFUL!\n")

  finally:
    # Close SparkSession if still open
    close_spark_session()


if __name__ == "__main__":
  main()