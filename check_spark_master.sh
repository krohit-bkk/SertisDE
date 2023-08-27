#!/bin/bash

SPARK_MASTER_URL="http://localhost:7077"

# Function to check if Spark master is running
check_spark_master() {
    echo "Checking Spark master at $SPARK_MASTER_URL..."
    response=$(curl -s "$SPARK_MASTER_URL")
    
    if [[ $response == *"Welcome to"* ]]; then
        echo "Spark master is up and running."
        exit 0
    else
        echo "Spark master is not reachable or not running."
        exit 1
    fi
}

check_spark_master

