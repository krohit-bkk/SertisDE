#!/bin/sh

while true; do
    current_time=$(date +"%Y-%m-%d %H:%M:%S")
    echo "$current_time I am alive! Going to sleep for 2 min!"
    sleep 2m
done
