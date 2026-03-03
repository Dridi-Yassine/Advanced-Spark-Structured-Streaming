#!/bin/bash

# Create input topic
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iot-events

# Create valid events topic
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic valid-events

# Create invalid events topic
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic invalid-events

echo "Kafka topics created successfully."
