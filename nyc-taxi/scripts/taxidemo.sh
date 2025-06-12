#!/bin/bash

# TODO has been replaced by the docker-compose file, but kept for reference

NETWORK_NAME="app-tier"
KAFKA_CONTAINER="kafka-nyc-taxi"
MZ_CONTAINER="materialize-nyc-taxi"
KAFKA_TOPIC="yellow-taxi-trips"

create_network() {
  if ! docker network ls | grep -q "$NETWORK_NAME"; then
    docker network create $NETWORK_NAME --driver bridge
  fi
}

case "$1" in
  start)
    echo "Starting Kafka and Materialize containers on Docker network '$NETWORK_NAME'..."
    
    create_network
    echo "Network $NETWORK_NAME created."

    # networking configured to allow outside-docker 9092 access (plaintext) and 29092 for internal docker networking
    docker run -d \
      --name $KAFKA_CONTAINER \
      --hostname $KAFKA_CONTAINER \
      --network $NETWORK_NAME \
      -e ALLOW_PLAINTEXT_LISTENER=yes \
      -e KAFKA_CFG_NODE_ID=0 \
      -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
      -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_INTERNAL://:29092,CONTROLLER://:9093 \
      -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://$KAFKA_CONTAINER:29092 \
      -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT \
      -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@$KAFKA_CONTAINER:9093 \
      -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -p 127.0.0.1:9092:9092 \
      bitnami/kafka:latest
    echo "Kafka started, waiting 12s to create topic..."
    sleep 12  # Wait for Kafka to initialize
    docker exec $KAFKA_CONTAINER kafka-topics.sh --create --topic $KAFKA_TOPIC --bootstrap-server localhost:9092
    echo "Kafka topic '$KAFKA_TOPIC' created."
    
    docker run -d \
      --name $MZ_CONTAINER \
      --network $NETWORK_NAME \
      -p 127.0.0.1:6874:6874 \
      -p 127.0.0.1:6875:6875 \
      -p 127.0.0.1:6876:6876 \
      materialize/materialized:latest
    echo "Materialize started."
    ;;
  stop)
    echo "Stopping containers..."
    docker stop $MZ_CONTAINER $KAFKA_CONTAINER
    echo "Removing containers..."
    docker rm $MZ_CONTAINER $KAFKA_CONTAINER
    echo "Removing network..."
    docker network rm $NETWORK_NAME
    ;;
  reset)
    echo "Deleting Kafka topic 'yellow-taxi-trips'..."
    docker exec $KAFKA_CONTAINER kafka-topics.sh --delete --topic yellow-taxi-trips --bootstrap-server localhost:9092
    sleep 2
    docker exec $KAFKA_CONTAINER kafka-topics.sh --create --topic yellow-taxi-trips --bootstrap-server localhost:9092
    echo "Kafka topic 'yellow-taxi-trips' has been re-created."
    ;;
  *)
    echo "Usage: $0 {start|stop}"
    exit 1
    ;;
esac