#!/bin/bash

# Se positionner dans le répertoire où se trouve ce script (run_kafka.sh)
cd "$(dirname "$0")"

echo "Démarrer les services Docker Compose"
docker-compose up -d

echo "Attendre que le broker Kafka soit accessible ..."
docker-compose exec broker1 sh -c "until nc -z 127.0.0.1 9092; do sleep 1; done"

# Vérifier si le topic existe déjà
TOPIC_EXISTS=$(docker exec broker1 kafka-topics --list --bootstrap-server 127.0.0.1:9092 | grep -w ens_notification)

if [ -z "$TOPIC_EXISTS" ]; then
  # Créer le topic s'il n'existe pas
  docker exec broker1 kafka-topics \
    --create \
    --topic ens_notification \
    --bootstrap-server broker1:9092 \
    --partitions 6 \
    --replication-factor 3 \
    --config min.insync.replicas=2
  echo "Le topic ens_notification créé avec succès."
else
  echo "Le topic ens_notification existe déjà."
fi

TOPIC_EXISTS=$(docker exec broker1 kafka-topics --list --bootstrap-server 127.0.0.1:9092 | grep -w ens_notification_dlt)
if [ -z "$TOPIC_EXISTS" ]; then
  # Créer le topic s'il n'existe pas
  docker exec broker1 kafka-topics \
    --create \
    --topic ens_notification_dlt \
    --bootstrap-server broker1:9092 \
    --partitions 6 \
    --replication-factor 3 \
    --config min.insync.replicas=2
  echo "Le topic ens_notification_dlt créé avec succès."
else
  echo "Le topic ens_notification_dlt existe déjà."
fi

TOPIC_EXISTS=$(docker exec broker1 kafka-topics --list --bootstrap-server 127.0.0.1:9092 | grep -w ens_notification_dlt-smir)
if [ -z "$TOPIC_EXISTS" ]; then
  # Créer le topic s'il n'existe pas
  docker exec broker1 kafka-topics \
    --create \
    --topic ens_notification_dlt-smir \
    --bootstrap-server broker1:9092 \
    --partitions 6 \
    --replication-factor 3 \
    --config min.insync.replicas=2
  echo "Le topic ens_notification_dlt-smir créé avec succès."
else
  echo "Le topic ens_notification_dlt-smir existe déjà."
fi

#echo "Rollback offsets"
#docker exec broker1 kafka-consumer-groups \
#	--bootstrap-server broker1:9092 \
#	--group ens-notification  \
#	--topic ens_notification \
#	--reset-offsets \
#	--to-earliest \
#	--execute

echo "akhq is running : http://localhost:8083"