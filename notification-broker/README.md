# Kafka Cluster avec 3 Brokers et Script de Démarrage

Ce dépôt contient une configuration Docker Compose qui déploie un cluster Kafka avec trois brokers ainsi qu'un script `run-kafka.sh` pour démarrer et configurer le cluster Kafka, y compris la création des topics nécessaires.

## Prérequis

- Docker
- Docker Compose

Assurez-vous d'avoir Docker et Docker Compose installés sur votre machine. Vous pouvez vérifier leur installation en exécutant les commandes suivantes :

```bash
docker --version
docker-compose --version
```

## Structure du dépôt
- ocker-compose.yml # Déploie 3 brokers Kafka et Zookeeper
─ run-kafka.sh # Script de démarrage Kafka (création des topics)
- README.md # Ce fichier

## Déploiement du Cluster Kafka

### 1. Exécuter le Script de Démarrage Kafka

Dans le répertoire contenant `docker-compose.yml`, lancez les services :

```bash
chmod +x run-kafka.sh
./run-kafka.sh
```

Le script fait :

Démarre les services Docker Compose.
Vérifie la disponibilité des brokers Kafka.
Crée les topics si nécessaire : ens_notification, ens_notification_dlt, ens_notification_dlt-smir.

### 2. Accéder à AKHQ

Une fois Kafka en cours d'exécution, vous pouvez accéder à AKHQ à l'adresse suivante pour gérer les topics :

http://localhost:8083