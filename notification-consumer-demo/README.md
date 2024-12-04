# Kafka Notifications Demo

Ce projet est une démonstration d'un consommateur Kafka flexible et configurable, permettant de tester différentes stratégies de traitement des messages, de gestion des erreurs, et de configuration de commits. Il est conçu pour explorer les comportements dans divers scénarios grâce à l'utilisation de **feature flags**.

---

## 🚀 Fonctionnalités principales

- **Modes de consommation** :
    - Traitement **message par message** (`record`).
    - Traitement en **lot** (`batch`).

- **Stratégies de commits** :
    - **Automatique** : Commit après chaque consommation.
    - **Manuelle** : Commit après traitement explicite des messages.
    - **Transactionnelle** : Garantit l'atomicité des opérations.

- **Gestion des erreurs** :
    - **Erreurs génériques** :
        - Mode `silent` : Les erreurs sont ignorées et le traitement continue.
        - Mode `throw` : Les erreurs arrêtent le traitement.
    - **Erreurs spécifiques (API SMIR)** :
        - Mode `silent` : Les erreurs liées aux limites de taux (rate limits) sont ignorées.
        - Mode `throw` : Les erreurs spécifiques sont remontées.

- **Configuration des rejeux et Dead Letter Topics (DLT)** :
    - Paramétrage du nombre de rejeux (`retries`).
    - Différents DLT configurables
      - DLT pour les erreurs générales (`dlt`).
      - DLT pour les erreurs de rate limit de l'API SMIR (`dlt-smir`).

- **Optimisation des performances** :
    - Taille des lots (`max-poll-records`).
    - Intervalle maximum entre deux polls (`max-poll-interval.ms`).

---

## 🛠️ Configuration

### 1. **Configuration Kafka**
Le fichier `application.yml` permet une configuration fine du consommateur Kafka.

```yaml
spring:
  kafka:
    consumer:
      bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
      notification:
        group-id: ens-notification
        topic: ens_notification
        mode: batch # record ou batch
        commit-strategy: transaction # auto, manual, transaction
        sync-commits: false
        exception-strategy: silent # silent ou throw
        exception-strategy-smir: silent # silent ou throw
        retries: 0
        retries-interval: 300000
        retries-smir: false
        dlt: ens_notification_dlt
        dlt-smir: ens_notification_dlt-smir
        max-poll-records: 50
        max-poll-interval.ms: 300000
